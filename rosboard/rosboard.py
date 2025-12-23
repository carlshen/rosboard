#!/usr/bin/env python3

import json
import yaml
import peewee
import peewee_async
from typing import Any, Optional
from pypcd4 import Encoding, MetaData, PointCloud
import redis
import queue
from datetime import datetime
from pathlib import Path
import asyncio
import importlib
import os
import socket
import threading
import time
import tornado, tornado.web, tornado.websocket
import traceback

if os.environ.get("ROS_VERSION") == "1":
    import rospy # ROS1
    from rospy_message_converter import message_converter
elif os.environ.get("ROS_VERSION") == "2":
    import rosboard.rospy2 as rospy # ROS2
    from rclpy.qos import HistoryPolicy, QoSProfile, QoSReliabilityPolicy, QoSDurabilityPolicy
    from rclpy_message_converter import message_converter
else:
    print("ROS not detected. Please source your ROS environment\n(e.g. 'source /opt/ros/DISTRO/setup.bash')")
    exit(1)

from rosgraph_msgs.msg import Log

from rosboard.serialization import ros2dict
from rosboard.subscribers.dmesg_subscriber import DMesgSubscriber
from rosboard.subscribers.processes_subscriber import ProcessesSubscriber
from rosboard.subscribers.system_stats_subscriber import SystemStatsSubscriber
from rosboard.subscribers.dummy_subscriber import DummySubscriber
from rosboard.handlers import ROSBoardSocketHandler, NoCacheStaticFileHandler
from rosboard.config import SAVE_DIR, FILE_TYPE
from rosboard.models import InfraFile
from nav_msgs.msg import OccupancyGrid, MapMetaData
from sensor_msgs.msg import PointCloud2
from sensor_msgs.msg import PointField
from geometry_msgs.msg import PoseStamped

class ROSBoardNode(object):
    instance = None
    def __init__(self, node_name = "rosboard_rosnode"):
        self.__class__.instance = self
        rospy.init_node(node_name)
        self.port = rospy.get_param("~port", 8888)
        self.title = rospy.get_param("~title", socket.gethostname())

        # desired subscriptions of all the websockets connecting to this instance.
        # these remote subs are updated directly by "friend" class ROSBoardSocketHandler.
        # this class will read them and create actual ROS subscribers accordingly.
        # dict of topic_name -> set of sockets
        self.remote_subs = {}

        # actual ROS subscribers.
        # dict of topic_name -> ROS Subscriber
        self.local_subs = {}

        # minimum update interval per topic (throttle rate) amang all subscribers to a particular topic.
        # we can throw data away if it arrives faster than this
        # dict of topic_name -> float (interval in seconds)
        self.update_intervals_by_topic = {}

        # last time data arrived for a particular topic
        # dict of topic_name -> float (time in seconds)
        self.last_data_times_by_topic = {}

        if rospy.__name__ == "rospy2":
            # ros2 hack: need to subscribe to at least 1 topic
            # before dynamic subscribing will work later.
            # ros2 docs don't explain why but we need this magic.
            self.sub_rosout = rospy.Subscriber("/rosout", Log, lambda x:x)

        tornado_settings = {
            'debug': True,
            'static_path': os.path.join(os.path.dirname(os.path.realpath(__file__)), 'html')
        }

        tornado_handlers = [
                (r"/websocket/ros", ROSBoardSocketHandler, {
                    "node": self,
                }),
                (r"/(.*)", NoCacheStaticFileHandler, {
                    "path": tornado_settings.get("static_path"),
                    "default_filename": "index.html"
                }),
        ]

        self.event_loop = None
        self.tornado_application = tornado.web.Application(tornado_handlers, **tornado_settings)
        asyncio.set_event_loop(asyncio.new_event_loop())
        self.event_loop = tornado.ioloop.IOLoop()
        self.tornado_application.listen(self.port)

        # allows tornado to log errors to ROS
        self.logwarn = rospy.logwarn
        self.logerr = rospy.logerr

        # tornado event loop. all the web server and web socket stuff happens here
        threading.Thread(target = self.event_loop.start, daemon = True).start()

        # loop to sync remote (websocket) subs with local (ROS) subs
        threading.Thread(target = self.sync_subs_loop, daemon = True).start()

        # loop to keep track of latencies and clock differences for each socket
        threading.Thread(target = self.pingpong_loop, daemon = True).start()

        # data dir for save pcd file, and redis client
        self.DATA_DIR = Path.home() / SAVE_DIR
        self.DATA_DIR.mkdir(parents=True, exist_ok=True)
        self.redis_client = redis.StrictRedis(host="localhost", port=6379, db=0, decode_responses=True)
        self.message_queue = queue.Queue(maxsize=10)
        self.message_num = 0
        # loop to keep track of message for save pcd file
        threading.Thread(target = self.savefile_loop, daemon = True).start()

        self.lock = threading.Lock()

        rospy.loginfo("ROSboard listening on :%d" % self.port)

    def start(self):
        rospy.spin()

    def get_msg_class(self, msg_type):
        """
        Given a ROS message type specified as a string, e.g.
            "std_msgs/Int32"
        or
            "std_msgs/msg/Int32"
        it imports the message class into Python and returns the class, i.e. the actual std_msgs.msg.Int32

        Returns none if the type is invalid (e.g. if user hasn't bash-sourced the message package).
        """
        try:
            msg_module, dummy, msg_class_name = msg_type.replace("/", ".").rpartition(".")
        except ValueError:
            rospy.logerr("invalid type %s" % msg_type)
            return None

        try:
            if not msg_module.endswith(".msg"):
                msg_module = msg_module + ".msg"
            return getattr(importlib.import_module(msg_module), msg_class_name)
        except Exception as e:
            rospy.logerr(str(e))
            return None

    if os.environ.get("ROS_VERSION") == "2":
        def get_topic_qos(self, topic_name: str) -> QoSProfile:
            """!
            Given a topic name, get the QoS profile with which it is being published
            @param topic_name (str) the topic name
            @return QosProfile the qos profile with which the topic is published. If no publishers exist
            for the given topic, it returns the sensor data QoS. returns None in case ROS1 is being used
            """
            if rospy.__name__ == "rospy2":
                topic_info = rospy._node.get_publishers_info_by_topic(topic_name=topic_name)
                if len(topic_info):
                    if topic_info[0].qos_profile.history == HistoryPolicy.UNKNOWN:
                        topic_info[0].qos_profile.history = HistoryPolicy.KEEP_LAST
                    return topic_info[0].qos_profile
                else:
                    rospy.logwarn(f"No publishers available for topic {topic_name}. Returning sensor data QoS")
                    return QoSProfile(
                            depth=10,
                            reliability=QoSReliabilityPolicy.BEST_EFFORT,
                            # reliability=QoSReliabilityPolicy.RELIABLE,
                            durability=QoSDurabilityPolicy.VOLATILE,
                            # durability=QoSDurabilityPolicy.TRANSIENT_LOCAL,
                        )
            else:
                rospy.logwarn("QoS profiles are only used in ROS2")
                return None

    def pingpong_loop(self):
        """
        Loop to send pings to all active sockets every 5 seconds.
        """
        while True:
            time.sleep(5)

            if self.event_loop is None:
                continue
            try:
                self.event_loop.add_callback(ROSBoardSocketHandler.send_pings)
            except Exception as e:
                rospy.logwarn(str(e))
                traceback.print_exc()

    def sync_subs_loop(self):
        """
        Periodically calls self.sync_subs(). Intended to be run in a thread.
        """
        while True:
            time.sleep(1)
            self.sync_subs()

    def savefile_loop(self):
        """
        Periodically calls save file for queue. Intended to be run in a thread.
        """
        while True:
            try:
                # 从队列获取消息（阻塞等待，直到有消息或超时）
                message = self.message_queue.get()

                # 处理消息
                if message is None:
                    continue
                if message.get("_topic_name", "") == "/global_map":
                    self.process_message(message)
                elif message.get("_topic_name", "") == "/global/GridMap":
                    self.save_map(message)
                else:
                    print("savefile_loop topic is not processed: %s" % message.get("_topic_name", ""))

                # 标记任务完成
                self.message_queue.task_done()

            except Exception as e:
                print(f"[savefile_loop] exception: {e}")

    def process_message(self, msg: json):
        if msg is None:
            print("process_message msg is None.")
            return
        msg.pop("_topic_name", None)
        msg.pop("_topic_type", None)
        msg.pop("_time", None)
        file_path = msg.pop("_file_path", None)
        if file_path is None:
            print("process_message file_path is None, need generate.")
            file_path = self.generate_filename()
        ros_msg = message_converter.convert_dictionary_to_ros_message('sensor_msgs/PointCloud2', msg)
        # transfer to PointCloud from ROS PointCloud2 message
        pc = PointCloud.from_msg(ros_msg)
        pc.save(file_path)
        if file_path.is_file():
            print("process_message: save file ok, save to db: %s" % file_path)
            saveFile = InfraFile.create()
            saveFile.name = Path(file_path).name
            saveFile.path = str(Path(file_path).resolve())
            saveFile.url = ""
            saveFile.type = "pcd"
            saveFile.size = os.path.getsize(file_path)
            saveFile.creator = "ros"
            saveFile.updater = "ros"
            saveFile.deleted = 0
            saveFile.save()
        else:
            print("process_message: save file error: %s" % file_path)

    def sync_topics(self, sock):
        # Acquire lock since either sync_subs_loop or websocket may call this function (from different threads)
        self.lock.acquire()
        try:
            # all topics and their types as strings e.g. {"/foo": "std_msgs/String", "/bar": "std_msgs/Int32"}
            self.all_topics = {}

            for topic_tuple in rospy.get_published_topics():
                topic_name = topic_tuple[0]
                topic_type = topic_tuple[1]
                if type(topic_type) is list:
                    topic_type = topic_type[0] # ROS2
                self.all_topics[topic_name] = topic_type

            if sock and sock.ws_connection and not sock.ws_connection.is_closing():
                json_msg = json.dumps([ROSBoardSocketHandler.MSG_TOPICS, self.all_topics ], separators=(',', ':'))
                print("sync_topics message: %s" % json_msg)
                sock.write_message(json_msg)
                # sync the cached redis mesage to socket client
                if self.has_key("/global_map"):
                    json_msg = self.load_json("/global_map")
                    # print("sync_topics get message: %s" % json.dumps(json_msg))
                    print("sync_topics for cached message len: %s" % len(json.dumps(json_msg)))
                    sock.write_message(json.dumps(json_msg))

        except Exception as e:
            rospy.logwarn(str(e))
            traceback.print_exc()
        self.lock.release()

    def sync_tasks(self, sock: socket, msg: json):
        # send task data to ros
        if msg is None or sock is None:
            print("sync_tasks msg is None.")
            return
        try:
            topic_name = msg.pop("_topic_name", None)
            topic_type = msg.pop("_topic_type", None)
            ros_msg = message_converter.convert_dictionary_to_ros_message('geometry_msgs/PoseStamped', msg)
            if ros_msg is not None and not rospy.is_shutdown():
                pub = rospy.Publisher(topic_name, PoseStamped, queue_size=10)
                pub.publish(ros_msg)

            if sock and sock.ws_connection and not sock.ws_connection.is_closing():
                json_ok = [ROSBoardSocketHandler.MSG_TASK,
                    {
                        "code": 0,
                        "message": "task data send successfully",
                    }]
                print("message: save task_data: %s" % json_ok)
                sock.write_message(json_ok)

        except Exception as e:
            rospy.logwarn(str(e))
            traceback.print_exc()

    def sync_subs(self):
        """
        Looks at self.remote_subs and makes sure local subscribers exist to match them.
        Also cleans up unused local subscribers for which there are no remote subs interested in them.
        """

        # Acquire lock since either sync_subs_loop or websocket may call this function (from different threads)
        self.lock.acquire()

        try:
            # all topics and their types as strings e.g. {"/foo": "std_msgs/String", "/bar": "std_msgs/Int32"}
            self.all_topics = {}

            for topic_tuple in rospy.get_published_topics():
                topic_name = topic_tuple[0]
                topic_type = topic_tuple[1]
                if type(topic_type) is list:
                    topic_type = topic_type[0] # ROS2
                self.all_topics[topic_name] = topic_type

            # self.event_loop.add_callback(
            #     ROSBoardSocketHandler.broadcast,
            #     [ROSBoardSocketHandler.MSG_TOPICS, self.all_topics ]
            # )

            for topic_name in self.remote_subs:
                if len(self.remote_subs[topic_name]) == 0:
                    continue

                # remote sub special (non-ros) topic: _dmesg
                # handle it separately here
                if topic_name == "_dmesg":
                    if topic_name not in self.local_subs:
                        rospy.loginfo("Subscribing to dmesg [non-ros]")
                        self.local_subs[topic_name] = DMesgSubscriber(self.on_dmesg)
                    continue

                if topic_name == "_system_stats":
                    if topic_name not in self.local_subs:
                        rospy.loginfo("Subscribing to _system_stats [non-ros]")
                        self.local_subs[topic_name] = SystemStatsSubscriber(self.on_system_stats)
                    continue

                if topic_name == "_top":
                    if topic_name not in self.local_subs:
                        rospy.loginfo("Subscribing to _top [non-ros]")
                        self.local_subs[topic_name] = ProcessesSubscriber(self.on_top)
                    continue

                # check if remote sub request is not actually a ROS topic before proceeding
                if topic_name not in self.all_topics:
                    rospy.logwarn("warning: topic %s not found" % topic_name)
                    continue

                # if the local subscriber doesn't exist for the remote sub, create it
                if topic_name not in self.local_subs:
                    topic_type = self.all_topics[topic_name]
                    msg_class = self.get_msg_class(topic_type)

                    if msg_class is None:
                        # invalid message type or custom message package not source-bashed
                        # put a dummy subscriber in to avoid returning to this again.
                        # user needs to re-run rosboard with the custom message files sourced.
                        self.local_subs[topic_name] = DummySubscriber()
                        self.event_loop.add_callback(
                            ROSBoardSocketHandler.broadcast,
                            [
                                ROSBoardSocketHandler.MSG_MSG,
                                {
                                    "_topic_name": topic_name, # special non-ros topics start with _
                                    "_topic_type": topic_type,
                                    "_error": "Could not load message type '%s'. Are the .msg files for it source-bashed?" % topic_type,
                                },
                            ]
                        )
                        continue

                    self.last_data_times_by_topic[topic_name] = 0.0

                    rospy.loginfo("Subscribing to %s" % topic_name)

                    kwargs = {}
                    if rospy.__name__ == "rospy2":
                        # In ros2 we also can pass QoS parameters to the subscriber.
                        # To avoid incompatibilities we subscribe using the same Qos
                        # of the topic's publishers
                        kwargs = {"qos": self.get_topic_qos(topic_name)}
                    self.local_subs[topic_name] = rospy.Subscriber(
                        topic_name,
                        self.get_msg_class(topic_type),
                        self.on_ros_msg,
                        callback_args = (topic_name, topic_type),
                        **kwargs
                    )

            # clean up local subscribers for which remote clients have lost interest
            for topic_name in list(self.local_subs.keys()):
                if topic_name not in self.remote_subs or \
                    len(self.remote_subs[topic_name]) == 0:
                        rospy.loginfo("Unsubscribing from %s" % topic_name)
                        self.local_subs[topic_name].unregister()
                        del(self.local_subs[topic_name])

        except Exception as e:
            rospy.logwarn(str(e))
            traceback.print_exc()

        self.lock.release()

    def on_system_stats(self, system_stats):
        """
        system stats received. send it off to the client as a "fake" ROS message (which could at some point be a real ROS message)
        """
        if self.event_loop is None:
            return

        msg_dict = {
            "_topic_name": "_system_stats", # special non-ros topics start with _
            "_topic_type": "rosboard_msgs/msg/SystemStats",
        }

        for key, value in system_stats.items():
            msg_dict[key] = value

        self.event_loop.add_callback(
            ROSBoardSocketHandler.broadcast,
            [
                ROSBoardSocketHandler.MSG_MSG,
                msg_dict
            ]
        )

    def on_top(self, processes):
        """
        processes list received. send it off to the client as a "fake" ROS message (which could at some point be a real ROS message)
        """
        if self.event_loop is None:
            return

        self.event_loop.add_callback(
            ROSBoardSocketHandler.broadcast,
            [
                ROSBoardSocketHandler.MSG_MSG,
                {
                    "_topic_name": "_top", # special non-ros topics start with _
                    "_topic_type": "rosboard_msgs/msg/ProcessList",
                    "processes": processes,
                },
            ]
        )

    def on_dmesg(self, text):
        """
        dmesg log received. make it look like a rcl_interfaces/msg/Log and send it off
        """
        if self.event_loop is None:
            return

        self.event_loop.add_callback(
            ROSBoardSocketHandler.broadcast,
            [
                ROSBoardSocketHandler.MSG_MSG,
                {
                    "_topic_name": "_dmesg", # special non-ros topics start with _
                    "_topic_type": "rcl_interfaces/msg/Log",
                    "msg": text,
                },
            ]
        )

    def on_ros_msg(self, msg, topic_info):
        """
        ROS messaged received (any topic or type).
        """
        topic_name, topic_type = topic_info
        t = time.time()
        if t - self.last_data_times_by_topic.get(topic_name, 0) < self.update_intervals_by_topic[topic_name] - 1e-4:
            return

        if self.event_loop is None:
            return

        # convert ROS message into a dict and get it ready for serialization
        #ros_msg_dict = ros2dict(msg)
        ros_msg_dict = message_converter.convert_ros_message_to_dictionary(msg)

        # add metadata
        ros_msg_dict["_topic_name"] = topic_name
        ros_msg_dict["_topic_type"] = topic_type
        ros_msg_dict["_time"] = time.time() * 1000
        # rospy.loginfo("sending message: %s" % ros_msg_dict)

        # log last time we received data on this topic
        self.last_data_times_by_topic[topic_name] = t

        # broadcast it to the listeners that care
        self.event_loop.add_callback(
            ROSBoardSocketHandler.broadcast,
            [ROSBoardSocketHandler.MSG_MSG, ros_msg_dict]
        )
        if topic_name == "/global_map":
            # store it to the redis cache as well
            json_msg = [ROSBoardSocketHandler.MSG_MSG, ros_msg_dict]
            # print("sync_topics set message: %s" % json_msg)
            self.cache_json(topic_name, json_msg)
        elif topic_name == "/global/GridMap":
            # store it to the redis cache as well
            if (self.message_num < 1):
                self.message_num += 1
                print("cache_json message num: %s" % self.message_num)
            #     file_path = self.map_filename()
            #     ros_msg_dict["_file_path"] = file_path
            #     self.message_queue.put(ros_msg_dict)

    def cache_json(self, key: str, data: Any, expire_seconds: Optional[int] = None) -> None:
        """将任意可 JSON 序列化的数据写入 Redis 指定 key，可选过期时间（秒）"""
        json_str = json.dumps(data, separators=(',', ':'))
        self.redis_client.set(name=key, value=json_str, ex=expire_seconds)

    def has_key(self, key: str) -> bool:
        """判断 Redis 中是否存在指定 key"""
        return bool(self.redis_client.exists(key))

    def load_json(self, key: str) -> Optional[Any]:
        """读取并解析指定 key 的 JSON 数据，不存在则返回 None"""
        json_str = self.redis_client.get(key)
        if json_str is None:
            return None
        return json.loads(json_str)

    def generate_filename(self) -> Path:
        """生成类似 point_20251217_142305_123 的文件名（精确到毫秒）"""
        now = datetime.now()
        # 日期时间部分
        dt_str = now.strftime("%Y%m%d_%H%M%S")
        # 毫秒部分
        mmm = f"{now.microsecond // 1000:03d}"
        filename = f"point_{dt_str}_{mmm}{FILE_TYPE}"
        return self.DATA_DIR / filename

    def map_filename(self) -> Path:
        """生成类似 point_20251217_142305_123 的文件名（精确到毫秒）"""
        now = datetime.now()
        # 日期时间部分
        dt_str = now.strftime("%Y%m%d_%H%M%S")
        # 毫秒部分
        mmm = f"{now.microsecond // 1000:03d}"
        filename = f"point_{dt_str}_{mmm}"
        return self.DATA_DIR / filename

    def save_map(self, msg: json):
        if msg is None:
            print("save_map msg is None.")
            return
        msg.pop("_topic_name", None)
        msg.pop("_topic_type", None)
        msg.pop("_time", None)
        base = msg.pop("_file_path", None)
        if base is None:
            print("save_map file_path is None, need generate.")
            base = self.map_filename()
        pgm_path = base.__str__() + ".pgm"
        yaml_path = base.__str__() + ".yaml"
        ros_msg = message_converter.convert_dictionary_to_ros_message('nav_msgs/OccupancyGrid', msg)
        # Save image (simple PGM) and YAML metadata compatible with yaml_server
        self.save_pgm(ros_msg, pgm_path)
        self.save_yaml(ros_msg, yaml_path, os.path.basename(pgm_path))
        # save to db for map files
        if os.path.exists(pgm_path):
            print("save_map: save pgm_path ok, save to db: %s" % pgm_path)
            saveFile = InfraFile.create()
            saveFile.name = Path(pgm_path).name
            saveFile.path = str(Path(pgm_path).resolve())
            saveFile.url = ""
            saveFile.type = "pgm"
            saveFile.size = os.path.getsize(pgm_path)
            saveFile.creator = "ros"
            saveFile.updater = "ros"
            saveFile.deleted = 0
            saveFile.save()
        else:
            print("save_map: save pgm_path error: %s" % yaml_path)
        if os.path.exists(yaml_path):
            print("save_map: save yaml_path ok, save to db: %s" % yaml_path)
            saveFile = InfraFile.create()
            saveFile.name = Path(yaml_path).name
            saveFile.path = str(Path(yaml_path).resolve())
            saveFile.url = ""
            saveFile.type = "yaml"
            saveFile.size = os.path.getsize(yaml_path)
            saveFile.creator = "ros"
            saveFile.updater = "ros"
            saveFile.deleted = 0
            saveFile.save()
        else:
            print("save_map: save yaml_path error: %s" % yaml_path)

    def save_pgm(self, msg: OccupancyGrid, path: str):
        width, height = msg.info.width, msg.info.height
        data = msg.data  # list[int] length w*h
        with open(path, "wb") as f:
            f.write(f"P5\n{width} {height}\n255\n".encode())
            for v in data:
                # Convert occupancy to grayscale: unknown=205, occupied=0, free=254
                if v == -1:
                    g = 205
                elif v >= 50:
                    g = 0
                else:
                    g = 254
                f.write(bytes([g]))

    def save_yaml(self, msg: OccupancyGrid, yaml_path: str, pgm_name: str):
        info: MapMetaData = msg.info
        origin = info.origin.position
        data = {
            "image": pgm_name,
            "resolution": info.resolution,
            "origin": [origin.x, origin.y, 0.0],
            "negate": 0,
            "occupied_thresh": 0.65,
            "free_thresh": 0.196,
        }
        with open(yaml_path, "w") as f:
            yaml.dump(data, f)

def main(args=None):
    ROSBoardNode().start()

if __name__ == '__main__':
    main()
