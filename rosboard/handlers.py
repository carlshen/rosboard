import json
import socket
import time
import tornado
import tornado.web
import tornado.websocket
import traceback
import types
import uuid

from . import __version__

class NoCacheStaticFileHandler(tornado.web.StaticFileHandler):
    def set_extra_headers(self, path):
        # Disable cache
        self.set_header('Cache-Control', 'no-store, no-cache, must-revalidate, max-age=0')

class ROSBoardSocketHandler(tornado.websocket.WebSocketHandler):
    sockets = set()

    def check_origin(self, origin):
        return True

    def initialize(self, node):
        # store the instance of the ROS node that created this WebSocketHandler so we can access it later
        self.node = node

    def get_compression_options(self):
        # Non-None enables compression with default options.
        return {}

    def open(self):
        self.id = uuid.uuid4()    # unique socket id
        self.latency = 0          # latency measurement
        self.last_ping_times = [0] * 1024
        self.ping_seq = 0

        self.set_nodelay(True)

        # polyfill of is_closing() method for older versions of tornado
        if not hasattr(self.ws_connection, "is_closing"):
            self.ws_connection.is_closing = types.MethodType(
                lambda self_: self_.stream.closed() or self_.client_terminated or self_.server_terminated,
                self.ws_connection
            )

        self.update_intervals_by_topic = {}  # this socket's throttle rate on each topic
        self.last_data_times_by_topic = {}   # last time this socket received data on each topic

        ROSBoardSocketHandler.sockets.add(self)

        self.write_message(json.dumps([ROSBoardSocketHandler.MSG_SYSTEM, {
            "hostname": self.node.title,
            "version": __version__,
        }], separators=(',', ':')))
        self.node.sync_message(self)

    def on_close(self):
        ROSBoardSocketHandler.sockets.remove(self)

        # when socket closes, remove ourselves from all subscriptions
        for topic_name in self.node.remote_subs:
            if self.id in self.node.remote_subs[topic_name]:
                self.node.remote_subs[topic_name].remove(self.id)
        print("warn: on_close socket is closed.")

    @classmethod
    def send_pings(cls):
        """
        Send pings to all sockets. When pongs are received they will be used for measuring
        latency and clock differences.
        """

        for socket in cls.sockets:
            try:
                socket.last_ping_times[socket.ping_seq % 1024] = time.time() * 1000
                if socket.ws_connection and not socket.ws_connection.is_closing():
                    socket.write_message(json.dumps([ROSBoardSocketHandler.MSG_PING, {
                        ROSBoardSocketHandler.PING_SEQ: socket.ping_seq,
                    }], separators=(',', ':')))
                socket.ping_seq += 1
            except Exception as e:
                print("Error sending message: %s" % str(e))

    @classmethod
    def broadcast(cls, message):
        """
        Broadcasts a dict-ified ROS message (message) to all sockets that care about that topic.
        The dict message should contain metadata about what topic it was
        being sent on: message["_topic_name"], message["_topic_type"].
        """

        try:
            if message[0] == ROSBoardSocketHandler.MSG_TOPICS:
                json_msg = json.dumps(message, separators=(',', ':'))
                # print("topics message: %s" % json_msg)
                for socket in cls.sockets:
                    if socket.ws_connection and not socket.ws_connection.is_closing():
                        socket.write_message(json_msg)
            elif message[0] == ROSBoardSocketHandler.MSG_MSG:
                topic_name = message[1]["_topic_name"]
                json_msg = None
                for socket in cls.sockets:
                    if topic_name not in socket.node.remote_subs:
                        continue
                    if socket.id not in socket.node.remote_subs[topic_name]:
                        continue
                    t = time.time()
                    if t - socket.last_data_times_by_topic.get(topic_name, 0.0) < \
                            socket.update_intervals_by_topic.get(topic_name) - 2e-4:
                        continue
                    if socket.ws_connection and not socket.ws_connection.is_closing():
                        if json_msg is None:
                            json_msg = json.dumps(message, separators=(',', ':'))
                        socket.write_message(json_msg)
                    socket.last_data_times_by_topic[topic_name] = t
        except Exception as e:
            print("Error sending message: %s" % str(e))
            traceback.print_exc()

    @classmethod
    def callback(cls, message, sid):
        """
        callback a ROS message (message) to the socket that call the request.
        The dict message should contain metadata about the result processed.
        """

        try:
            json_msg = json.dumps(message, separators=(',', ':'))
            # print("callback result message: %s" % json_msg)
            for socket in cls.sockets:
                if socket and socket.id == sid:
                    if socket.ws_connection and not socket.ws_connection.is_closing():
                        socket.write_message(json_msg)
                    else:
                        print("callback result message error for socket is closed.")
        except Exception as e:
            print("Error callback message: %s" % str(e))
            traceback.print_exc()

    def on_message(self, message):
        """
        Message received from the client.
        """

        if self.ws_connection.is_closing():
            print("warn: on_message connection is closing.")
            return

        # JSON decode it, give up if it isn't valid JSON
        try:
            argv = json.loads(message)
        except (ValueError, TypeError):
            print("error: bad: %s" % message)
            return

        # make sure the received argv is a list and the first element is a string and indicates the type of command
        if type(argv) is not list or len(argv) < 1 or type(argv[0]) is not str:
            print("error: bad: %s" % message)
            return

        # if we got a pong for our own ping, compute latency and clock difference
        elif argv[0] == ROSBoardSocketHandler.MSG_PONG:
            if len(argv) != 2 or type(argv[1]) is not dict:
                print("error: pong: bad: %s" % message)
                return

            received_pong_time = time.time() * 1000
            self.latency = (received_pong_time - self.last_ping_times[argv[1].get(ROSBoardSocketHandler.PONG_SEQ, 0) % 1024]) / 2
            if self.latency > 1000.0:
                self.node.logwarn("socket %s has high latency of %.2f ms" % (str(self.id), self.latency))
            
            if self.latency > 10000.0:
                self.node.logerr("socket %s has excessive latency of %.2f ms; closing connection" % (str(self.id), self.latency))
                self.close()

        # client wants to subscribe to topic
        elif argv[0] == ROSBoardSocketHandler.MSG_SUB:
            if len(argv) != 2 or type(argv[1]) is not dict:
                print("error: sub: bad: %s" % message)
                return

            topic_name = argv[1].get("_topic_name")
            max_update_rate = float(argv[1].get("maxUpdateRate", 24.0))

            self.update_intervals_by_topic[topic_name] = 1.0 / max_update_rate
            self.node.update_intervals_by_topic[topic_name] = min(
                self.node.update_intervals_by_topic.get(topic_name, 1.),
                self.update_intervals_by_topic[topic_name]
            )

            if topic_name is None:
                print("error: no topic specified")
                return

            print("message: sub: %s" % message)
            if topic_name not in self.node.remote_subs:
                self.node.remote_subs[topic_name] = set()

            self.node.remote_subs[topic_name].add(self.id)
            self.node.sync_subs()

        # client wants to unsubscribe from topic
        elif argv[0] == ROSBoardSocketHandler.MSG_UNSUB:
            if len(argv) != 2 or type(argv[1]) is not dict:
                print("error: unsub: bad: %s" % message)
                return
            topic_name = argv[1].get("_topic_name")

            print("message: unsub: %s" % message)
            if topic_name not in self.node.remote_subs:
                self.node.remote_subs[topic_name] = set()

            try:
                self.node.remote_subs[topic_name].remove(self.id)
            except KeyError:
                print("KeyError trying to remove sub")
        # client wants to query topics
        elif argv[0] == ROSBoardSocketHandler.MSG_TOPICS:
            # all topics and their types as strings
            topic_data = argv[1]
            topic_data["_msg"] = ROSBoardSocketHandler.MSG_TOPICS
            topic_data["_sid"] = self.id
            if self.node.message_queue.full():
                json_err = [ROSBoardSocketHandler.MSG_TOPICS,
                    {
                        "code": -100,
                        "message": "Topic query is too many, please low down.",
                    }]
                print("message: topic_data: %s" % json_err)
                if self and self.ws_connection and not self.ws_connection.is_closing():
                    self.write_message(json.dumps(json_err))
            else:
                self.node.message_queue.put(topic_data)
        elif argv[0] == ROSBoardSocketHandler.MSG_TASK:
            # send list of current tasks
            # print("message: task: %s" % message)
            task_data = argv[1]
            if task_data is None:
                json_err = [ROSBoardSocketHandler.MSG_TASK,
                    {
                        "code": -100,
                        "message": "Task data in None.",
                    }]
                print("message: task_data: %s" % json_err)
                if self and self.ws_connection and not self.ws_connection.is_closing():
                    self.write_message(json.dumps(json_err))
            else:
                self.node.sync_tasks(self, task_data)
        elif argv[0] == ROSBoardSocketHandler.MSG_DEVICE:
            # send message of current device
            # print("message: device: %s" % message)
            device_data = argv[1]
            if device_data is None or self.node.message_queue.full():
                json_err = [ROSBoardSocketHandler.MSG_DEVICE,
                    {
                        "code": -100,
                        "message": "Device query is too many, please low down.",
                    }]
                print("message: device_data: %s" % json_err)
                if self and self.ws_connection and not self.ws_connection.is_closing():
                    self.write_message(json.dumps(json_err))
            else:
                device_data["_msg"] = ROSBoardSocketHandler.MSG_DEVICE
                device_data["_sid"] = self.id
                self.node.message_queue.put(device_data)
        elif argv[0] == ROSBoardSocketHandler.MSG_PCD:
            # save to file and insert into DB
            # print("message: file: %s" % message)
            point_cloud_data = argv[1]
            if point_cloud_data is None or self.node.message_queue.full():
                json_err = [ROSBoardSocketHandler.MSG_PCD,
                    {
                        "code": -100,
                        "message": "Pcd query is too many, please low down.",
                        "file_path": ""
                    }]
                print("message: point_cloud_data: %s" % json_err)
                if self and self.ws_connection and not self.ws_connection.is_closing():
                    self.write_message(json.dumps(json_err))
            elif point_cloud_data.get("_topic_name") == "query" or point_cloud_data.get("_topic_name") == "del" \
                    or point_cloud_data.get("_topic_name") == "file":
                point_cloud_data["_msg"] = ROSBoardSocketHandler.MSG_PCD
                point_cloud_data["_sid"] = self.id
                self.node.message_queue.put(point_cloud_data)
            else:
                point_cloud_data["_msg"] = ROSBoardSocketHandler.MSG_PCD_S
                point_cloud_data["_sid"] = self.id
                self.node.message_queue.put(point_cloud_data)
        elif argv[0] == ROSBoardSocketHandler.MSG_PGM:
            # save to file and insert into DB
            # print("message: file: %s" % message)
            point_cloud_map = argv[1]
            if point_cloud_map is None or self.node.message_queue.full():
                json_err = [ROSBoardSocketHandler.MSG_PGM,
                    {
                        "code": -100,
                        "message": "Pgm query is too many, please low down.",
                        "file_path": ""
                    }]
                print("message: point_cloud_map: %s" % json_err)
                if self and self.ws_connection and not self.ws_connection.is_closing():
                    self.write_message(json.dumps(json_err))
            elif point_cloud_map.get("_topic_name") == "query" or point_cloud_map.get("_topic_name") == "del" \
                    or point_cloud_map.get("_topic_name") == "file":
                point_cloud_map["_msg"] = ROSBoardSocketHandler.MSG_PGM
                point_cloud_map["_sid"] = self.id
                self.node.message_queue.put(point_cloud_map)
            else:
                point_cloud_map["_msg"] = ROSBoardSocketHandler.MSG_PGM_S
                point_cloud_map["_sid"] = self.id
                self.node.message_queue.put(point_cloud_map)
        elif argv[0] == ROSBoardSocketHandler.MSG_LOG:
            # query DB for the log
            log_msg = argv[1]
            # print("log_msg: %s" % log_msg)
            if (log_msg is None) or (log_msg.get("_topic_name") is None) or self.node.ros_queue.full():
                json_err = [ROSBoardSocketHandler.MSG_LOG,
                    {
                        "code": -100,
                        "message": "message data format error"
                    }]
                print("message: log_msg: %s" % json_err)
                if self and self.ws_connection and not self.ws_connection.is_closing():
                    self.write_message(json.dumps(json_err))
            else:
                log_msg["_msg"] = ROSBoardSocketHandler.MSG_LOG
                log_msg["_sid"] = self.id
                self.node.ros_queue.put(log_msg)

ROSBoardSocketHandler.MSG_PING = "p";
ROSBoardSocketHandler.MSG_PONG = "q";
ROSBoardSocketHandler.MSG_MSG = "msg";
ROSBoardSocketHandler.MSG_TOPICS = "topic";
ROSBoardSocketHandler.MSG_SUB = "sub";
ROSBoardSocketHandler.MSG_SYSTEM = "sys";
ROSBoardSocketHandler.MSG_UNSUB = "unsub";
ROSBoardSocketHandler.MSG_PCD = "pcd";
ROSBoardSocketHandler.MSG_PGM = "pgm";
ROSBoardSocketHandler.MSG_PCD_S = "pcd_s";
ROSBoardSocketHandler.MSG_PGM_S = "pgm_s";
ROSBoardSocketHandler.MSG_TASK = "task";
ROSBoardSocketHandler.MSG_DEVICE = "device";
ROSBoardSocketHandler.MSG_LOG = "log";
ROSBoardSocketHandler.MSG_CLOUD = "cloud";
ROSBoardSocketHandler.MSG_MAP = "map";
ROSBoardSocketHandler.MSG_VIDEO = "video";

ROSBoardSocketHandler.PING_SEQ = "s";
ROSBoardSocketHandler.PONG_SEQ = "s";
ROSBoardSocketHandler.PONG_TIME = "t";
