from peewee import Model, DateTimeField
from datetime import datetime
from rosboard.config import mysql_db, mysql_password, mysql_user, mysql_host, mysql_port
import peewee_async
import peewee

# 异步数据库
from playhouse.shortcuts import ReconnectMixin
from peewee_async import MySQLDatabase as AsyncMySQLDatabase
# 异步数据库断线重连类
class ReconnectAsyncMySQLDatabase(ReconnectMixin, AsyncMySQLDatabase):
    pass

database = ReconnectAsyncMySQLDatabase(mysql_db,host=mysql_host,port=mysql_port,user=mysql_user,password=mysql_password)

# 建立基础类
class BaseModel(Model):

    id = peewee.BigIntegerField(primary_key=True, unique=True,
            constraints=[peewee.SQL('AUTO_INCREMENT')])
    create_time = DateTimeField(default=datetime.now, verbose_name="添加时间", help_text='添加时间')
    update_time = DateTimeField(default=datetime.now, verbose_name='更新时间', help_text='更新时间')

    def save(self, *args, **kwargs):
        if self._pk is None:
            self.create_time = datetime.now()
        self.update_time = datetime.now()
        return super(BaseModel, self).save(*args, **kwargs)

    class Meta:
        database = database


# 文件
class InfraFile(BaseModel):
    config_id = peewee.IntegerField(default=0, verbose_name='config_id', help_text='配置')
    name = peewee.TextField(default='', verbose_name='name', help_text='文件名')
    path = peewee.TextField(default='', verbose_name='path', help_text='文件路径')
    url = peewee.TextField(default='', verbose_name='url', help_text='文件URL')
    type = peewee.TextField(default='', verbose_name='type', help_text='文件类型')
    size = peewee.BigIntField(default=0, verbose_name='size', help_text='文件大小')
    status = peewee.IntegerField(default=0, verbose_name='status', help_text='文件状态')
    task_id = peewee.TextField(default='', verbose_name='task_id', help_text='任务id')
    user_id = peewee.TextField(default='', verbose_name='user_id', help_text='用户id')
    creator = peewee.TextField(default='', verbose_name='creator', help_text='创建者')
    updater = peewee.TextField(default='', verbose_name='updater', help_text='更新者')
    deleted = peewee.IntegerField(default=0, verbose_name='deleted', help_text='是否删除')

    class Meta:
        db_table = "infra_file"


# 设备
class DeviceList(BaseModel):
    device_name = peewee.TextField(default='', verbose_name='device_name', help_text='设备名称')
    device_type = peewee.TextField(default='', verbose_name='device_type', help_text='设备类型')
    device_ip = peewee.TextField(default='', verbose_name='device_ip', help_text='设备IP')
    mesh_ip = peewee.TextField(default='', verbose_name='mesh_ip', help_text='自组网IP')
    lidar_ip = peewee.TextField(default='', verbose_name='lidar_ip', help_text='激光雷达IP')
    device_delay = peewee.TextField(default='', verbose_name='device_delay', help_text='设备延迟')
    device_status = peewee.IntegerField(default=0, verbose_name='device_status', help_text='设备状态')
    device_model = peewee.TextField(default='', verbose_name='device_model', help_text='设备型号')
    device_battery = peewee.TextField(default='', verbose_name='device_battery', help_text='设备电量')
    device_charge = peewee.TextField(default='', verbose_name='device_charge', help_text='设备充电桩')
    device_location = peewee.TextField(default='', verbose_name='device_location', help_text='设备地点')
    device_position = peewee.TextField(default='', verbose_name='device_position', help_text='设备地点位置')
    is_charging = peewee.IntegerField(default=0, verbose_name='is_charging', help_text='是否充电状态')
    task_interrupt = peewee.TextField(default='', verbose_name='task_interrupt', help_text='中断任务ID')
    node_interrupt = peewee.TextField(default='', verbose_name='node_interrupt', help_text='中断节点ID')
    task_id = peewee.TextField(default='', verbose_name='task_id', help_text='当前任务ID')
    node_id = peewee.TextField(default='', verbose_name='node_id', help_text='当前节点ID')
    user_id = peewee.TextField(default='', verbose_name='user_id', help_text='用户id')
    pull_key = peewee.TextField(default='', verbose_name='pull_key', help_text='pull key')
    push_key = peewee.TextField(default='', verbose_name='push_key', help_text='push key')
    device_desc = peewee.TextField(default='', verbose_name='device_desc', help_text='设备描述')
    creator = peewee.TextField(default='', verbose_name='creator', help_text='创建者')
    updater = peewee.TextField(default='', verbose_name='updater', help_text='更新者')

    class Meta:
        db_table = "device_list"

# 任务列表
class TaskList(BaseModel):
    task_no = peewee.TextField(default='', verbose_name='task_no', help_text='任务编号')
    task_name = peewee.TextField(default='', verbose_name='task_name', help_text='任务名称')
    description = peewee.TextField(default='', verbose_name='description', help_text='任务描述')
    task_map = peewee.TextField(default='', verbose_name='task_map', help_text='任务地图')
    task_type = peewee.TextField(default='', verbose_name='task_type', help_text='任务类型')
    task_pri = peewee.TextField(default='', verbose_name='task_pri', help_text='任务优先级')
    task_cron = peewee.TextField(default='', verbose_name='task_cron', help_text='任务cron表达式')
    task_timer = DateTimeField(default=datetime.now, verbose_name='task_timer', help_text='任务定时时间')
    task_recent = DateTimeField(default=datetime.now, verbose_name='task_recent', help_text='最近执行时间')
    task_status = peewee.TextField(default='', verbose_name='task_status', help_text='任务状态')
    task_rob = peewee.TextField(default='', verbose_name='task_rob', help_text='执行机器人')
    task_fail = peewee.IntegerField(default=0, verbose_name='task_fail', help_text='任务失败重试')
    task_handoff = peewee.IntegerField(default=0, verbose_name='task_handoff', help_text='任务交接与否')
    task_source = peewee.TextField(default='', verbose_name='task_source', help_text='任务来源')
    task_prompt = peewee.TextField(default='', verbose_name='task_prompt', help_text='任务提示')
    user_id = peewee.TextField(default='', verbose_name='user_id', help_text='用户id')
    creator = peewee.TextField(default='', verbose_name='creator', help_text='创建者')
    updater = peewee.TextField(default='', verbose_name='updater', help_text='更新者')

    class Meta:
        db_table = "task_list"

# 任务节点列表
class TaskNode(BaseModel):
    task_no = peewee.TextField(default='', verbose_name='task_no', help_text='任务编号')
    step = peewee.IntegerField(default=0, verbose_name='step', help_text='任务节点步骤')
    name = peewee.TextField(default='', verbose_name='name', help_text='任务节点名称')
    description = peewee.TextField(default='', verbose_name='description', help_text='任务节点描述')
    action = peewee.TextField(default='', verbose_name='action', help_text='任务动作')
    status = peewee.TextField(default='', verbose_name='status', help_text='节点状态')
    location = peewee.TextField(default='', verbose_name='location', help_text='节点地点位置')
    task_x = peewee.DoubleField(default=0.0, verbose_name='task_x', help_text='节点地点坐标X')
    task_y = peewee.DoubleField(default=0.0, verbose_name='task_y', help_text='节点地点坐标Y')
    task_z = peewee.DoubleField(default=0.0, verbose_name='task_z', help_text='节点地点坐标Z')
    robot = peewee.TextField(default='', verbose_name='robot', help_text='执行机器人')
    type = peewee.TextField(default='', verbose_name='type', help_text='任务类型')
    results = peewee.TextField(default='', verbose_name='results', help_text='任务结果')
    photo_id = peewee.TextField(default='', verbose_name='photo_id', help_text='照片ID')
    hot_id = peewee.TextField(default='', verbose_name='hot_id', help_text='热力图ID')
    voice_id = peewee.TextField(default='', verbose_name='voice_id', help_text='语音ID')
    depends = peewee.TextField(default='', verbose_name='depends', help_text='前置依赖')
    user_id = peewee.TextField(default='', verbose_name='user_id', help_text='用户id')
    creator = peewee.TextField(default='', verbose_name='creator', help_text='创建者')
    updater = peewee.TextField(default='', verbose_name='updater', help_text='更新者')

    class Meta:
        db_table = "task_node"

# 日志
class DeviceLog(BaseModel):
    device = peewee.TextField(default='', verbose_name='device', help_text='设备名称')
    title = peewee.TextField(default='', verbose_name='title', help_text='标题')
    type = peewee.IntegerField(default=0, verbose_name='type', help_text='类型')
    log = peewee.TextField(default='', verbose_name='log', help_text='日志')
    status = peewee.IntegerField(default=0, verbose_name='status', help_text='日志状态')
    task_no = peewee.TextField(default='', verbose_name='task_no', help_text='任务编号')
    task_id = peewee.TextField(default='', verbose_name='task_id', help_text='任务id')
    user_id = peewee.TextField(default='', verbose_name='user_id', help_text='用户id')
    creator = peewee.TextField(default='', verbose_name='creator', help_text='创建者')
    updater = peewee.TextField(default='', verbose_name='updater', help_text='更新者')

    class Meta:
        db_table = "device_log"

