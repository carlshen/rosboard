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
    size = peewee.IntegerField(default=0, verbose_name='size', help_text='文件大小')
    creator = peewee.TextField(verbose_name='creator', help_text='创建者')
    updater = peewee.TextField(verbose_name='updater', help_text='更新者')
    deleted = peewee.IntegerField(default=0, verbose_name='deleted', help_text='是否删除')

    class Meta:
        db_table = "infra_file"

