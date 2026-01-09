# mysql数据库配置
mysql_db = "fy_project"
mysql_user = "root"
# mysql_user = "user"
mysql_password = "12345678"
mysql_host = "localhost"
mysql_port = 3306

# redis数据库配置
redis_link = "redis://localhost"
redis_pass = "redis_e2iaWP"
pull_add = "http://127.0.0.1:8080/index/api/addStreamProxy"
pull_del = "http://127.0.0.1:8080/index/api/delStreamProxy"
push_add = "http://127.0.0.1:8080/index/api/addStreamPusherProxy"
push_del = "http://127.0.0.1:8080/index/api/delStreamPusherProxy"
secret="s6TV6T3ZOSqz43m5Kbg5XyxF90Hr6aog"
vhost="__defaultVhost__"
app="live"
stream="test"
# url="rtsp%3A%2F%2F192.168.0.215%3A8554%2Flive%2Fback"
url="rtsp%3A%2F%2F{}%3A8554%2Flive%2Fback"

# 项目调试模式配置
debug = True

# 文件类型与目录配置
SAVE_DIR = "data"
FILE_TYPE = ".pcd"
