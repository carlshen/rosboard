# mysql数据库配置
mysql_db = "fy_project"
mysql_user = "root"
# mysql_user = "user"
mysql_password = "12345678"
mysql_host = "localhost"
mysql_port = 3306

# redis数据库配置
redis_ip = "127.0.0.1"
redis_port = "8001"
redis_pass = "redis_e2iaWP"
pull_add = "http://127.0.0.1:8080/index/api/addStreamProxy"
pull_del = "http://127.0.0.1:8080/index/api/delStreamProxy"
push_add = "http://127.0.0.1:8080/index/api/addStreamPusherProxy"
push_del = "http://127.0.0.1:8080/index/api/delStreamPusherProxy"
secret="s6TV6T3ZOSqz43m5Kbg5XyxF90Hr6aog"
vhost="__defaultVhost__"
app="live"
stream="test"
schema="rtsp"
pull_port=8554
push_port=12345
push_ip = "127.0.0.1"
pull_url="rtsp://{}:{}/live/back"
push_url="http://{}:{}/live/robot{}/hls.m3u8"

# 项目调试模式配置
debug = True
Cloud_Compress = False

# 文件类型与目录配置
SAVE_DIR = "data"
FILE_TYPE = ".pcd"
