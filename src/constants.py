
SPARK_PROPERTIES_FILE="/opt/spark/conf/spark-properties.conf"
AWS_CREDS_PROVIDER="org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
CONTAINER="sparkhistoryserver"
CONTAINER_LAYER="spark-history-server"
CONFIG_KEY_S3_ENDPOINT="s3-endpoint"
CONFIG_KEY_S3_ACCESS_KEY="s3-access-key"
CONFIG_KEY_S3_SECRET_KEY="s3-secret-key"
CONFIG_KEY_S3_LOGS_DIR="spark-logs-s3-dir"
SPARK_USER="spark"
SPARK_USER_GROUP="spark"
SPARK_USER_UID=185
SPARK_USER_GID=185
SPARK_USER_WORKDIR="/opt/spark"
SPARK_HISTORY_SERVER_SCRIPT="/opt/spark/sbin/start-history-server.sh"
SPARK_HISTORY_SERVER_LAUNCH_CMD=f"{SPARK_HISTORY_SERVER_SCRIPT} --properties-file {SPARK_PROPERTIES_FILE}"
