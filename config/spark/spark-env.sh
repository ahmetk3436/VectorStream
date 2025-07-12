SPARK_MASTER_HOST=spark-master
SPARK_MASTER_PORT=7077
SPARK_WORKER_MEMORY=2g
SPARK_WORKER_CORES=2
SPARK_HISTORY_OPTS="-Dspark.history.fs.logDirectory=/opt/spark/spark-events \
                    -Dspark.history.ui.port=18080"