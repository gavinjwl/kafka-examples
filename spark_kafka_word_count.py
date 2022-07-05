'''
spark-submit \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.sql.hive.convertMetastoreParquet=false" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 \
    spark_kafka_word_count.py \
    <brokers> <topic>
'''
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: spark_kafka_word_count.py <boostrp_servers> <topic>', file=sys.stderr)
        sys.exit(-1)

    boostrp_servers = sys.argv[1]
    topic = sys.argv[2]

    kafka_options = {
        'kafka.security.protocol': 'plaintext',
        'kafka.bootstrap.servers': boostrp_servers,
        # 'startingOffsets': 'earliest',
        'startingOffsets': 'latest',
    }

    spark = SparkSession.Builder()\
        .enableHiveSupport()\
        .appName('spark_kafka_word_count')\
        .getOrCreate()

    df = spark \
        .readStream \
        .format('kafka') \
        .options(**kafka_options) \
        .option('subscribe', topic) \
        .load()

    words_df = df.selectExpr('CAST(key AS STRING)', 'CAST(value AS STRING)', 'timestamp AS kafka_timestamp')\
        .select(split(col('value'), '\s+').alias('splitted'))\
        .select(explode(col('splitted')).alias('word'))\
        .where("word != ''")\
        .groupBy('word').count()

    query_plan = (
        words_df
        .writeStream
        .outputMode('complete')
        .format('console')
        .queryName('consoleQuery')
        .start()
    )

    print(query_plan.explain(extended=True))
    print(query_plan.status)
    query_plan.awaitTermination()
