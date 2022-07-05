# Kafka examples

Before starting, you must have a Apache Kafka cluster and Apache Spark cluster.

## Producing data

Making sure you installed the python [dependencies](./pyproject.toml) before you run `python3 kafka_word_generator.py <brokers> <topic>`

## Consuming data

Execute following command on your Apache Spark cluster

```bash
spark-submit \
    --conf "spark.serializer=org.apache.spark.serializer.KryoSerializer" \
    --conf "spark.sql.hive.convertMetastoreParquet=false" \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 \
    spark_kafka_word_count.py \
    <brokers> <topic>
```
