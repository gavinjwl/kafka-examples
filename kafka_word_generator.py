'''
https://github.com/dpkp/kafka-python/blob/master/example.py
'''
import sys
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from faker import Faker

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: kafka_word_generator.py <boostrp_servers> <topic>', file=sys.stderr)
        sys.exit(-1)

    boostrp_servers = sys.argv[1]
    topic = sys.argv[2]

    try:
        admin = KafkaAdminClient(bootstrap_servers=boostrp_servers)
        admin.create_topics([NewTopic(
            name=topic,
            num_partitions=1,
            replication_factor=1)
        ])
    except Exception:
        pass

    fake = Faker(locale=['en_US'])

    producer = KafkaProducer(
        security_protocol='PLAINTEXT',
        bootstrap_servers=boostrp_servers,
    )

    K = 1000
    for _ in range(1000 * K):
        message = fake.sentence(nb_words=20)
        future = producer.send(topic, message.encode('utf-8'))
        # future.get(timeout=60)
    producer.flush()
    metrics=producer.metrics()
    record_send_rate = metrics['producer-metrics']['record-send-rate']
    print(f'record-send-rate: {record_send_rate}')
    producer.close()
