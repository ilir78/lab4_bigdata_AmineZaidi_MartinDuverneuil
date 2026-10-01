# admin.py
from confluent_kafka.admin import AdminClient, NewTopic

config = {'bootstrap.servers': 'localhost:9092'}
admin_client = AdminClient(config)

topic = 'gutenberg_topic'

# Création du nouveau topic pour le livre
admin_client.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])

# Vérification
x = admin_client.list_topics()
print("Topics disponibles :")
for t in x.topics.keys():
    print(t)