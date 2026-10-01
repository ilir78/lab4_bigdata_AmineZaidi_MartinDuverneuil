# producer.py
import socket
import time
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': socket.gethostname()
}
producer = Producer(conf)
topic = 'gutenberg_topic'

print("Début de l'envoi du livre...")

# Lecture du livre ligne par ligne
with open('book.txt', 'r', encoding='utf-8') as file:
    for line in file:
        line = line.strip()
        if line:  # On n'envoie pas les lignes vides
            producer.produce(topic=topic, value=line)
            print(f"Envoyé: {line[:60]}...")
            time.sleep(0.05)  # Petite pause pour simuler un flux continu

producer.flush()
producer.close()
print("Envoi terminé.")