# consumer.py
import re
from confluent_kafka import Consumer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'book_reader_group',
    'auto.offset.reset': 'smallest'
}
consumer = Consumer(conf)
topic = 'gutenberg_topic'
consumer.subscribe([topic])

MAX_EMPTY_POLLS = 10
empty_polls = 0

print("Écoute des messages en cours...")

# On ouvre le fichier de sortie pour écrire les messages nettoyés
with open('cleaned_book.txt', 'w', encoding='utf-8') as out_file:
    while True:
        msg = consumer.poll(1.0)

        # Gestion du Timeout (fin du flux)
        if msg is None:
            empty_polls += 1
            if empty_polls >= MAX_EMPTY_POLLS:
                print("Plus aucun message reçu. Fermeture.")
                break
            continue

        # Gestion des erreurs
        if msg.error():
            print(f"Erreur Consumer : {msg.error()}")
            continue

        empty_polls = 0
        raw_text = msg.value().decode('utf-8')

        # Nettoyage de base : tout en minuscules, on ne garde que lettres/chiffres/espaces
        cleaned_text = re.sub(r'[^a-z0-9\s]', '', raw_text.lower())
        
        # Si la ligne n'est pas vide après nettoyage, on l'écrit
        if cleaned_text.strip():
            out_file.write(cleaned_text + '\n')
            print(f"Reçu et nettoyé: {cleaned_text[:60]}")

consumer.close()