import json
import time
import random
from kafka import KafkaProducer
from datetime import datetime

# Connexion au broker Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    api_version=(0, 10, 1), # Très important pour la compatibilité
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all' # Garantit que Kafka a bien reçu le message
)

categories = ['Action', 'Comedy', 'Drama', 'Horror', 'Sci-Fi']
films = ['Academy Dinosaur', 'Bucket Brotherhood', 'Chamber Italian', 'Grosse Wonderful']

print("🚀 Simulateur de locations démarré...")

try:
    while True:
        # Création d'une donnée factice (simulant une nouvelle ligne en zone Bronze)
        data = {
            'rental_id': random.randint(20000, 99999),
            'customer_id': random.randint(1, 600),
            'film_id': random.randint(1, 1000),
            'title': random.choice(films),
            'category': random.choice(categories),
            'rental_rate': round(random.uniform(2.99, 9.99), 2),
            'rental_date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'actual_rental_duration': random.randint(1, 7)
        }
        
        # Envoi au topic
        producer.send('dvd_rentals', value=data)
        print(f"📡 Envoi : {data['title']} ({data['category']}) - {data['rental_rate']}€")
        
        time.sleep(3) # Simule une vente toutes les 3 secondes
except KeyboardInterrupt:
    print("Stopping...")