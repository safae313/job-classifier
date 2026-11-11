"""Producer niveau 1 — lit un fichier texte ligne par ligne et envoie chaque ligne dans Kafka.

Usage : python producer/file_producer.py [--file data/offres_stream.txt] [--delay 1.5] [--loop]
"""
import argparse
import os
import time

from kafka import KafkaProducer

parser = argparse.ArgumentParser()
parser.add_argument("--file", default="data/offres_stream.txt")
parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "job-offers"))
parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
parser.add_argument("--delay", type=float, default=1.5, help="secondes entre deux messages")
parser.add_argument("--loop", action="store_true", help="relire le fichier en boucle")
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers=args.bootstrap,
    value_serializer=lambda v: v.encode("utf-8"),
    acks="all",
)

try:
    while True:
        sent = 0
        with open(args.file, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                producer.send(args.topic, line)
                sent += 1
                print(f"[{sent:03d}] -> {args.topic} : {line[:90]}")
                time.sleep(args.delay)
        producer.flush()
        if not args.loop:
            break
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    producer.close()
    print("Producer arrêté.")
