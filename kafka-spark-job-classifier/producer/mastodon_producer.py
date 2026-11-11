"""Producer niveau 4 — lit en temps réel les posts Mastodon publiés sous des hashtags
d'offres d'emploi et les envoie dans Kafka (format JSON).

Mastodon est utilisé car son API publique est gratuite (contrairement à X/Twitter).
Le script interroge l'endpoint /api/v1/timelines/tag/<hashtag> toutes les N secondes
avec `since_id` : seuls les nouveaux posts sont récupérés.

Usage : python producer/mastodon_producer.py [--instance https://mastodon.social]
                                             [--tags hiring,devjobs] [--interval 15]
Si l'instance exige une authentification, exporter MASTODON_TOKEN=<token>.
"""
import argparse
import html
import json
import os
import re
import time
from collections import deque

import requests
from kafka import KafkaProducer

DEFAULT_TAGS = ["hiring", "jobs", "techjobs", "devjobs", "datajobs", "remotejobs",
                "cybersecurityjobs", "devopsjobs", "cloudjobs", "emploi",
                "recrutement", "offredemploi"]

parser = argparse.ArgumentParser()
parser.add_argument("--instance", default=os.getenv("MASTODON_INSTANCE", "https://mastodon.social"))
parser.add_argument("--tags", default=",".join(DEFAULT_TAGS))
parser.add_argument("--interval", type=int, default=15, help="secondes entre deux tours")
parser.add_argument("--langs", default="fr,en", help="langues acceptées (vide = toutes)")
parser.add_argument("--min-length", type=int, default=40)
parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "job-offers"))
parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
args = parser.parse_args()

tags = [t.strip().lstrip("#") for t in args.tags.split(",") if t.strip()]
langs = {l.strip() for l in args.langs.split(",") if l.strip()}

session = requests.Session()
session.headers["User-Agent"] = "inpt-bigdata-student-project/1.0"
if os.getenv("MASTODON_TOKEN"):
    session.headers["Authorization"] = f"Bearer {os.environ['MASTODON_TOKEN']}"

producer = KafkaProducer(
    bootstrap_servers=args.bootstrap,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
    acks="all",
)


def html_to_text(content: str) -> str:
    text = re.sub(r"<br\s*/?>|</p>", " ", content)
    text = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\s+", " ", html.unescape(text)).strip()


since_ids = {}            # hashtag -> id du post le plus récent déjà vu
seen = deque(maxlen=5000)  # un même post peut apparaître sous plusieurs hashtags
seen_set = set()
total = 0

print(f"Écoute de {args.instance} — hashtags : {', '.join('#' + t for t in tags)}")
try:
    while True:
        for tag in tags:
            params = {"limit": 40}
            if tag in since_ids:
                params["since_id"] = since_ids[tag]
            try:
                r = session.get(f"{args.instance}/api/v1/timelines/tag/{tag}",
                                params=params, timeout=10)
                if r.status_code == 429:
                    print("Limite de requêtes atteinte, pause de 60 s")
                    time.sleep(60)
                    continue
                r.raise_for_status()
                statuses = r.json()
            except requests.RequestException as exc:
                print(f"#{tag} : erreur réseau ({exc})")
                continue

            if statuses:
                since_ids[tag] = statuses[0]["id"]  # le plus récent est en premier

            for status in reversed(statuses):  # du plus ancien au plus récent
                if status["id"] in seen_set or status.get("reblog"):
                    continue
                if len(seen) == seen.maxlen:
                    seen_set.discard(seen[0])
                seen.append(status["id"])
                seen_set.add(status["id"])

                if langs and status.get("language") and status["language"] not in langs:
                    continue
                text = html_to_text(status.get("content", ""))
                if len(text) < args.min_length:
                    continue

                message = {
                    "text": text,
                    "source": "mastodon",
                    "url": status.get("url"),
                    "author": status["account"]["acct"],
                    "created_at": status.get("created_at"),
                    "tag": tag,
                    "lang": status.get("language"),
                }
                producer.send(args.topic, message)
                total += 1
                print(f"[{total:04d}] #{tag:<18} {text[:80]}")
            time.sleep(1)  # politesse envers l'API

        producer.flush()
        time.sleep(args.interval)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    producer.close()
    print(f"Producer arrêté — {total} posts envoyés.")
