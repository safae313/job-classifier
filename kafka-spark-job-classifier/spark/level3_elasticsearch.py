"""Niveau 3 (et 4) — Kafka -> Spark (MLlib) -> Elasticsearch -> Kibana.

Le même job sert au niveau 4 : il suffit de lancer le producer Mastodon
au lieu du producer fichier.

Prérequis : python spark/train_model.py  +  docker compose up -d
Lancement : python spark/level3_elasticsearch.py
"""
import os

from elasticsearch import Elasticsearch, helpers
from pyspark.sql import functions as F

from common import (CHECKPOINT_DIR, add_keyword_columns, apply_model,
                    build_spark, load_model, read_offers)

ES_URL = os.getenv("ES_URL", "http://localhost:9200")
INDEX = os.getenv("ES_INDEX", "job-offers")

es = Elasticsearch(ES_URL)

if not es.indices.exists(index=INDEX):
    es.indices.create(index=INDEX, mappings={
        "properties": {
            "@timestamp":      {"type": "date"},
            "text":            {"type": "text"},
            "category":        {"type": "keyword"},
            "predicted_label": {"type": "keyword"},
            "confidence":      {"type": "float"},
            "kw_label":        {"type": "keyword"},
            "kw_score":        {"type": "integer"},
            "skills":          {"type": "keyword"},
            "source":          {"type": "keyword"},
            "url":             {"type": "keyword"},
            "author":          {"type": "keyword"},
            "posted_at":       {"type": "date", "ignore_malformed": True},
            "agreement":       {"type": "boolean"},
        }
    })
    print(f"Index '{INDEX}' créé")

spark = build_spark("level3-elasticsearch")
model = load_model()

classified = apply_model(add_keyword_columns(read_offers(spark)), model)

documents = classified.select(
    F.date_format("kafka_ts", "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("@timestamp"),
    "text", "category", "predicted_label", "confidence",
    "kw_label", "kw_score", "skills", "source", "url", "author", "posted_at",
    (F.col("kw_label") == F.col("predicted_label")).alias("agreement"),
)


def write_to_es(batch_df, batch_id):
    rows = batch_df.collect()  # micro-batchs de petite taille : OK en local
    if not rows:
        return
    actions = [{"_index": INDEX,
                "_source": {k: v for k, v in row.asDict().items() if v is not None}}
               for row in rows]
    ok, errors = helpers.bulk(es, actions, raise_on_error=False)
    print(f"[batch {batch_id}] {ok} document(s) indexé(s)"
          + (f", {len(errors)} erreur(s) : {errors[:1]}" if errors else ""))
    for row in rows:
        print(f"   -> {row['category']:<14} ({row['confidence']:.2f})  {row['text'][:70]}")


query = (documents.writeStream
         .foreachBatch(write_to_es)
         .option("checkpointLocation", str(CHECKPOINT_DIR / "level3"))
         .trigger(processingTime="3 seconds")
         .start())

print(f"Niveau 3 démarré — écriture dans {ES_URL}/{INDEX} (Ctrl+C pour arrêter)")
query.awaitTermination()
