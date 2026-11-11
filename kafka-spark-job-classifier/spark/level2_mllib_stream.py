"""Niveau 2 — Kafka -> Spark Structured Streaming -> modèle MLlib -> console.

Prérequis : python spark/train_model.py
Lancement : python spark/level2_mllib_stream.py
"""
from pyspark.sql import functions as F

from common import (CHECKPOINT_DIR, add_keyword_columns, apply_model,
                    build_spark, load_model, read_offers)

spark = build_spark("level2-mllib")
model = load_model()

offers = read_offers(spark)
classified = apply_model(add_keyword_columns(offers), model)

result = classified.select(
    F.date_format("kafka_ts", "HH:mm:ss").alias("heure"),
    F.substring("text", 1, 70).alias("offre"),
    F.col("category").alias("mllib"),
    F.col("confidence").alias("confiance"),
    F.col("kw_label").alias("mots_cles"),  # pour comparer avec le niveau 1
)

query = (result.writeStream
         .outputMode("append")
         .format("console")
         .option("truncate", "false")
         .option("checkpointLocation", str(CHECKPOINT_DIR / "level2"))
         .trigger(processingTime="2 seconds")
         .start())

print("Niveau 2 démarré — en attente de messages sur Kafka (Ctrl+C pour arrêter)")
query.awaitTermination()
