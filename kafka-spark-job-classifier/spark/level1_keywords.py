"""Niveau 1 — Kafka -> Spark Structured Streaming -> classification par mots-clés -> console.

Lancement :  python spark/level1_keywords.py
"""
from pyspark.sql import functions as F

from common import build_spark, read_offers, add_keyword_columns, CHECKPOINT_DIR

spark = build_spark("level1-keywords")

offers = read_offers(spark)
classified = add_keyword_columns(offers)

result = classified.select(
    F.date_format("kafka_ts", "HH:mm:ss").alias("heure"),
    F.substring("text", 1, 80).alias("offre"),
    F.col("kw_label").alias("categorie"),
    F.col("kw_score").alias("score"),
    F.concat_ws(", ", "skills").alias("mots_cles"),
)

query = (result.writeStream
         .outputMode("append")
         .format("console")
         .option("truncate", "false")
         .option("checkpointLocation", str(CHECKPOINT_DIR / "level1"))
         .trigger(processingTime="2 seconds")
         .start())

print("Niveau 1 démarré — en attente de messages sur Kafka (Ctrl+C pour arrêter)")
query.awaitTermination()
