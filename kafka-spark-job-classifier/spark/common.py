"""Code partagé par tous les jobs Spark du projet."""
import os
import re
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import (ArrayType, IntegerType, StringType,
                               StructField, StructType)

# ---------------------------------------------------------------------------
# Configuration (surchargeable par variables d'environnement)
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent.parent
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "job-offers")
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "latest")  # ou "earliest"
MODEL_PATH = str(BASE_DIR / "models" / "job_classifier")
CHECKPOINT_DIR = BASE_DIR / "checkpoints"
CONFIDENCE_THRESHOLD = float(os.getenv("CONFIDENCE_THRESHOLD", "0.5"))

SPARK_VERSION = "3.5.1"  # doit correspondre à la version de pyspark installée
KAFKA_PACKAGE = f"org.apache.spark:spark-sql-kafka-0-10_2.12:{SPARK_VERSION}"

CATEGORIES = ["Data", "Développement", "Sécurité", "Cloud"]


def build_spark(app_name: str, with_kafka: bool = True) -> SparkSession:
    builder = (SparkSession.builder
               .appName(app_name)
               .master("local[*]")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.session.timeZone", "UTC"))
    if with_kafka:
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    # Rend ce module importable par les workers Python (nécessaire pour les UDF)
    spark.sparkContext.addPyFile(str(Path(__file__).resolve()))
    return spark


def read_offers(spark: SparkSession) -> DataFrame:
    """Lit le topic Kafka et renvoie un DataFrame (text, source, url, author, ...).

    Accepte deux formats de message :
      - texte brut (producer fichier, niveau 1)
      - JSON {"text": ..., "source": ..., "url": ...} (producer Mastodon, niveau 4)
    """
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", STARTING_OFFSETS)
           .option("failOnDataLoss", "false")
           .load())

    value = F.col("value").cast("string")
    return (raw.select(
                F.coalesce(F.get_json_object(value, "$.text"), value).alias("text"),
                F.coalesce(F.get_json_object(value, "$.source"), F.lit("file")).alias("source"),
                F.get_json_object(value, "$.url").alias("url"),
                F.get_json_object(value, "$.author").alias("author"),
                F.get_json_object(value, "$.created_at").alias("posted_at"),
                F.col("timestamp").alias("kafka_ts"))
            .filter(F.length(F.trim(F.col("text"))) > 0))


# ---------------------------------------------------------------------------
# Niveau 1 : classification par mots-clés
# ---------------------------------------------------------------------------
KEYWORDS = {
    "Data": [
        "data", "données", "donnees", "big data", "data scientist", "data analyst",
        "data engineer", "machine learning", "deep learning", "ml", "nlp", "ia",
        "spark", "hadoop", "sql", "etl", "power bi", "tableau", "pandas",
        "scikit-learn", "tensorflow", "pytorch", "statistiques", "statistics",
        "databricks", "airflow", "snowflake", "bi", "business intelligence",
    ],
    "Développement": [
        "développeur", "developpeur", "développeuse", "developer", "développement",
        "software engineer", "ingénieur logiciel", "java", "spring", "spring boot",
        "react", "angular", "vue.js", "javascript", "typescript", "node.js", "php",
        "symfony", "laravel", "c#", ".net", "flutter", "kotlin", "swift", "android",
        "ios", "frontend", "front-end", "backend", "back-end", "full stack",
        "fullstack", "api rest", "html", "css", "hibernate", "junit",
    ],
    "Sécurité": [
        "sécurité", "securite", "security", "cybersécurité", "cybersecurite",
        "cybersecurity", "soc", "siem", "pentest", "pentester", "test d'intrusion",
        "tests d'intrusion", "penetration testing", "firewall", "pare-feu",
        "pare-feux", "iso 27001", "vulnérabilité", "vulnérabilités",
        "vulnerability", "vulnerabilities", "forensic", "forensique", "grc",
        "iam", "rssi", "ciso", "splunk", "qradar", "burp suite", "metasploit",
        "nessus", "edr", "oscp", "cissp", "ceh", "phishing", "ebios",
    ],
    "Cloud": [
        "cloud", "aws", "azure", "gcp", "google cloud", "kubernetes", "k8s",
        "docker", "devops", "terraform", "ansible", "sre", "ci/cd", "jenkins",
        "gitlab ci", "helm", "openshift", "serverless", "prometheus", "grafana",
        "argocd", "cloudformation", "infrastructure as code", "iac", "finops",
    ],
}

_PATTERNS = {
    cat: [(kw, re.compile(r"(?<!\w)" + re.escape(kw) + r"(?!\w)", re.IGNORECASE))
          for kw in kws]
    for cat, kws in KEYWORDS.items()
}


def keyword_classify(text):
    """Renvoie (catégorie, score, mots-clés trouvés). 'Inconnu' si aucun match."""
    text = text or ""
    scores, found = {}, []
    for cat, patterns in _PATTERNS.items():
        hits = [kw for kw, pattern in patterns if pattern.search(text)]
        scores[cat] = len(hits)
        found.extend(hits)
    best = max(scores, key=scores.get)
    label = best if scores[best] > 0 else "Inconnu"
    return label, scores[best], sorted(set(kw.lower() for kw in found))


KEYWORD_SCHEMA = StructType([
    StructField("kw_label", StringType()),
    StructField("kw_score", IntegerType()),
    StructField("skills", ArrayType(StringType())),
])

keyword_udf = F.udf(keyword_classify, KEYWORD_SCHEMA)


def add_keyword_columns(df: DataFrame) -> DataFrame:
    return (df.withColumn("kw", keyword_udf(F.col("text")))
              .select("*", "kw.kw_label", "kw.kw_score", "kw.skills")
              .drop("kw"))


# ---------------------------------------------------------------------------
# Niveaux 2+ : modèle MLlib
# ---------------------------------------------------------------------------
def load_model():
    from pyspark.ml import PipelineModel
    if not Path(MODEL_PATH).exists():
        raise SystemExit(f"Modèle introuvable ({MODEL_PATH}). "
                         "Lance d'abord : python spark/train_model.py")
    return PipelineModel.load(MODEL_PATH)


def apply_model(df: DataFrame, model) -> DataFrame:
    """Ajoute predicted_label, confidence et category (avec seuil 'Incertain')."""
    from pyspark.ml.functions import vector_to_array
    out = model.transform(df)
    out = out.withColumn("confidence",
                         F.round(F.array_max(vector_to_array(F.col("probability"))), 3))
    return out.withColumn(
        "category",
        F.when(F.col("confidence") < CONFIDENCE_THRESHOLD, F.lit("Incertain"))
         .otherwise(F.col("predicted_label")))
