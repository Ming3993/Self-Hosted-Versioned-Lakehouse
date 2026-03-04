from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# ==============================
# Spark Session
# ==============================
spark = (
    SparkSession.builder
    .appName("Ingest JSONL to Iceberg (MinIO + Nessie)")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.nessie",
            "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.nessie.uri",
            "http://minio.lan:19120/api/v1")
    .config("spark.sql.catalog.nessie.ref", "main")
    .config("spark.sql.catalog.nessie.warehouse",
            "s3a://silver-zone/")
    .config("spark.sql.catalog.nessie.io-impl",
            "org.apache.iceberg.aws.s3.S3FileIO")

    .config("spark.sql.catalog.nessie.s3.endpoint", "http://minio.lan:9000")
    .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
    .config("spark.sql.catalog.nessie.s3.region", "us-east-1")

    # ---------- MinIO ----------
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio.lan:9000")
    .config("spark.hadoop.fs.s3a.access.key", "MINIO_ACCESS_KEY")
    .config("spark.hadoop.fs.s3a.secret.key", "MINIO_SECRET_KEY")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

    .getOrCreate()
)

spark.sql("CREATE DATABASE IF NOT EXISTS nessie.events")

# ==============================
# Mapping source → table
# ==============================
sources = {
    "auth_events": "s3a://ingestion-zone/events/auth_events_folder/*",
    "listen_events": "s3a://ingestion-zone/events/listen_events_folder/*",
    "page_view_events": "s3a://ingestion-zone/events/page_view_events_folder/*",
    "status_change_events": "s3a://ingestion-zone/events/status_change_events_folder/*"
}

# ==============================
# Ingest từng loại event
# ==============================
for table, path in sources.items():
    print(f"Processing {table} ...")

    df = spark.read.json(path)

    (
        df.writeTo(f"nessie.events.{table}")
        .using("iceberg")
        .tableProperty("format-version", "2")
        .createOrReplace()
    )

    print(f"Done {table}")

spark.stop()
