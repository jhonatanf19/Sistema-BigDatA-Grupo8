import argparse
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


BASE_DIR = Path(__file__).resolve().parents[1]
CHECKPOINT_DIR = BASE_DIR / "data" / "checkpoints" / "retail_streaming"
OUTPUT_STREAM_DIR = BASE_DIR / "output" / "streaming_alerts"

for d in [CHECKPOINT_DIR, OUTPUT_STREAM_DIR]: d.mkdir(parents=True, exist_ok=True)

retail_event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("customer_segment", StringType(), True),
    StructField("sku", StringType(), True),
    StructField("category", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("channel", StringType(), True),
    StructField("is_high_value_alert", BooleanType(), True)
])


def process_retail_batch(batch_df, batch_id):
    """Analiza cada ráfaga de datos de Kafka."""
    if batch_df.isEmpty():
        return

    print(f"\n--- [MICRO-BATCH {batch_id}] ---")
    batch_df.cache()

    alerts_df = batch_df.filter(
        (F.col("is_high_value_alert") == True) | 
        (F.col("event_type") == "devolucion_solicitada")
    )

    stats_df = batch_df.groupBy("category").agg(
        F.count("*").alias("transacciones"),
        F.round(F.sum("amount"), 2).alias("volumen_venta_pen")
    )

    # 3. Visualización en Consola
    print("Métricas de Ventas Actuales:")
    stats_df.show()

    if alerts_df.count() > 0:
        print("⚠️ ALERTAS DETECTADAS (Alta Prioridad):")
        alerts_df.select("event_id", "customer_segment", "amount", "event_type").show()

    alerts_df.toPandas().to_csv(
        OUTPUT_STREAM_DIR / f"alerts_batch_{batch_id}.csv", 
        index=False
    )
    
    batch_df.unpersist()


def main():
    spark = (SparkSession.builder
            .appName("FalabellaStreamingAnalytics")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
            .getOrCreate())
    
    spark.sparkContext.setLogLevel("ERROR")

    # Lectura desde Kafka
    raw_stream_df = (spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:19092")
        .option("subscribe", "retail-events")
        .option("startingOffsets", "latest")
        .load())

    # Deserialización de JSON
    processed_stream_df = (raw_stream_df
        .selectExpr("CAST(value AS STRING)")
        .select(F.from_json("value", retail_event_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("timestamp")))

    # Definición del Sink (Salida)
    query = (processed_stream_df.writeStream
        .foreachBatch(process_retail_batch)
        .option("checkpointLocation", str(CHECKPOINT_DIR))
        .trigger(processingTime="10 seconds") # Procesar cada 10 segs
        .start())

    logger.info("Pipeline Streaming activo. Esperando eventos...")
    query.awaitTermination()

if __name__ == "__main__":
    main()