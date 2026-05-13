import logging
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


# =========================
# RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]

STREAM_FILE = BASE_DIR / "data" / "streaming_eventos.jsonl"

OUTPUT_DIR = BASE_DIR / "output"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# CREAR SPARK
# =========================
def create_spark():
    return (
        SparkSession.builder
        .appName("RetailStreamingLocal")
        .master("local[*]")
        .getOrCreate()
    )


# =========================
# MAIN
# =========================
def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print(">>> Iniciando análisis de streaming local...")

    # =========================
    # LEER JSONL
    # =========================
    df = (
        spark.read
        .option("multiline", False)
        .json(str(STREAM_FILE))
    )

    print(">>> Vista previa eventos:")
    df.show(5, truncate=False)

    # =========================
    # ALERTAS
    # =========================
    alerts_df = df.filter(
        (F.col("is_high_value_alert") == True) |
        (F.col("is_stock_alert") == True) |
        (F.col("event_type") == "pago_rechazado") |
        (F.col("event_type") == "devolucion_producto")
    )

    # =========================
    # MÉTRICAS POR CATEGORÍA
    # =========================
    categoria_df = (
        df.groupBy("category")
        .agg(
            F.count("*").alias("transacciones"),
            F.round(
                F.sum("amount"),
                2
            ).alias("volumen_venta")
        )
        .orderBy(F.desc("volumen_venta"))
    )

    # =========================
    # MÉTRICAS POR EVENTO
    # =========================
    eventos_df = (
        df.groupBy("event_type")
        .count()
        .orderBy(F.desc("count"))
    )

    # =========================
    # MOSTRAR
    # =========================
    print(">>> Métricas por Categoría:")
    categoria_df.show(truncate=False)

    print(">>> Frecuencia de Eventos:")
    eventos_df.show(truncate=False)

    print(">>> Alertas Detectadas:")
    alerts_df.select(
        "event_id",
        "customer_id",
        "product_name",
        "amount",
        "stock",
        "event_type"
    ).show(truncate=False)

    # =========================
    # EXPORTAR ALERTAS
    # =========================
    alerts_output = OUTPUT_DIR / "alertas_streaming.json"

    alerts_pd = alerts_df.toPandas()

    alerts_pd.to_json(
        alerts_output,
        orient="records",
        force_ascii=False,
        indent=4
    )

    print(f"✔ Alertas exportadas: {alerts_output.name}")

    spark.stop()

    print(">>> STREAMING ANALYTICS FINALIZADO")
    print("✔ Análisis local completado")
    print("✔ Sin Kafka")
    print("✔ Alertas generadas")


# =========================
# ENTRYPOINT
# =========================
if __name__ == "__main__":
    main()