from pathlib import Path
import shutil
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
KPI_DIR = BASE_DIR / "output" / "kpis"

for d in [PROCESSED_DIR, KPI_DIR]: d.mkdir(parents=True, exist_ok=True)

def create_spark_session():
    return (SparkSession.builder
            .appName("FalabellaRetailETL")
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "8")
            .getOrCreate())


def read_raw_csv(spark, filename):
    return (spark.read.option("header", True)
            .option("inferSchema", True)
            .csv(str(RAW_DIR / filename)))

def save_report(df, name):
    output_path = KPI_DIR / name
    # Guardado simplificado para reportes finales
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(str(output_path + "_temp"))
    print(f"Reporte generado: {name}")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    print(">>> Iniciando ETL de Ventas Retail (30k registros)...")

    sales_df = read_raw_csv(spark, "sales_30k.csv")
    catalog_df = read_raw_csv(spark, "catalog.csv")
    customers_df = read_raw_csv(spark, "customers.csv")

    sales_cleaned = (sales_df
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("month", F.month("timestamp"))
        .withColumn("is_high_value", F.when(F.col("total_net") > 2000, "SI").otherwise("NO"))
        .withColumn("discount_pct", (F.col("discount_amount") / F.col("subtotal")) * 100)
    )

    master_df = sales_cleaned.join(customers_df, "customer_id", "left")

    master_df.createOrReplaceTempView("ventas_master")

    kpi_categorias = spark.sql("""
        SELECT 
            category, 
            COUNT(*) as num_ventas,
            ROUND(SUM(total_net), 2) as ingresos_totales,
            ROUND(AVG(total_net), 2) as ticket_promedio,
            ROUND(SUM(discount_amount), 2) as total_descuentos_otorgados
        FROM ventas_master
        GROUP BY category
        ORDER BY ingresos_totales DESC
    """)

    kpi_canales = spark.sql("""
        SELECT 
            channel, 
            COUNT(*) as transacciones,
            ROUND(SUM(total_net), 2) as venta_neta
        FROM ventas_master
        GROUP BY channel
    """)

    print(">>> Guardando datos maestros en formato Parquet...")
    master_df.write.mode("overwrite").parquet(str(PROCESSED_DIR / "ventas_final.parquet"))

    kpi_categorias.show()
    kpi_canales.show()

    spark.stop()
    print(">>> Proceso ETL Finalizado.")

if __name__ == "__main__":
    main()