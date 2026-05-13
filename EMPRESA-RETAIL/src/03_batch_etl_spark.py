from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pandas as pd

# =========================
# RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / "data"
PROCESSED_DIR = BASE_DIR / "output" / "processed"
KPI_DIR = BASE_DIR / "output"

# Crear carpetas necesarias
for d in [PROCESSED_DIR, KPI_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# =========================
# CREAR SESIÓN SPARK
# =========================
def create_spark_session():
    return (
        SparkSession.builder
        .appName("RetailBigDataETL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# =========================
# LECTURA CSV
# =========================
def read_raw_csv(spark, filename):
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(RAW_DIR / filename))
    )


# =========================
# EXPORTAR CON PANDAS (EVITA WINUTILS)
# =========================
def export_with_pandas(spark_df, output_file):
    pdf = spark_df.toPandas()
    pdf.to_csv(output_file, index=False, encoding="utf-8")
    print(f"✔ Exportado: {output_file.name}")


# =========================
# MAIN
# =========================
def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")

    print(">>> Iniciando ETL Batch Retail Big Data...")

    # =========================
    # CARGA DE DATOS
    # =========================
    ventas_df = read_raw_csv(spark, "ventas_principal.csv")

    productos_df = (
        read_raw_csv(spark, "productos.csv")
        .drop("category")
    )

    clientes_df = (
        spark.read
        .option("multiline", True)
        .json(str(RAW_DIR / "clientes.json"))
    )

    # =========================
    # LIMPIEZA Y TRANSFORMACIÓN
    # =========================
    ventas_cleaned = (
        ventas_df
        .withColumn("date", F.to_timestamp("date"))
        .withColumn("month", F.month("date"))
        .withColumn(
            "is_high_value",
            F.when(F.col("total_amount") > 2000, "SI").otherwise("NO")
        )
        .withColumn(
            "discount_pct",
            F.col("discount") * 100
        )
    )

    # =========================
    # JOIN
    # =========================
    master_df = (
        ventas_cleaned
        .join(productos_df, "product_id", "left")
        .join(clientes_df, "customer_id", "left")
    )

    # =========================
    # DATAFRAME
    # =========================
    print(">>> Vista DataFrame Integrado:")
    master_df.show(5)

    # =========================
    # RDD
    # =========================
    print(">>> Uso de RDD:")
    ventas_rdd = master_df.rdd
    print(f"Total registros en RDD: {ventas_rdd.count()}")

    # =========================
    # SPARK SQL
    # =========================
    master_df.createOrReplaceTempView("ventas_master")

    kpi_categorias = spark.sql("""
        SELECT
            category,
            COUNT(*) AS num_ventas,
            ROUND(SUM(total_amount), 2) AS ingresos_totales,
            ROUND(AVG(total_amount), 2) AS ticket_promedio
        FROM ventas_master
        GROUP BY category
        ORDER BY ingresos_totales DESC
    """)

    kpi_metodos_pago = spark.sql("""
        SELECT
            payment_method,
            COUNT(*) AS total_transacciones,
            ROUND(SUM(total_amount), 2) AS total_ingresos
        FROM ventas_master
        GROUP BY payment_method
        ORDER BY total_ingresos DESC
    """)

    kpi_ciudades = spark.sql("""
        SELECT
            city,
            COUNT(*) AS total_ventas,
            ROUND(SUM(total_amount), 2) AS ingresos_ciudad
        FROM ventas_master
        GROUP BY city
        ORDER BY ingresos_ciudad DESC
    """)

    # =========================
    # MOSTRAR KPIs
    # =========================
    print(">>> KPI Categorías")
    kpi_categorias.show()

    print(">>> KPI Métodos de Pago")
    kpi_metodos_pago.show()

    print(">>> KPI Ciudades")
    kpi_ciudades.show()

    # =========================
    # EXPORTAR TODO CON PANDAS
    # =========================
    print(">>> Exportando archivos finales...")

    export_with_pandas(
        master_df,
        PROCESSED_DIR / "ventas_final.csv"
    )

    export_with_pandas(
        kpi_ciudades,
        KPI_DIR / "kpis_batch.csv"
    )

    export_with_pandas(
        kpi_categorias,
        KPI_DIR / "kpi_categorias.csv"
    )

    export_with_pandas(
        kpi_metodos_pago,
        KPI_DIR / "kpi_metodos_pago.csv"
    )

    reporte_final = master_df.select(
        "sale_id",
        "date",
        "customer_id",
        "name",
        "city",
        "product_name",
        "category",
        "quantity",
        "total_amount",
        "payment_method"
    )

    export_with_pandas(
        reporte_final,
        KPI_DIR / "reporte_final.csv"
    )

    spark.stop()

    print(">>> ETL FINALIZADO CORRECTAMENTE")
    print("✔ DataFrame utilizado")
    print("✔ RDD utilizado")
    print("✔ Spark SQL utilizado")
    print("✔ KPIs generados")
    print("✔ Exportación final completada sin winutils")


# =========================
# ENTRY POINT
# =========================
if __name__ == "__main__":
    main()