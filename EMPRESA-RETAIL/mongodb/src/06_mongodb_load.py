from pathlib import Path
import pandas as pd
import json
from pymongo import MongoClient

# =========================
# CONEXIÓN
# =========================
client = MongoClient("mongodb://localhost:27017/")

db = client["empresa"]

# Colecciones
ventas_collection = db["ventas"]
reporte_collection = db["reporte_final"]
alertas_collection = db["alertas_streaming"]


# =========================
# RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[2]

OUTPUT_DIR = BASE_DIR / "output" / "processed"
MAIN_OUTPUT = BASE_DIR / "output"


# =========================
# LIMPIAR COLECCIONES
# =========================
ventas_collection.delete_many({})
reporte_collection.delete_many({})
alertas_collection.delete_many({})


# =========================
# CARGAR ventas_final.csv
# =========================
ventas_df = pd.read_csv(
    OUTPUT_DIR / "ventas_final.csv"
)

ventas_data = ventas_df.to_dict(
    orient="records"
)

ventas_collection.insert_many(
    ventas_data
)

print(f"✔ Ventas cargadas: {len(ventas_data)}")


# =========================
# CARGAR reporte_final.csv
# =========================
reporte_df = pd.read_csv(
    MAIN_OUTPUT / "reporte_final.csv"
)

reporte_data = reporte_df.to_dict(
    orient="records"
)

reporte_collection.insert_many(
    reporte_data
)

print(f"✔ Reporte final cargado: {len(reporte_data)}")


# =========================
# CARGAR alertas_streaming.json
# =========================
with open(
    MAIN_OUTPUT / "alertas_streaming.json",
    "r",
    encoding="utf-8"
) as file:
    alertas_data = json.load(file)

if alertas_data:
    alertas_collection.insert_many(
        alertas_data
    )

print(f"✔ Alertas cargadas: {len(alertas_data)}")


print(">>> CARGA MONGODB COMPLETADA")