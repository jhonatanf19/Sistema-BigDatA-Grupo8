from pymongo import MongoClient
import pandas as pd

# =========================
# CONEXIÓN
# =========================
client = MongoClient("mongodb://localhost:27017/")

db = client["empresa"]

ventas = db["ventas"]
alertas = db["alertas_streaming"]


# =========================
# TOP CLIENTES
# =========================
top_clientes = list(
    ventas.aggregate([
        {
            "$group": {
                "_id": "$customer_id",
                "nombre": {"$first": "$name"},
                "total_gastado": {"$sum": "$total_amount"}
            }
        },
        {"$sort": {"total_gastado": -1}},
        {"$limit": 10}
    ])
)

print("\n>>> TOP 10 CLIENTES POR GASTO")
print(pd.DataFrame(top_clientes))


# =========================
# VENTAS POR CIUDAD
# =========================
ventas_ciudad = list(
    ventas.aggregate([
        {
            "$group": {
                "_id": "$city",
                "ventas_totales": {"$sum": "$total_amount"}
            }
        },
        {"$sort": {"ventas_totales": -1}}
    ])
)

print("\n>>> VENTAS POR CIUDAD")
print(pd.DataFrame(ventas_ciudad))


# =========================
# ALERTAS CRÍTICAS
# =========================
alertas_criticas = list(
    alertas.find(
        {
            "$or": [
                {"is_high_value_alert": True},
                {"is_stock_alert": True}
            ]
        }
    ).limit(20)
)

print("\n>>> ALERTAS CRÍTICAS")
print(pd.DataFrame(alertas_criticas))


# =========================
# PRODUCTOS MÁS VENDIDOS
# =========================
top_productos = list(
    ventas.aggregate([
        {
            "$group": {
                "_id": "$product_name",
                "cantidad_vendida": {"$sum": "$quantity"}
            }
        },
        {"$sort": {"cantidad_vendida": -1}},
        {"$limit": 10}
    ])
)

print("\n>>> TOP PRODUCTOS")
print(pd.DataFrame(top_productos))


print("\n>>> CONSULTAS MONGODB COMPLETADAS")