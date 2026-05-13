from pathlib import Path
from datetime import datetime, timedelta
import random
import numpy as np
import pandas as pd
from faker import Faker

# =========================
# CONFIGURACIÓN GLOBAL
# =========================
SEED = 2026
random.seed(SEED)
np.random.seed(SEED)

fake = Faker("es_ES")
Faker.seed(SEED)

N_PRODUCTS = 1000
N_CUSTOMERS = 10000
N_SALES = 30000

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data"
RAW_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# UTILIDADES
# =========================
def save_csv(df, filename):
    output_path = RAW_DIR / filename
    df.to_csv(output_path, index=False, encoding="utf-8")
    print(f"{filename} creado: {len(df):,} registros")


def weighted_choice(values, weights):
    return random.choices(values, weights=weights, k=1)[0]


# =========================
# GENERAR PRODUCTOS
# =========================
def generate_products():
    categories = {
        "Electrohogar": (500, 4500),
        "Tecnología": (200, 7000),
        "Moda": (29, 350),
        "Calzado": (80, 600),
        "Hogar": (15, 1200)
    }

    rows = []

    for i in range(1, N_PRODUCTS + 1):
        cat = random.choice(list(categories.keys()))
        min_p, max_p = categories[cat]

        rows.append({
            "product_id": f"PROD-{i:04d}",
            "product_name": f"{cat} Item {i}",
            "category": cat,
            "base_price": round(random.uniform(min_p, max_p), 2),
            "stock": random.randint(0, 500)
        })

    return pd.DataFrame(rows)


# =========================
# GENERAR CLIENTES
# =========================
def generate_customers():
    rows = []

    for i in range(1, N_CUSTOMERS + 1):
        rows.append({
            "customer_id": f"CUS-{i:06d}",
            "name": fake.name(),
            "city": random.choice([
                "Lima",
                "Arequipa",
                "Cusco",
                "Piura",
                "Trujillo"
            ]),
            "age": random.randint(18, 70)
        })

    return pd.DataFrame(rows)


# =========================
# GENERAR VENTAS
# =========================
def generate_sales(products_df):
    product_ids = products_df["product_id"].tolist()

    product_price_map = dict(
        zip(products_df["product_id"], products_df["base_price"])
    )

    product_cat_map = dict(
        zip(products_df["product_id"], products_df["category"])
    )

    payment_methods = [
        "Tarjeta CMR",
        "Débito",
        "Efectivo",
        "App Fpay"
    ]

    start_date = datetime(2026, 1, 1)
    end_date = datetime(2026, 5, 12)

    rows = []

    for i in range(1, N_SALES + 1):
        prod_id = random.choice(product_ids)

        quantity = weighted_choice(
            [1, 2, 3, 4],
            [0.7, 0.2, 0.07, 0.03]
        )

        price = product_price_map[prod_id]

        discount = 0.15 if random.random() < 0.2 else 0.0

        total = round(
            (price * quantity) * (1 - discount),
            2
        )

        sale_date = start_date + timedelta(
            seconds=random.randint(
                0,
                int((end_date - start_date).total_seconds())
            )
        )

        rows.append({
            "sale_id": f"SALE-{i:06d}",
            "date": sale_date.strftime("%Y-%m-%d %H:%M:%S"),
            "customer_id": f"CUS-{random.randint(1, N_CUSTOMERS):06d}",
            "product_id": prod_id,
            "category": product_cat_map[prod_id],
            "quantity": quantity,
            "unit_price": price,
            "discount": discount,
            "total_amount": total,
            "payment_method": random.choice(payment_methods),
            "store_location": random.choice([
                "Lima Centro",
                "Miraflores",
                "San Isidro",
                "Online"
            ])
        })

    return pd.DataFrame(rows)


# =========================
# GENERAR INVENTARIO TXT
# =========================
def generate_inventory(products_df):
    inventory_path = RAW_DIR / "inventario.txt"

    with open(inventory_path, "w", encoding="utf-8") as f:
        for _, row in products_df.iterrows():
            f.write(f"{row['product_id']},{row['stock']}\n")

    print("inventario.txt creado")


# =========================
# GENERAR LOGS
# =========================
def generate_logs():
    log_path = RAW_DIR / "logs_ventas.log"

    eventos = [
        "venta realizada",
        "pago rechazado",
        "stock bajo",
        "devolución procesada"
    ]

    with open(log_path, "w", encoding="utf-8") as f:
        for _ in range(5000):
            tipo = random.choice([
                "INFO",
                "ERROR",
                "WARNING"
            ])

            fecha = fake.date_time_between(
                start_date='-120d',
                end_date='now'
            )

            evento = random.choice(eventos)

            f.write(
                f"{fecha} [{tipo}] {evento}\n"
            )

    print("logs_ventas.log creado")


# =========================
# MAIN
# =========================
def main():
    print("Iniciando generación de datos para Proyecto Retail Big Data...")

    # PRODUCTOS
    prods_df = generate_products()
    save_csv(prods_df, "productos.csv")

    # CLIENTES (JSON)
    cust_df = generate_customers()

    clientes_json_path = RAW_DIR / "clientes.json"

    cust_df.to_json(
        clientes_json_path,
        orient="records",
        force_ascii=False,
        indent=4
    )

    print(f"clientes.json creado: {len(cust_df):,} registros")

    # VENTAS
    sales_df = generate_sales(prods_df)
    save_csv(sales_df, "ventas_principal.csv")

    # INVENTARIO
    generate_inventory(prods_df)

    # LOGS
    generate_logs()

    # RESUMEN
    print("-" * 40)
    print("Resumen de ventas por categoría:")
    print(sales_df["category"].value_counts())

    print("-" * 40)
    print("Archivos generados correctamente en /data:")
    print("✔ productos.csv")
    print("✔ clientes.json")
    print("✔ ventas_principal.csv")
    print("✔ inventario.txt")
    print("✔ logs_ventas.log")


if __name__ == "__main__":
    main()