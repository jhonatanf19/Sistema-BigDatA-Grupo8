import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

# =========================
# ESTILO
# =========================
plt.style.use("ggplot")
sns.set_palette("husl")

# =========================
# RUTAS
# =========================
BASE_DIR = Path(__file__).resolve().parents[1]

OUTPUT_DIR = BASE_DIR / "output"
VIS_DIR = OUTPUT_DIR / "visualizations"

VIS_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# CARGA CSV
# =========================
def load_csv(filename):
    file_path = OUTPUT_DIR / filename

    if not file_path.exists():
        raise FileNotFoundError(f"No existe: {file_path}")

    return pd.read_csv(file_path)


# =========================
# GUARDAR
# =========================
def save_plot(name):
    plt.tight_layout()
    plt.savefig(VIS_DIR / f"{name}.png", dpi=150)
    plt.close()
    print(f"✔ Gráfico generado: {name}.png")


# =========================
# KPI CIUDADES
# =========================
def plot_sales_by_city():
    df = load_csv("kpis_batch.csv")

    plt.figure(figsize=(10, 6))

    sns.barplot(
        data=df,
        x="city",
        y="ingresos_ciudad"
    )

    plt.title(
        "Ventas Totales por Ciudad",
        fontsize=14,
        fontweight="bold"
    )

    plt.xlabel("Ciudad")
    plt.ylabel("Ingresos")

    plt.xticks(rotation=45)

    save_plot("ventas_por_ciudad")


# =========================
# KPI CATEGORÍAS
# =========================
def plot_top_categories():
    df = load_csv("kpi_categorias.csv")

    plt.figure(figsize=(10, 6))

    sns.barplot(
        data=df,
        x="category",
        y="ingresos_totales"
    )

    plt.title(
        "Top Categorías por Ingresos",
        fontsize=14,
        fontweight="bold"
    )

    plt.xlabel("Categoría")
    plt.ylabel("Ingresos Totales")

    plt.xticks(rotation=45)

    save_plot("top_categorias")


# =========================
# STREAMING EVENTOS
# =========================
def plot_streaming_events():
    file_path = BASE_DIR / "data" / "streaming_eventos.jsonl"

    df = pd.read_json(
        file_path,
        lines=True
    )

    event_counts = (
        df["event_type"]
        .value_counts()
        .reset_index()
    )

    event_counts.columns = [
        "event_type",
        "count"
    ]

    plt.figure(figsize=(10, 6))

    sns.barplot(
        data=event_counts,
        x="event_type",
        y="count"
    )

    plt.title(
        "Distribución de Eventos Streaming",
        fontsize=14,
        fontweight="bold"
    )

    plt.xlabel("Tipo de Evento")
    plt.ylabel("Cantidad")

    plt.xticks(rotation=45)

    save_plot("eventos_streaming")


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    print(">>> Iniciando visualizaciones Big Data...")

    try:
        plot_sales_by_city()
        plot_top_categories()
        plot_streaming_events()

        print(">>> VISUALIZACIONES COMPLETADAS")
        print("✔ ventas_por_ciudad.png")
        print("✔ top_categorias.png")
        print("✔ eventos_streaming.png")

    except Exception as e:
        print(f"Error: Verifica outputs del ETL. Detalle: {e}")