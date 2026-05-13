import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

plt.style.use('ggplot')
sns.set_palette("husl")

BASE_DIR = Path(__file__).resolve().parents[1]
KPI_DIR = BASE_DIR / "output" / "kpis"
CHARTS_DIR = BASE_DIR / "output" / "charts"
CHARTS_DIR.mkdir(parents=True, exist_ok=True)

def load_kpi(filename):
    path = KPI_DIR / f"{filename}_temp"
    csv_file = list(path.glob("part-*.csv"))[0]
    return pd.read_csv(csv_file)

def save_plot(name):
    plt.tight_layout()
    plt.savefig(CHARTS_DIR / f"{name}.png", dpi=150)
    plt.close()
    print(f"Grafico exportado: {name}.png")

# 
def plot_market_share():
    df = load_kpi("kpi_categorias")
    
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.barplot(data=df, x='category', y='ingresos_totales', ax=ax)
    
    plt.title("Ingresos Totales por Categoría (Market Share)", fontsize=14, fontweight='bold')
    plt.xlabel("Categoría de Producto")
    plt.ylabel("Ventas Totales (S/.)")
    
    for p in ax.patches:
        ax.annotate(f'S/.{p.get_height():,.0f}', 
                    (p.get_x() + p.get_width() / 2., p.get_height()), 
                    ha='center', va='center', xytext=(0, 9), 
                    textcoords='offset points', fontsize=9)
    
    save_plot("market_share_categorias")

def plot_ticket_vs_discounts():
    df = load_kpi("kpi_categorias")
    
    plt.figure(figsize=(10, 6))
    plt.scatter(df['ticket_promedio'], df['total_descuentos_otorgados'], s=df['num_ventas']*2, alpha=0.6)
    
    # Anotar puntos
    for i, txt in enumerate(df['category']):
        plt.annotate(txt, (df['ticket_promedio'][i], df['total_descuentos_otorgados'][i]))

    plt.title("Relación Ticket Promedio vs Inversión en Descuentos", fontsize=12)
    plt.xlabel("Ticket Promedio (S/.)")
    plt.ylabel("Inversión en Descuentos (S/.)")
    
    save_plot("eficiencia_descuentos")

# 
def plot_channel_performance():
    df = load_kpi("kpi_canales")
    
    plt.figure(figsize=(8, 8))
    plt.pie(df['venta_neta'], labels=df['channel'], autopct='%1.1f%%', 
            startangle=140, colors=['#ff9999','#66b3ff','#99ff99'])
    
    plt.title("Distribución de Ventas por Canal de Venta", fontsize=14, fontweight='bold')
    save_plot("distribucion_canales")


if __name__ == "__main__":
    print(">>> Iniciando Motor de Visualización Big Data...")
    try:
        plot_market_share()
        plot_channel_performance()
        plot_ticket_vs_discounts()
        print(">>> Proceso finalizado. Los gráficos están listos para el informe.")
    except Exception as e:
        print(f"Error: Asegúrate de que el script 03_batch_etl_retail_spark.py se haya ejecutado. Detalle: {e}")