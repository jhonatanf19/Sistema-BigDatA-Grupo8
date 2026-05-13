## 1. Tecnologías usadas

- Docker
- Python
- Apache Spark / PySpark
- Spark SQL
- RDD
- DataFrames
- Kafka
- CSV
- JSON
- Parquet
- Matplotlib
- MongoDB como extensión opcional

## 2. Resultados esperados

- Pedidos por distrito
- Tiempo promedio de entrega
- Pedidos retrasados
- Cancelaciones por distrito
- Ranking de repartidores
- Alertas en tiempo real
- Reportes CSV
- Archivos Parquet
- Gráficos PNG

## 3. Estructura del proyecto

```text
rapidex-bigdata-streaming/
├── data/
│   ├── raw/
│   ├── processed/
│   └── checkpoints/
├── docs/
├── notebooks/
├── output/
│   ├── charts/
│   ├── kpis/
│   └── streaming/
├── src/
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
