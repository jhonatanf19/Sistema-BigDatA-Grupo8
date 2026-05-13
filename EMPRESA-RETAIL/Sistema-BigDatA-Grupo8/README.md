# Sistema Big Data Grupo 8

## Descripción del proyecto

Proyecto Big Data orientado al análisis histórico y monitoreo en tiempo real de ventas en una empresa retail tipo Falabella.

La solución integra procesamiento batch y streaming utilizando Apache Spark, Apache Kafka, MongoDB y Docker.

---

# Tecnologías utilizadas

- Apache Spark
- Apache Kafka
- MongoDB
- Hadoop HDFS
- Docker
- Python
- GitHub

---

# Funcionalidades

- Procesamiento batch de ventas históricas.
- Integración de múltiples formatos de datos.
- Streaming en tiempo real con Kafka.
- Generación de KPIs.
- Alertas automáticas.
- Visualización de resultados.

---

# Integrantes

- Alessandro Garcia Rengifo
- Jhonatan Frank Portillo Toledo
- Nicol Sharon Mendoza
- Ruth Belén Gutierrez Reyes
- Franklin Anthony Contreras Chavez

---

# Arquitectura del proyecto

```
Archivos históricos → Spark Batch → MongoDB
Kafka → Spark Streaming → MongoDB
```

---

# Estructura del proyecto

```text
Sistema-BigDatA-Grupo8/
├── data/
├── spark/
├── kafka/
├── mongodb/
├── visualizaciones/
└── docker-compose.yml
```

---

# Repositorio del proyecto

https://github.com/jhonatanf19/Sistema-BigDatA-Grupo8
