import json
import random
import time
import argparse
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd
from confluent_kafka import Producer

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RetailStreamingProducer:
    """Productor Kafka para simular transacciones de Retail en tiempo real."""

    def __init__(self, bootstrap_servers="broker:19092", topic="retail-events"):
        self.topic = topic
        self.conf = {'bootstrap.servers': bootstrap_servers}
        self.producer = Producer(self.conf)
        
        # Configuración de rutas
        self.base_dir = Path(__file__).resolve().parents[1]
        self.raw_dir = self.base_dir / "data" / "raw"
        
        # Tipos de eventos retail
        self.event_types = [
            "intento_pago", "pago_exitoso", "orden_despachada", 
            "devolucion_solicitada", "carrito_abandonado"
        ]
        self.event_weights = [0.15, 0.50, 0.20, 0.05, 0.10]

    def delivery_report(self, err, msg):
        """Callback para confirmar la recepción del mensaje en Kafka."""
        if err is not None:
            logger.error(f"Fallo en entrega de mensaje: {err}")
        else:
            # Solo logueamos cada 50 mensajes para no saturar
            pass

    def load_context_data(self):
        """Carga datos de los CSV generados en el script 01 para dar coherencia."""
        try:
            catalog = pd.read_csv(self.raw_dir / "catalog.csv")
            customers = pd.read_csv(self.raw_dir / "customers.csv")
            return catalog, customers
        except FileNotFoundError:
            logger.error("No se encontraron archivos maestros. Ejecute el script 01 primero.")
            exit(1)

    def create_retail_event(self, event_id, catalog, customers):
        """Genera un JSON de evento de venta con lógica de negocio."""
        product = catalog.sample(1).iloc[0]
        customer = customers.sample(1).iloc[0]
        event_type = random.choices(self.event_types, weights=self.event_weights)[0]
        
        # Simular alerta si es una venta de alto valor (Big Data Analytics)
        is_high_value = product['base_price'] > 2000
        
        return {
            "event_id": f"EVT-RT-{event_id:07d}",
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "customer_id": customer['customer_id'],
            "customer_segment": customer['segment'],
            "sku": product['product_sku'],
            "category": product['category'],
            "amount": float(product['base_price']),
            "channel": random.choice(["Web", "App Mobile", "Kiosko"]),
            "is_high_value_alert": bool(is_high_value)
        }

    def start_streaming(self, n_events=1000, delay=0.5):
        """Inicia el ciclo de envío de eventos."""
        catalog, customers = self.load_context_data()
        logger.info(f"Iniciando streaming de {n_events} eventos hacia topic: {self.topic}...")

        try:
            for i in range(1, n_events + 1):
                event = self.create_retail_event(i, catalog, customers)
                
                # Enviar a Kafka
                self.producer.produce(
                    topic=self.topic,
                    key=event["customer_id"],
                    value=json.dumps(event).encode('utf-8'),
                    callback=self.delivery_report
                )
                
                self.producer.poll(0) # Atender callbacks
                
                if i % 100 == 0:
                    logger.info(f"Progreso: {i}/{n_events} eventos enviados.")
                
                time.sleep(delay)

        except KeyboardInterrupt:
            logger.warning("Streaming interrumpido por el usuario.")
        finally:
            self.producer.flush()
            logger.info("Productor cerrado y mensajes enviados.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Retail Event Generator")
    parser.add_argument("--events", type=int, default=1000)
    parser.add_argument("--delay", type=float, default=0.2)
    args = parser.parse_args()

    producer = RetailStreamingProducer()
    producer.start_streaming(n_events=args.events, delay=args.delay)