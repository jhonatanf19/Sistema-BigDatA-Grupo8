import json
import random
import time
import argparse
import logging
from datetime import datetime
from pathlib import Path
import pandas as pd

# =========================
# CONFIGURACIÓN LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


class RetailStreamingProducer:
    """Generador de eventos retail en tiempo real (modo local sin Kafka)."""

    def __init__(self, topic="retail-events"):
        self.topic = topic

        # =========================
        # KAFKA DESACTIVADO TEMPORALMENTE
        # =========================
        self.producer = None

        # =========================
        # RUTAS
        # =========================
        self.base_dir = Path(__file__).resolve().parents[1]
        self.raw_dir = self.base_dir / "data"

        # =========================
        # EVENTOS OBLIGATORIOS
        # =========================
        self.event_types = [
            "venta_realizada",
            "stock_bajo",
            "pago_rechazado",
            "devolucion_producto"
        ]

        self.event_weights = [
            0.55,
            0.15,
            0.20,
            0.10
        ]

    # =========================
    # CARGAR DATOS BASE
    # =========================
    def load_context_data(self):
        try:
            productos = pd.read_csv(
                self.raw_dir / "productos.csv"
            )

            clientes = pd.read_json(
                self.raw_dir / "clientes.json"
            )

            return productos, clientes

        except FileNotFoundError:
            logger.error(
                "No se encontraron productos.csv o clientes.json. Ejecuta primero el script 01."
            )
            exit(1)

    # =========================
    # GENERAR EVENTO
    # =========================
    def create_retail_event(self, event_id, productos, clientes):
        producto = productos.sample(1).iloc[0]
        cliente = clientes.sample(1).iloc[0]

        event_type = random.choices(
            self.event_types,
            weights=self.event_weights,
            k=1
        )[0]

        stock_actual = random.randint(
            0,
            max(1, int(producto["stock"]))
        )

        is_high_value = producto["base_price"] > 2000

        return {
            "event_id": f"EVT-RT-{event_id:07d}",
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,

            # CLIENTE
            "customer_id": cliente["customer_id"],
            "customer_name": cliente["name"],
            "customer_city": cliente["city"],
            "customer_age": int(cliente["age"]),

            # PRODUCTO
            "product_id": producto["product_id"],
            "product_name": producto["product_name"],
            "category": producto["category"],

            # TRANSACCIÓN
            "amount": float(producto["base_price"]),
            "stock": stock_actual,

            "channel": random.choice([
                "Web",
                "App Mobile",
                "Tienda Física"
            ]),

            # ALERTAS
            "is_high_value_alert": bool(is_high_value),
            "is_stock_alert": bool(stock_actual < 10)
        }

    # =========================
    # KAFKA DESACTIVADO
    # =========================
    def safe_kafka_send(self, event):
        return

    # =========================
    # STREAMING
    # =========================
    def start_streaming(self, n_events=1000, delay=0.2):
        productos, clientes = self.load_context_data()

        logger.info(
            f"Iniciando generación de {n_events} eventos retail..."
        )

        # =========================
        # ARCHIVO LOCAL JSON
        # =========================
        output_events_path = self.raw_dir / "streaming_eventos.json"

        try:
            with open(output_events_path, "w", encoding="utf-8") as outfile:

                for i in range(1, n_events + 1):
                    event = self.create_retail_event(
                        i,
                        productos,
                        clientes
                    )

                    # =========================
                    # KAFKA DESACTIVADO
                    # =========================
                    self.safe_kafka_send(event)

                    # =========================
                    # GUARDAR EN JSON
                    # =========================
                    outfile.write(
                        json.dumps(
                            event,
                            ensure_ascii=False
                        ) + "\n"
                    )

                    # =========================
                    # LOG PROGRESO
                    # =========================
                    if i % 100 == 0:
                        logger.info(
                            f"Progreso: {i}/{n_events} eventos generados."
                        )

                    time.sleep(delay)

        except KeyboardInterrupt:
            logger.warning(
                "Generación interrumpida por el usuario."
            )

        finally:
            pass

        logger.info(
            "Proceso finalizado. Eventos guardados correctamente en streaming_eventos.json"
        )


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Retail Event Generator"
    )

    parser.add_argument(
        "--events",
        type=int,
        default=1000
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=0.2
    )

    args = parser.parse_args()

    producer = RetailStreamingProducer()

    producer.start_streaming(
        n_events=args.events,
        delay=args.delay
    )