import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

# Configuración del clúster y simulación
BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "telemetria-frio"
VEHICULOS = ["CAM-01", "CAM-02", "CAM-03", "CAM-04"]
TOTAL_EVENTOS = 60
INTERVALO_SEGUNDOS = 0.5
UMBRAL_ANOMALIA = -18.0

# Coordenadas base en torno a Sevilla (coherentes con el ejemplo del enunciado)
UBICACIONES_BASE = {
    "CAM-01": (37.3891, -5.9845),
    "CAM-02": (37.3925, -5.9912),
    "CAM-03": (37.3750, -5.9680),
    "CAM-04": (37.4010, -5.9755),
}


def crear_productor() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks="all",
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def main():
    producer = crear_productor()

    total_anomalias_objetivo = 8
    indices_anomalos = set(random.sample(range(TOTAL_EVENTOS), total_anomalias_objetivo))

    anomalias_emitidas = 0

    print(f"Iniciando simulación de telemetría hacia el topic '{TOPIC}'...")
    print("-" * 75)

    try:
        for i in range(TOTAL_EVENTOS):
            # Rotación determinista entre los 4 vehículos (15 eventos por camión)
            vehiculo = VEHICULOS[i % len(VEHICULOS)]
            es_anomalia = i in indices_anomalos

            if es_anomalia:
                # Temperatura > -18.0 °C (rotura de cadena de frío)
                temp_c = round(random.uniform(-17.5, -14.0), 1)
                anomalias_emitidas += 1
            else:
                # Temperatura normal de congelación (-22.0 °C a -18.5 °C)
                temp_c = round(random.uniform(-22.0, -18.5), 1)

            # Simulación de ligero desplazamiento GPS
            lat_base, lon_base = UBICACIONES_BASE[vehiculo]
            lat = round(lat_base + random.uniform(-0.005, 0.005), 4)
            lon = round(lon_base + random.uniform(-0.005, 0.005), 4)

            # Timestamp actual en formato ISO 8601 UTC (YYYY-MM-DDTHH:MM:SSZ)
            timestamp_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            evento = {
                "vehiculo": vehiculo,
                "temp_c": temp_c,
                "lat": lat,
                "lon": lon,
                "timestamp_iso": timestamp_iso,
            }

            # Envío síncrono (.get) para obtener partición y offset reales del broker
            future = producer.send(TOPIC, key=vehiculo, value=evento)
            record_metadata = future.get(timeout=10)

            estado = " [ALERTA > -18.0°C]" if es_anomalia else ""
            print(
                f"[{i + 1:02d}/{TOTAL_EVENTOS}] Vehículo: {vehiculo} | "
                f"Temp: {temp_c:>5.1f} °C | "
                f"Partición: {record_metadata.partition} | "
                f"Offset: {record_metadata.offset}{estado}"
            )

            time.sleep(INTERVALO_SEGUNDOS)

    except KeyboardInterrupt:
        print("\nSimulación interrumpida manualmente por el usuario.")
    finally:
        producer.flush()
        producer.close()
        print("-" * 75)
        print(f"Resumen final: {TOTAL_EVENTOS} eventos enviados.")
        print(f"Total de anomalías emitidas (temp_c > -18.0 °C): {anomalias_emitidas}")


if __name__ == "__main__":
    main()