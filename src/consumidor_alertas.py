import json
import os
from kafka import KafkaConsumer

# Configuración del clúster y del Consumer Group
BOOTSTRAP_SERVERS = ["localhost:9092"]
TOPIC = "telemetria-frio"
GROUP_ID = "grupo-alertas-frio"
UMBRAL_ANOMALIA = -18.0
RUTA_LOG_ALERTAS = os.path.join("evidencias", "alertas_detectadas.log")


def crear_consumidor() -> KafkaConsumer:
    return KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )


def main():
    # Asegura que el directorio 'evidencias' exista antes de abrir el log
    os.makedirs(os.path.dirname(RUTA_LOG_ALERTAS), exist_ok=True)

    consumer = crear_consumidor()
    print(f"Consumidor iniciado | Topic: '{TOPIC}' | Group: '{GROUP_ID}'")
    print("Esperando mensajes... (Pulsa Ctrl + C para detener)\n" + "-" * 75)

    try:
        # Abrimos el archivo de log en modo append ('a') con codificación UTF-8
        with open(RUTA_LOG_ALERTAS, mode="a", encoding="utf-8") as log_file:
            for mensaje in consumer:
                particion = mensaje.partition
                offset = mensaje.offset
                clave = mensaje.key
                evento = mensaje.value

                vehiculo = evento.get("vehiculo", clave)
                temp_c = evento.get("temp_c")
                lat = evento.get("lat")
                lon = evento.get("lon")
                timestamp_iso = evento.get("timestamp_iso")

                # 1. Trazabilidad en consola para cada mensaje recibido
                print(
                    f"[Partición: {particion} | Offset: {offset:>3}] "
                    f"Vehículo: {vehiculo} | Temp: {temp_c:>5.1f} °C"
                )

                # 2. Detección de rotura de cadena de frío (temp_c > -18.0)
                if temp_c is not None and temp_c > UMBRAL_ANOMALIA:
                    print(
                        f"  >>> ¡ALERTA CRÍTICA DE CADENA DE FRÍO! "
                        f"{vehiculo} alcanzó {temp_c:.1f} °C (Partición {particion}, Offset {offset}) <<<"
                    )

                    linea_log = (
                        f"ALERTA | Partición: {particion} | Offset: {offset} | "
                        f"Vehículo: {vehiculo} | Temp: {temp_c:.1f} °C | "
                        f"Lat: {lat} | Lon: {lon} | Timestamp: {timestamp_iso}\n"
                    )
                    log_file.write(linea_log)
                    log_file.flush()

                # 3. Confirmación manual de offset tras procesar y persistir (at-least-once)
                consumer.commit()

    except KeyboardInterrupt:
        print("\nConsumidor detenido manualmente (Ctrl + C).")
    finally:
        consumer.close()
        print("Conexión del consumidor cerrada limpiamente.")


if __name__ == "__main__":
    main()