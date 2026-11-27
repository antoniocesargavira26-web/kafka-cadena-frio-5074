# Migración de ingestión batch a Event Streaming con Apache Kafka 4.0 (KRaft)

> Este repositorio es un prototipo funcional basado en Apache Kafka 4.0 en modo KRaft sobre Docker para desacoplar la ingestión de telemetría, garantizar el orden cronológico por vehículo, escalar horizontalmente el consumo de alertas con garantías at-least-once y dejar preparada la base para su futura integración con Apache Spark Structured Streaming.

---

## 1. Arquitectura y Estructura del Repositorio

* Descripción resumida del flujo de datos (`Productor Python` -> `Topic telemetria-frio [3 particiones]` -> `Consumer Group grupo-alertas-frio` -> `Log de alertas`).

* Árbol de directorios del proyecto:

```text
kafka-cadena-frio-5074/
├── docker-compose.yml
├── requirements.txt
├── README.md
├── src/
│   ├── productor_frio.py
│   └── consumidor_alertas.py
└── evidencias/
    ├── E1_topic_describe.png
    ├── E2_productor_salida.txt
    ├── E3A_group_antes_fallo.png
    ├── E3B_group_despues_fallo.png
    └── alertas_detectadas.log
```

---

## 2. Prerrequisitos e Instalación del Entorno

### 2.1. Requisitos previos
* Lista de software necesario: 
    Docker Desktop / Compose v2. 
    Python 3.10 o superior.
    Git y cuenta activa en GitHub.
    Dos o más ventanas de terminal para simular ejecuciones concurrentes.

### 2.2. Preparación del entorno Python
1. **Crear el entorno virtual ('venv')** en la raíz del proyecto:
```bash
python -m venv .venv
```

2. Comando para instalar dependencias desde `requirements.txt` (`pip install -r requirements.txt`).

---

## 3. Guía de Reproducción Paso a Paso

### 3.1. Despliegue de Apache Kafka 4.0 (KRaft) y creación del Topic
* Comando para levantar el contenedor (`docker compose up -d`).
* Comandos CLI (`kafka-topics.sh`) para crear e inspeccionar el topic `telemetria-frio` (3 particiones, factor de replicación 1).

### 3.2. Ejecución de Consumidores y Productor (Prueba en caliente)
* **Terminal 1 y 2:** Comando para arrancar las dos instancias de `src/consumidor_alertas.py`.
* **Terminal 3:** Comando de inspección inicial del grupo (`kafka-consumer-groups.sh --describe`).
* **Terminal 3:** Comando para lanzar `src/productor_frio.py`.
* Instrucciones de simulación de caída (`Ctrl + C` en uno de los consumidores a mitad de emisión) e inspección final del grupo (`LAG = 0`).

### 3.3. Índice de Evidencias Generadas
* Tabla o lista enlazando cada archivo guardado en `evidencias/` (`E1_topic_describe`, `E2_productor_salida`, `E3A_group_antes_fallo`, `E3B_group_despues_fallo`, `alertas_detectadas.log`) con lo que demuestra.

---

## 4. Análisis Arquitectónico

### 4.1. HDFS frente a Kafka: Solución al *Small Files Problem*
* `[Tu argumentación aquí: explica cómo almacena metadatos el NameNode de HDFS frente a los logs secuenciales/segmentos de Kafka y cómo se desacopla la ingesta antes de persistir en batch/micro-batch]`.

### 4.2. Claves de partición, orden cronológico y escalado de consumidores
* **Impacto de eliminar `key`:** `[Tu argumentación aquí: estrategia de particionado por defecto sin clave y efecto sobre la garantía de orden por camión]`.
* **Rol de una 4.ª instancia con 3 particiones:** `[Tu argumentación aquí: relación máxima 1:1 entre particiones y consumidores activos dentro de un mismo Consumer Group y estado de la instancia sobrante]`.

### 4.3. Puente hacia Apache Spark Structured Streaming (Listeners duales)
* `[Tu argumentación aquí: explicación de la separación entre listener externo e interno en Docker y la cadena exacta que debe configurarse en KAFKA_ADVERTISED_LISTENERS y variables complementarias para permitir localhost:9092 y kafka:9092]`.

---

## 5. Referencias (Formato IEEE)

* `[1]` Autor/Organización, "Título del documento o documentación oficial," *Plataforma/Editorial*, Año. [En línea]. Disponible en: `<URL verificable>`. [Accedido: Día-Mes-Año].
* `[2]` ...