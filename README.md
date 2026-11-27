# Migración de ingestión batch a Event Streaming con Apache Kafka 4.0 (KRaft)

> Prototipo funcional basado en Apache Kafka 4.0 en modo KRaft sobre Docker para desacoplar la ingestión de telemetría de FríoSur Logística S. L., garantizar el orden cronológico por vehículo, escalar horizontalmente el consumo de alertas con garantías *at-least-once* y dejar preparada la arquitectura para su futura integración con Apache Spark Structured Streaming.

---

## 1. Arquitectura y Estructura del Repositorio

* El flujo de datos sigue una arquitectura desacoplada orientada a eventos: 

`Productor Python` -> `Topic telemetria-frio [3 particiones]` -> `Consumer Group grupo-alertas-frio` -> `Log de alertas`.

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
```powershell
.\.venv\Scripts\Activate.ps1
```
2. Comando para instalar dependencias desde `requirements.txt`:
```bash
    pip install -r requirements.txt
```
---

## 3. Guía de Reproducción 

### 3.1. Despliegue de Apache Kafka 4.0 (KRaft) y creación del Topic
* Comando para levantar el contenedor:
```bash
docker compose up -d
```
* Comandos CLI (`kafka-topics.sh`) para crear e inspeccionar el topic `telemetria-frio` (3 particiones, factor de replicación 1):

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --topic telemetria-frio --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic telemetria-frio
```

### 3.2. Ejecución de Consumidores y Productor 
* **Terminal 1 y 2:** Comando para arrancar las dos instancias de `src/consumidor_alertas.py`:
```bash
.\.venv\Scripts\Activate.ps1
python src/consumidor_alertas.py
```
* **Terminal 3:** Comando de inspección inicial del grupo (`kafka-consumer-groups.sh --describe`):
```bash
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group grupo-alertas-frio
```
* **Terminal 3:** Comando para lanzar `src/productor_frio.py`:
```bash
.\.venv\Scripts\Activate.ps1
python src/productor_frio.py
```
* Instrucciones de simulación de caída (`Ctrl + C` en uno de los consumidores a mitad de emisión) e inspección final del grupo (`LAG = 0` al usar otra vez el comando inicial de **Terminal 3**).

### 3.3. Advertencia
Al ejecutar el comando de los códigos python es posible que surja un error. Dependiende de la instalación, alterne entre **py** y **python** (Ej: de `python src/productor_frio.py` a `py src/productor_frio.py` )

---

## 4. Análisis Arquitectónico

### 4.1. HDFS frente a Kafka: Solución al *Small Files Problem*
* En la arquitectura heredada de FríoSur Logística, cada lectura de telemetría generaba un pequeño fichero CSV independiente cada minuto en HDFS. Dado que el **NameNode** mantiene en memoria RAM todo el espacio de nombres del sistema de ficheros (inodos, bloques y ubicaciones), cada objeto consume aproximadamente **150 bytes** de memoria independientemente de su tamaño real en disco [2]. La creación masiva de miles de ficheros diminutos diarios satura la memoria del NameNode, incrementa la latencia de sus operaciones RPC y degrada el rendimiento de los trabajos de lectura por el elevado número de operaciones *seek* y apertura de ficheros [2].

**Apache Kafka** resuelve este cuello de botella mediante tres mecanismos clave [1]:

1. **Almacenamiento secuencial basado en *Commit Logs*:** En lugar de crear un fichero por evento, Kafka concatena los mensajes de forma secuencial  dentro de ficheros de segmento contiguos en disco (por defecto de hasta 1 GB por segmento de partición), aprovechando la velocidad de escritura secuencial del sistema operativo y la caché de páginas sin sobrecargar el sistema de archivos con miles de inodos [1].
2. **Desacoplamiento entre ingesta y almacenamiento analítico:** Kafka actúa como un búfer distribuido y persistente que absorbe los picos de telemetría en tiempo real a baja latencia, eliminando la escritura directa de los camiones sobre HDFS [1].
3. **Compactación en escrituras *micro-batch* (Parquet):** Al integrar posteriormente Apache Spark Structured Streaming [3], los eventos acumulados en Kafka se consumen por ventanas de tiempo y se consolidan en un único fichero columnar **Parquet** de gran tamaño (cercano al tamaño de bloque HDFS de 128 MB [2]), reduciendo drásticamente la huella de metadatos en el NameNode.

### 4.2. Claves de partición, orden cronológico y escalado de consumidores
* **Impacto de eliminar `key`:** Si eliminamos el parámetro `key` del productor, Kafka deja de calcular el hash determinista sobre el identificador del vehículo y recurre a su particionador por defecto (*Uniform Sticky Partitioner*), que reparte los lotes de mensajes entre las 3 particiones para equilibrar la carga [1]. Esto provoca que las lecturas sucesivas de un mismo camión aterricen en particiones distintas. Dado que Kafka **solo garantiza el orden cronológico estricto (FIFO) dentro de una misma partición** [1], los consumidores procesarían los eventos de un vehículo desordenados en el tiempo, impidiendo reconstruir con precisión su evolución térmica o determinar el instante exacto de una rotura de la cadena de frío.

* **Rol de una 4.ª instancia con 3 particiones:** En Apache Kafka, una partición solo puede ser consumida por **un único consumidor activo** dentro del mismo *Consumer Group* [1]. Al existir **3 particiones** en `telemetria-frio` y conectar **4 instancias** bajo `grupo-alertas-frio`, tres consumidores asumirán una partición cada uno y la **4.ª instancia quedará inactiva**, sin recibir tráfico [1]. Sin embargo, cumple una función de **reserva pasiva**: ante la caída de cualquiera de los tres consumidores activos, el coordinador del grupo ejecutará un reequilibrio y le asignará de inmediato la partición libre para mantener el consumo sin interrupciones [1].

### 4.3. Puente hacia Apache Spark Structured Streaming
En un entorno Docker, los clientes que se ejecutan en la máquina anfitriona (nuestros scripts Python) resuelven el broker mediante `localhost`, mientras que los contenedores desplegados en la misma red interna de Docker (como **Apache Spark Structured Streaming** [3]) se comunican mediante el nombre DNS del servicio (`kafka`). Si Kafka anunciara únicamente `localhost:9092`, Spark recibiría esa dirección tras el primer contacto e intentaría conectarse a su propio contenedor local, fallando la conexión.

Para que ambos accesos coexistan simultáneamente, es necesario definir **dos *listeners* diferenciados** (uno interno para la red Docker y otro externo mapeado al puerto del *host*) mediante las siguientes variables en el `docker-compose.yml` [1]:

* **`KAFKA_ADVERTISED_LISTENERS` (cambio exacto requerido):**
```yaml
  KAFKA_ADVERTISED_LISTENERS: 'PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:9092'
```
---

## 5. Referencias

* `[1]` Apache Software Foundation, “Apache Kafka Documentation”, Apache Kafka. [En línea]. Disponible en: https://kafka.apache.org/documentation/
* `[2]` Apache Software Foundation, “HDFS Architecture Guide”, Apache Hadoop Documentation. [En línea]. Disponible en: https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html
* `[3]` Apache Software Foundation, "Structured Streaming + Kafka Integration Guide," Apache Spark 3.5.0 Documentation, 2024. [En línea]. Disponible en: https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html.