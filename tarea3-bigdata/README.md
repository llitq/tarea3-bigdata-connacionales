# Procesamiento batch con Apache Spark: colombianos inscritos en el exterior

Tarea 3 – Procesamiento de Datos con Apache Spark · Curso Big Data (código 202016911) · UNAD

**Autor:** Danny Alejandro Robayo Vasquez
**Video del análisis batch:** https://youtu.be/RMMQDaiIM9o?si=lw1WUP_moauFlHt6
**Video del análisis en tiempo real (Kafka + Spark Streaming):** https://youtu.be/DmoyiPvtVO0?si=j_zWrwyIvbFIaYa0

---

## 1. Descripción del problema

¿Cómo ha evolucionado el registro de colombianos en el exterior entre 2013 y 2026, y cuál es su perfil por país de residencia, nivel académico y área de conocimiento?

Para responderlo se construyó un proceso batch en **PySpark** que carga el dataset desde **HDFS**, lo limpia y transforma, realiza un análisis exploratorio con **RDD** y **DataFrames**, genera gráficas y guarda el resultado limpio en formato **Parquet**.

## 2. Dataset

- **Nombre:** Connacionales inscritos en el Registro Ciudadano en Línea
- **Fuente:** Portal de Datos Abiertos de Colombia (datos.gov.co) – https://www.datos.gov.co/Estad-sticas-Nacionales/Colombianos-en-el-exterior-de-25-a-40-a-os/3k8j-4d42
- **Tamaño:** 639.041 filas y 14 columnas (~121 MB en CSV)
- **Columnas:** país, código ISO, ciudad de residencia, oficina consular, edad, área de conocimiento, nivel académico, estado civil, sexo, pertenencia étnica, ciudad de nacimiento, localización (coordenadas), fecha de registro y cantidad de personas.

> El archivo CSV **no se incluye** en este repositorio por su tamaño. Se descarga desde el portal (opción *Exportar → CSV*).

## 3. Solución

Archivo principal: [`tarea3_batch.py`](tarea3_batch.py)

| Etapa | Qué hace |
|---|---|
| **Carga** | Lee el CSV desde HDFS (`hdfs://localhost:9000/Tarea3/connacionales.csv`) y renombra las columnas |
| **Limpieza** | Unifica valores "sin dato" escritos de varias formas, convierte tipos, parsea fechas y descarta las de año 1900 (inválidas) |
| **Transformación** | Separa la localización en latitud y longitud, divide el lugar de nacimiento en país, departamento y municipio, y crea las columnas `grupo_edad` y `nivel_agrupado` |
| **Análisis con RDD** | Suma de personas por país con `map` y `reduceByKey` |
| **Análisis con DataFrames** | Agregaciones por año, país, sexo, grupo de edad, nivel académico y área de conocimiento; porcentaje de formación universitaria por país |
| **Visualización** | 5 gráficas con matplotlib (carpeta `graficas_tarea3/`) |
| **Almacenamiento** | Dataset limpio en HDFS, formato Parquet particionado por año |

### Decisiones de limpieza
- Los valores `SIN INFORMACIÓN`, `NO INDICA`, `(NO REGISTRA)`, `DESCONOCIDO` y `SIN ETNIA REGISTRADA` se unifican como `SIN DATO`.
- `NINGUNA` y `NINGUNO` **no** se unifican: pueden ser respuestas válidas (por ejemplo, sin pertenencia étnica o sin estudios).
- Cada fila puede representar más de una persona, por eso los conteos usan la **suma** de la columna `cantidad`, no `count()`.

## 4. Requisitos

- Ubuntu 22.04 (máquina virtual en VirtualBox)
- Java 11
- Hadoop 3.3.6 con HDFS en `localhost:9000`
- Python 3.10 y PySpark 3.5.x
- matplotlib

Instalación de las librerías de Python:

```bash
pip install -r requirements.txt
# o, para matplotlib en Ubuntu:
sudo apt install -y python3-matplotlib
```

## 5. Instrucciones de ejecución

1. **Iniciar Hadoop** (como usuario `hadoop`) y comprobar los procesos:
   ```bash
   start-all.sh
   jps
   ```
2. **Descargar el dataset** desde datos.gov.co, renombrarlo a `connacionales.csv` y copiarlo a la VM.
3. **Cargarlo a HDFS:**
   ```bash
   hdfs dfs -mkdir -p /Tarea3
   hdfs dfs -put connacionales.csv /Tarea3
   hdfs dfs -chmod 777 /Tarea3      # solo si el script se ejecuta con otro usuario
   hdfs dfs -ls -h /Tarea3
   ```
4. **Ejecutar el análisis:**
   ```bash
   python3 tarea3_batch.py | tee salida_batch.txt
   ```
5. **Resultados:**
   - Salida de consola: `salida_batch.txt`
   - Gráficas: `~/graficas_tarea3/`
   - Dataset limpio: `hdfs dfs -ls /Tarea3/connacionales_limpio`

## 6. Resultados

| Indicador | Valor |
|---|---|
| Filas cargadas | 639.041 |
| Filas con fecha inválida (descartadas) | 156 |
| Filas después de la limpieza | 638.885 |

**Países con más inscritos:** España (212.243), Estados Unidos (179.747), Ecuador (37.923), Chile (25.880) y Australia (20.075).

**Evolución por año:** el registro crece desde 2013, con una caída en 2020 y un aumento marcado desde 2022. El año 2026 es parcial.

Gráficas generadas:

| | |
|---|---|
| ![Inscritos por año](graficas_tarea3/1_inscritos_por_anio.png) | ![Top 10 países](graficas_tarea3/2_top10_paises.png) |
| ![Nivel académico](graficas_tarea3/3_nivel_academico.png) | ![Áreas de conocimiento](graficas_tarea3/4_top10_areas.png) |
| ![Formación universitaria por país](graficas_tarea3/5_pct_universitario_pais.png) | |

## 7. Limitaciones

- El dataset solo incluye personas de **25 a 40 años**, por lo que el análisis no describe a toda la población colombiana en el exterior.
- **2026 es un año parcial**, así que no es comparable con los años completos.
- Una parte importante de los registros no reporta nivel académico o área de conocimiento.
- Las áreas de conocimiento mezclan dos clasificaciones distintas, con categorías que se solapan (por ejemplo, salud y ciencias de la salud).
- El montaje es de un solo nodo en una máquina virtual con memoria limitada.

## 8. Procesamiento en tiempo real (Kafka + Spark Streaming)

La carpeta [`streaming/`](streaming/) contiene el desarrollo del Anexo 3: un productor que simula sensores y un consumidor de Spark Structured Streaming que calcula promedios por ventanas de un minuto.

1. Iniciar ZooKeeper y Kafka (Kafka 3.9.2) y crear el topic:
   ```bash
   /opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
       --replication-factor 1 --partitions 1 --topic sensor_data
   ```
2. En una terminal, el productor:
   ```bash
   python3 streaming/kafka_producer.py
   ```
3. En otra terminal, el consumidor (la versión del paquete debe coincidir con la de `spark-submit`):
   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<VERSION> \
       streaming/spark_streaming_consumer.py
   ```

## 9. Estructura del repositorio

```
├── README.md
├── requirements.txt
├── tarea3_batch.py
├── salida_batch.txt
├── graficas_tarea3/
└── streaming/
    ├── kafka_producer.py
    └── spark_streaming_consumer.py
```
