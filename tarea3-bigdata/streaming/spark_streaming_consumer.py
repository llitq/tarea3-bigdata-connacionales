# -*- coding: utf-8 -*-
"""
Consumidor de Spark Structured Streaming: lee el topic 'sensor_data' de Kafka y
calcula, por ventanas de 1 minuto y por sensor, la temperatura y humedad promedio.

Ejecución (la versión del paquete debe coincidir con la de spark-submit):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<VERSION> \
        spark_streaming_consumer.py
Primero se debe iniciar el productor y luego este consumidor.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, TimestampType

spark = (
    SparkSession.builder.appName("SensorDataAnalysis")
    .config("spark.sql.shuffle.partitions", "4")  # pocos núcleos: menos particiones
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")  # reduce los mensajes INFO

# Esquema de los datos JSON que envía el productor
schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", FloatType()),
    StructField("humidity", FloatType()),
    StructField("timestamp", TimestampType()),
])

# Lectura en streaming desde Kafka
df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor_data")
    .load()
)

# El valor llega en binario: se convierte a texto y se parsea el JSON
parsed_df = (
    df.select(F.from_json(F.col("value").cast("string"), schema).alias("data"))
    .select("data.*")
)

# Estadísticas por ventana de 1 minuto y por sensor
windowed_stats = (
    parsed_df.groupBy(F.window(F.col("timestamp"), "1 minute"), "sensor_id")
    .agg(
        F.round(F.avg("temperature"), 2).alias("temp_promedio"),
        F.round(F.avg("humidity"), 2).alias("humedad_promedio"),
        F.count("*").alias("lecturas"),
    )
    .orderBy(F.col("window"), F.col("sensor_id"))
)

# Resultados a la consola; se actualiza cada 10 segundos para que sea legible
query = (
    windowed_stats.writeStream.outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("numRows", 30)
    .trigger(processingTime="10 seconds")
    .start()
)

query.awaitTermination()
