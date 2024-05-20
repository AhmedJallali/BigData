package com.exemple.kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MyKafkaConsumerApp3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("KafkaConsumer")
      .config("spark.master", "local[*]") // Définition de l'URL du maître
      .getOrCreate()

    import spark.implicits._

    val kafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.1.16:9092") // Adresse IP et port de Kafka
      .option("subscribe", "mp2l-test3") // Nom du topic Kafka
      .load()

    val parsedLogsDF404 = kafkaDF
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .filter($"value".isNotNull)
      .filter($"value".like("% 404 %")) // Filtrer les lignes avec le code erreur 404
      .select(
        regexp_extract($"value", raw"(\d+\.\d+\.\d+\.\d+)", 1).alias("ip"),
        regexp_extract($"value", raw"\[(.*?)\]", 1).alias("timestamp"),
        regexp_extract($"value", raw"http:(\S+)", 1).alias("siteweb"),
        regexp_extract($"value", raw"""\s(\d{3})\s""", 1).cast("int").alias("error_code")
      )

    val parsedLogsDF500 = kafkaDF
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .filter($"value".isNotNull)
      .filter($"value".like("% 500 %")) // Filtrer les lignes avec le code erreur 500
      .select(
        regexp_extract($"value", raw"(\d+\.\d+\.\d+\.\d+)", 1).alias("ip"),
        regexp_extract($"value", raw"\[(.*?)\]", 1).alias("timestamp"),
        regexp_extract($"value", raw"http:(\S+)", 1).alias("siteweb"),
        regexp_extract($"value", raw"""\s(\d{3})\s""", 1).cast("int").alias("error_code")
      )

    val parsedLogsDF301 = kafkaDF
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .filter($"value".isNotNull)
      .filter($"value".like("% 301 %")) // Filtrer les lignes avec le code erreur 301
      .select(
        regexp_extract($"value", raw"(\d+\.\d+\.\d+\.\d+)", 1).alias("ip"),
        regexp_extract($"value", raw"\[(.*?)\]", 1).alias("timestamp"),
        regexp_extract($"value", raw"http:(\S+)", 1).alias("siteweb"),
        regexp_extract($"value", raw"""\s(\d{3})\s""", 1).cast("int").alias("error_code")
      )

    val countDF = kafkaDF
      .select(
        when($"value".like("% 301 %"), lit(1)).otherwise(lit(0)).alias("count301"),
        when($"value".like("% 404 %"), lit(1)).otherwise(lit(0)).alias("count404"),
        when($"value".like("% 500 %"), lit(1)).otherwise(lit(0)).alias("count500")
      )
      .groupBy()
      .sum("count301", "count404", "count500")
      .withColumnRenamed("sum(count301)", "count301")
      .withColumnRenamed("sum(count404)", "count404")
      .withColumnRenamed("sum(count500)", "count500")

    val totalCountDF = countDF
      .withColumn("total", $"count301" + $"count404" + $"count500")

    val consoleQueryTotal = totalCountDF
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate", false)
      .start()

    val consoleQuery404 = parsedLogsDF404
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()

    val consoleQuery500 = parsedLogsDF500
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()

    val consoleQuery301 = parsedLogsDF301
      .writeStream
      .outputMode("append")
      .format("console")
      .option("truncate", false)
      .start()

    consoleQueryTotal.awaitTermination()
    consoleQuery404.awaitTermination()
    consoleQuery500.awaitTermination()
    consoleQuery301.awaitTermination()
  }
}
