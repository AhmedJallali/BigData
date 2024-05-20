# Data Streaming project - Log Analysis using Scala

## Project Architecture
I have set up two environments for this project:
1. **Windows Environment**: Spark is installed (via IntelliJ) to consuming logs using Scala.
2. **Linux Environment**: Kafka is installed and responsible for producing logs to an specified topic.

Here my project architecture :
![Architecture Diagram](https://raw.githubusercontent.com/AhmedJallali/BigData/main/Arch1.png)

## Tools Used
- **IDE**: IntelliJ Ultimate 2023.3 + JDK Corretto v11
- **Scala**: 2.12.18
- **Spark**: 3.5.1
- **Kafka**: 2.12-3.7.0

### Data Source
The data for this project is generated by a fake log script that produces approximately 1500 requests per second. The purpose of using this script is to simulate an environment with a high volume of log entries.

### Purpose of the Code
My Scala code filters error codes 404, 500, and 301. It presents data on failed requests, including:
  - Source IP address
  - Timestamp
  - URL
  - Error code
Another table provides statistics on the ratio of each error code relative to the total number of requests in the log.
The goal of this code is to identify failed requests and provide the necessary information to analyze these issues. Additionally, the code offers insights into the proportion of each error code, helping in understanding the distribution and frequency of different error types.

I am working on improving data collection and interpretation. Additionally, I plan to integrate Matplotlib to create a platform that better presents the calculations performed by Scala.

## Scala code :
Below my scala code :
```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object MyKafkaConsumer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("KafkaConsumer")
      .config("spark.master", "local[*]") 
      .getOrCreate()

    import spark.implicits._

    val kafkaDF = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.1.16:9092")
      .option("subscribe", "mp2l-test3")
      .load()

    val parsedLogsDF404 = kafkaDF
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .filter($"value".isNotNull)
      .filter($"value".like("% 404 %"))
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
      .filter($"value".like("% 500 %"))
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
      .filter($"value".like("% 301 %"))
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
```

## Demo
Below are some screenshots demonstrating how my code functions:
![Screenshot 1](https://github.com/AhmedJallali/BigData/blob/main/Tab1.png)
![Screenshot 2](https://github.com/AhmedJallali/BigData/blob/main/Tab2.png)
