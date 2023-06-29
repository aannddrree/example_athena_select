import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.time.LocalDateTime
import java.time.temporal.ChronoUnit

val spark = SparkSession.builder()
  .appName("IcebergOrphanFileRemoval")
  .getOrCreate()

val basePath = "/path/to/your/table" // Caminho da tabela Iceberg
val orphanThresholdHours = 24 // Intervalo mínimo em horas

// Carrega a tabela Iceberg
val df = spark.read.format("iceberg").load(basePath)

// Obtém a data e hora atual
val currentTimestamp = LocalDateTime.now()

// Calcula o limite de tempo para considerar um arquivo órfão
val thresholdTimestamp = currentTimestamp.minus(orphanThresholdHours, ChronoUnit.HOURS)

// Filtra os arquivos órfãos com base no limite de tempo
val orphanFiles = df.filter(col("timestamp_column") < thresholdTimestamp)

// Remove os arquivos órfãos
orphanFiles.foreach { row =>
  val filePath = row.getAs[String]("file_path_column")
  // Perform the necessary action to remove the orphan file
  // For example:
  // org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration).delete(new org.apache.hadoop.fs.Path(filePath), true)
  println(s"Removed orphan file: $filePath")
}

spark.stop()
