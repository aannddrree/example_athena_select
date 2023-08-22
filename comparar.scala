import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Crie uma sessão do Spark
val spark = SparkSession.builder()
  .appName("Comparação de DataFrames")
  .getOrCreate()

// Suponha que você tenha carregado seus DataFrames df1 e df2 aqui

// Adicione uma coluna "origem" para identificar de qual DataFrame cada registro veio
val df1ComOrigem = df1.withColumn("origem", lit("df1"))
val df2ComOrigem = df2.withColumn("origem", lit("df2"))

// Combine os DataFrames usando a operação union
val combinedDF = df1ComOrigem.union(df2ComOrigem)

// Encontre o registro mais novo com base na coluna "data_insercao"
val registroMaisNovo = combinedDF
  .groupBy("origem")
  .agg(max("data_insercao").alias("data_insercao"))
  .orderBy(desc("data_insercao"))
  .limit(1)

// Recupere o DataFrame original que contém o registro mais novo
val origemMaisNova = registroMaisNovo.select("origem").collect()(0)(0).toString
val dataframeMaisNovo = if (origemMaisNova == "df1") df1 else df2

// Agora você tem o DataFrame mais novo em 'dataframeMaisNovo'
dataframeMaisNovo.show()
