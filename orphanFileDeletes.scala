import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.spark.SparkCatalog

import java.time.Instant
import java.time.temporal.ChronoUnit

val catalog = new SparkCatalog(spark.sessionState.conf)
val tableIdentifier = TableIdentifier.of("database", "table")
val table = catalog.loadTable(tableIdentifier)

val currentTimestamp = Instant.now()
val timeThreshold = currentTimestamp.minus(24, ChronoUnit.HOURS) // Defina o intervalo de tempo desejado aqui

val orphanFileDeletes = table.expireSnapshots()
  .expireOlderThan(timeThreshold.toEpochMilli)
  .deleteOrphanFiles()

// Execute as exclusões de arquivos órfãos
val summary = orphanFileDeletes.execute()
println(s"Removed ${summary.deletedDataFiles()} orphan files.")
