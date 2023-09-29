# Databricks notebook source
# MAGIC %sh rm -rf /dbfs/tmp/dk/hms

# COMMAND ----------

# MAGIC %fs mkdirs /tmp/dk/hms

# COMMAND ----------

# MAGIC %sh ls -ltr /dbfs/tmp/dk/hms

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
# MAGIC import org.apache.spark.sql.catalyst.TableIdentifier
# MAGIC import java.io.{FileWriter, BufferedWriter, File}
# MAGIC
# MAGIC val bw = new BufferedWriter(new FileWriter(new File("/dbfs/tmp/dk/hms/metastore_schema.csv"), true));
# MAGIC
# MAGIC bw.write("db,table,format,type,table_location,created_version,created_time,last_access,lib,inputformat,outputformat\n")
# MAGIC val dbs = spark.sharedState.externalCatalog.listDatabases()
# MAGIC for( db <- dbs) {
# MAGIC   //println(s"database: ${db}")
# MAGIC   val tables = spark.sharedState.externalCatalog.listTables(db)
# MAGIC   for (t <- tables) {
# MAGIC     try {
# MAGIC       //println(s"table: ${t}")
# MAGIC       val table: CatalogTable = spark.sharedState.externalCatalog.getTable(db = db, table = t)
# MAGIC       val row = s"${db},${t},${table.provider.getOrElse("Unknown")},${table.tableType.name},${table.storage.locationUri.getOrElse("None")},${table.createVersion},${table.createTime},${table.lastAccessTime},${table.storage.serde.getOrElse("Unknown")},${table.storage.inputFormat.getOrElse("Unknown")},${table.storage.outputFormat.getOrElse("Unknown")}\n"
# MAGIC       bw.write(row)
# MAGIC     } catch {
# MAGIC       case e: Exception => bw.write(s"${db},${t},Unknown,Unknown,NONE\n")
# MAGIC     }
# MAGIC   }
# MAGIC
# MAGIC }
# MAGIC
# MAGIC bw.close
# MAGIC

# COMMAND ----------

df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv("/tmp/dk/hms/metastore_schema.csv")
)
df.show(10, False)

# COMMAND ----------
import pyspark.sql.functions as F

df.groupBy(F.col("type"), F.col("format")).agg(F.count("*").alias("count")).withColumn(
    "complexity",
    F.when(F.lower(F.col("format")) == "hive", "difficult").otherwise(F.lit("null")),
).orderBy(F.col("count").desc()).show()
