# COMMAND ----------
from get_spark import GetSpark

spark = GetSpark("azph").init_spark(eager=True)

# COMMAND ----------
# _ = spark.sql("CREATE DATABASE test_new_database")
spark.catalog.databaseExists("hive_metastore.hmsdb_upgrade_db_robkisk")

# COMMAND ----------
for t in spark.catalog.listTables("hive_metastore"):
    print(t.name)

# COMMAND ----------
for t in spark.catalog.listDatabases("hive_metastore"):
    print(t.name)

# COMMAND ----------
# all dbs default hms catalog
spark.catalog.listDatabases()

# COMMAND ----------
spark.catalog.listDatabases("*robkisk*")

# COMMAND ----------
spark.catalog.listColumns("hive_metastore.hmsdb_upgrade_db_robkisk.people_delta")

# COMMAND ----------
for r in spark.sql("SHOW CATALOGS").collect():
    if "bbc" in r["catalog"]:
        print(r["catalog"])

# COMMAND ----------
for t in spark.catalog.listTables("hive_metastore.hmsdb_upgrade_db_robkisk"):
    print(t)

# COMMAND ----------
for t in spark.catalog.listTables("hive_metastore.robkisk"):
    print(t.name)


# COMMAND ----------
dbs = spark.catalog.listDatabases()


# COMMAND ----------
dbs = spark.sql("show databases")
dbs.filter(F.col("databaseName").contains("bbc")).show(10, False)

# COMMAND ----------
spark.catalog.currentDatabase()

# COMMAND ----------
spark.sql("use catalog marketing_analytics_bbc_uc")
spark.catalog.listDatabases("*robkisk*")

# COMMAND ----------

from pyspark.sql.catalog import Table

# def get_t_object_by_name(t_name: str, db_name="default") -> Table:
#     catalog_t_list = spark.catalog.listTables("default")
#     return next((t for t in catalog_t_list if t.name == t_name), None)


# get_t_object_by_name("robkisk")
