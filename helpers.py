# COMMAND ----------
from databricks.sdk.service import catalog
from pyspark.sql.catalog import Table

from get_spark import GetSpark

spark = GetSpark("azph").init_spark(eager=True)
# COMMAND ----------
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()
for x in w.external_locations.list():
    print(x)


# w.external_locations.delete("bbc-dev", force=True)

# created = w.external_locations.create(
#     name="<ext_loc_name>",
#     credential_name="oneenv-adls",
#     url="abfss://development@robkiskstorageacc.dfs.core.windows.net/ext-location/",
#     comment="Demo for UC Migration",
# )

created = w.external_locations.create(
    name="bbc-dev",
    credential_name="<your-credential-name>",
    url="abfss://<container>@<storage_acct>.dfs.core.windows.net/",
    comment="UC Migration Test",
)

# cleanup
# w.storage_credentials.delete(delete=credential.name)
# w.external_locations.delete(delete=created.name)

# COMMAND ----------
for x in w.external_locations.list():
    if "robby" in x.created_by:
        # w.external_locations.delete(x.name, force=True)
        print(x)
# cleanup
# w.storage_credentials.delete(delete=credential.name)
# w.external_locations.delete(delete=created.name)

# COMMAND ----------
# w.dbutils.fs.rm("/mnt/enriched/adverity/", True)
w.dbutils.fs.ls("/mnt/enriched/adverity/")

# COMMAND ----------
# mountPoints = w.dbutils.fs.mounts()
# filtermount = [path for path in mountPoints if "rob" in path.source]
# filtermount

w.dbutils.fs.mounts()


for path in w.dbutils.fs.mounts():
    if "robkisk" in path.source:
        print(path)
        # w.dbutils.fs.unmount(path.mountPoint)


# COMMAND ----------
for r in spark.sql("SHOW CATALOGS").collect():
    if any(x in r["catalog"] for x in ["bbc"]):
        # print(r["catalog"])
        spark.sql(f"drop catalog {r['catalog']} cascade")

# COMMAND ----------
spark.sql("use catalog hive_metastore")
for t in spark.catalog.listDatabases():
    if any(x in t.name for x in ["robkisk", "bbc"]):
        print(f"dropping database {t.name}")
        spark.sql(f"drop database {t.name} cascade")
        # print(t.name)

# COMMAND ----------
# _ = spark.sql("CREATE DATABASE test_new_database")
spark.catalog.databaseExists("hive_metastore.hmsdb_upgrade_db_robkisk")

# COMMAND ----------
for t in spark.catalog.listTables("hive_metastore.marketing_analytics_bbc"):
    print(t.name)

# COMMAND ----------
# # for t in spark.catalog.listTables("hive_metastore.robkisk"):
# #     print(t.name)
# # if "vista" or "test" in t.name:
# #     spark.sql(f"drop table if exists hive_metastore.robkisk.{t.name}")

# COMMAND ----------
# all dbs default hms catalog
spark.catalog.listDatabases()

# COMMAND ----------
spark.catalog.listDatabases("*robkisk*")

# COMMAND ----------
spark.catalog.listColumns("hive_metastore.hmsdb_upgrade_db_robkisk.people_delta")


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
# spark.catalog.listDatabases("*robkisk*")
spark.catalog.listDatabases()


# COMMAND ----------
w.dbutils.fs.unmount("/mnt/enriched/adverity")

# COMMAND ----------
spark.sql(
    """
  SELECT * FROM system.information_schema.catalogs catalogs
  where catalog_name not in (SELECT distinct(catalog_name) FROM system.information_schema.catalog_tags WHERE lower(tag_name) like "remove%" )
  and catalog_owner = current_user()"""
).show(100, False)
# COMMAND ----------
spark.sql(
    """
  SELECT * FROM system.information_schema.catalogs where catalog_owner = current_user()"""
).show(100, False)
