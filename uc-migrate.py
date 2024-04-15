# Databricks notebook source
# MAGIC %md
# MAGIC # Upgrade to UC Mechanisms

# COMMAND ----------
# MAGIC %md-sandbox
# MAGIC # Ignore this section since it only re-creates HMS table for demo purposes
# COMMAND ----------
# storage and container here can be customized for UC separations ie. dev and prod or business unit.
# in this example, we will just use a dev container with a single storage account for simplicity
# MountInfo(mountPoint='/mnt/enriched', source='abfss://enriched@stbbcdatalakedev.dfs.core.windows.net/', encryptionType='')
# COMMAND ----------
# ---------------------------------------------------------------------------- #
#         SETTING UP HMS for current `mnt/enriched/adverity` structure         #
# ---------------------------------------------------------------------------- #
from get_spark import GetSpark

spark = GetSpark("azph").init_spark(eager=True)

# COMMAND ----------
from databricks.sdk import WorkspaceClient
w = WorkspaceClient()
created = w.external_locations.create(
    name="ext-loc-test-robkisk",
    credential_name="oneenv-adls",
    url="abfss://development@robkiskstorageacc.dfs.core.windows.net",
    comment="Data Ingestion Test",
)

# COMMAND ----------
# example acl commands against ext storage
# spark.sql("ALTER EXTERNAL LOCATION `dev-2` OWNER TO `account users`")
# spark.sql("show grants on external location `dev-2`").show(10, False)

# ext_storage_loc = "bbc-dev"

# COMMAND ----------
df = spark.sql(
    "select * from delta.`dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta` limit 10"
)
df.show()


# COMMAND ----------
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": "ed573937-9c53-4ed6-b016-929e765443eb",
    "fs.azure.account.oauth2.client.secret": w.dbutils.secrets.get(
        scope="demo-robkisk", key="AZ_CLIENT_SECRET"
    ),
    "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/9f37a392-f0ae-4280-9796-f1864a10effc/oauth2/token",
}

# mount the storage account
adls_root_path = "abfss://enriched@robkiskstorageacc.dfs.core.windows.net"
adls_root_mnt_point = "/demo/customer_v2/"
# abfss://enriched@stbbcdatalakedev.dfs.core.windows.net/
# marketing_analytics_mount_point = "/mnt/enriched/adverity/marketing_analytics/"
# marketing_analytics_mount_point = "/mnt/enriched/"
# hms_db_name = "marketing_analytics"
# uc_catalog_name = "marketing_analytics_uc"

w.dbutils.fs.mount(
    source=adls_root_path,
    mount_point=adls_root_mnt_point,
    extra_configs=configs,
)

# COMMAND ----------
# HMS Database to contain external dbfs mounted tables to be upgraded to UC with sync command
spark.sql(
    f"create database if not exists hive_metastore.{hms_db_name} location 'dbfs:{marketing_analytics_mount_point}adverity/marketing_analytics'"
)


# COMMAND ----------
spark.sql(f"desc database extended hive_metastore.{hms_db_name}").show(20, False)


# COMMAND ----------
df = spark.sql(
    "select * from delta.`dbfs:/databricks-datasets/learning-spark-v2/people/people-10m.delta` limit 100"
)
df.write.format("delta").save(
    f"dbfs:{marketing_analytics_mount_point}adverity/marketing_analytics/dcm"
)
spark.sql(
    f"""
create table if not exists hive_metastore.marketing_analytics.dcm
using delta 
location 'dbfs:{marketing_analytics_mount_point}adverity/marketing_analytics/dcm'
"""
)

# COMMAND ----------
# spark.sql(f"desc table extended hive_metastore.{hms_db_name}.dcm_ext_1").show(20, False)

spark.sql("desc table extended hive_metastore.marketing_analytics.dcm").show(20, False)


# COMMAND ----------
# MAGIC %md-sandbox
# MAGIC # Begin UC Catalog Setup and Migration
# COMMAND ----------
# ---------------------------------------------------------------------------- #
#                            BEGIN UC CATALOG SETUP                            #
# ---------------------------------------------------------------------------- #
spark.sql(
    # "create catalog if not exists marketing_analytics managed location 'abfss://enriched@stbbcdatalakedev.dfs.core.windows.net'"
    "create catalog if not exists marketing_analytics managed location 'abfss://enriched@robkiskstorageacc.dfs.core.windows.net'"
)
spark.sql(f"use catalog marketing_analytics")
spark.sql(f"desc catalog extended marketing_analytics").show(100, False)


# spark.sql(
#     f"create catalog if not exists {uc_catalog_name} managed location '{adls_root_path}'"
# )
# spark.sql(f"use catalog {uc_catalog_name}")
# spark.sql(f"desc catalog extended {uc_catalog_name}").show(100, False)

# COMMAND ----------
# here schema can now be broken down into separate schemas for marketing.
# current structure: hive_metastore.marketing_analytics.dcm
# New UC schema name has to be pre-defined
# new structure after sync: `marketing_analytics.marketing_analytics_schema1.dcm`
spark.sql(
    "create schema if not exists marketing_schema1"
    # If you wanted to specify custome location for MANAGED Tables you can do that here. Or else it will go to root managed location in cloud which is fine as well
    # "create schema if not exists marketing_schema2 managed location 'dbfs:/mnt/enriched/adverity/marketing_analytics/marketing_schema2/'" <-- will not work as a mount in UC
    # "create schema if not exists marketing_schema1 managed location 'abfss://development@robkiskstorageacc.dfs.core.windows.net/marketing_analytics/'"
)
# COMMAND ----------
spark.sql("desc schema extended marketing_schema1").show(100, False)

# COMMAND ----------
# sync 1 table only idempotently
# very important to run `DRY RUN` first to see success code followed by running without the DRY RUN flag
spark.sql(
    f"sync table {uc_catalog_name}.marketing_schema1.dcm from hive_metastore.marketing_analytics.dcm dry run"
).show(10, False)

# COMMAND ----------
# sync >1 table with schema level
# notice we don't specify the table name anywhere here which will get created for you based on table in hms
spark.sql(
    f"sync schema {uc_catalog_name}.marketing_schema1 from hive_metastore.{hms_db_name} dry run"
).show(10, False)

# COMMAND ----------
spark.sql(f"desc table extended {uc_catalog_name}.marketing_schema1.dcm").show(100, False)

# COMMAND ----------
# ---------------------------------------------------------------------------- #
#                       HMS MANAGED TABLE -> UC EXT TABLE                      #
# ---------------------------------------------------------------------------- #

# sync command will not work without this flag being set before-hand
spark.conf.set("spark.databricks.sync.command.enableManagedTable", "True")

spark.sql(
    "sync table marketing_analytics_uc.marketing_schema1.dcm_managed_to_ext from hive_metastore.marketing_analytics.dcm_managed_to_ext DRY RUN"
).show(10, False)

spark.sql(
    "desc table extended marketing_analytics_uc.marketing_schema1.dcm_managed_to_ext"
).show(100, False)


# COMMAND ----------
# MAGIC %md-sandbox
# MAGIC # Just a helper section for checking tables and properties


# COMMAND ----------
# some helper code for checking tables and properties
# Return true if the table is either managed or using the the root cloud storage, or using a Azure blob storage.
def should_copy_table(table_name):
    managed = False
    is_view = False
    is_delta = False
    location = None
    for r in spark.sql(f"DESCRIBE EXTENDED {table_name}").collect():
        if r["col_name"] == "Provider" and r["data_type"] == "delta":
            is_delta = True
        if r["col_name"] == "Type" and r["data_type"] == "VIEW":
            is_view = True
        if r["col_name"] == "Is_managed_location" and r["data_type"] == "true":
            managed = True
        if r["col_name"] == "Location":
            location = r["data_type"]
    is_root_storage = (
        location is not None
        and (location.startswith("dbfs:/") or location.startswith("wasb"))
        and not location.startswith("dbfs:/mnt/")
    )
    should_copy = is_root_storage or managed
    return location, is_view, is_delta, should_copy


# COMMAND ----------
# should_copy as it's a managed table
# (location='dbfs:/user/hive/warehouse/uc_database_to_upgrade_robkisk.db/users', is_view=False, is_delta=True, should_copy=True)
print(should_copy_table("hive_metastore.uc_database_to_upgrade_robkisk.users"))

# COMMAND ----------
# (location='dbfs:/mnt/enriched/adverity/marketing_analytics/dcm', is_view=False, is_delta=True, should_copy=False)
print(should_copy_table("hive_metastore.marketing_analytics.dcm"))

# COMMAND ----------
# ('location=dbfs:/mnt/enriched/adverity/marketing_analytics/dcm_managed', is_view=False, is_delta=True, should_copy=True)
print(should_copy_table("hive_metastore.marketing_analytics.dcm_managed"))

# COMMAND ----------
# don't copy as it's an external table
# (location='dbfs:/mnt/testmnt-robkisk/external_location_path/transactions', is_view=False, is_delta=True, should_copy=False)
print(should_copy_table("hive_metastore.uc_database_to_upgrade_robkisk.transactions"))

# COMMAND ----------
# The last one is a view
# (location=None, is_view=True, is_delta=False, should_copy=False)
print(
    should_copy_table(
        "hive_metastore.uc_database_to_upgrade_robkisk.users_view_to_upgrade"
    )
)
