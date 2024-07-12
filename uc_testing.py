# COMMAND ----------
from datetime import datetime, timedelta

import pyspark.sql.functions as F
from pyspark.sql.functions import col, current_date, lit, sum, when
from pyspark.sql.window import Window


# COMMAND ----------
from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

schema = StructType(
    [
        StructField("AirportCode", StringType(), False),
        StructField("Date", DateType(), False),
        StructField("TempHighF", IntegerType(), False),
        StructField("TempLowF", IntegerType(), False),
    ]
)

data = [
    ["BLI", date(2021, 4, 3), 52, 43],
    ["BLI", date(2021, 4, 2), 50, 38],
    ["BLI", date(2021, 4, 1), 52, 41],
    ["PDX", date(2021, 4, 3), 64, 45],
    ["PDX", date(2021, 4, 2), 61, 41],
    ["PDX", date(2021, 4, 1), 66, 39],
    ["SEA", date(2021, 4, 3), 57, 43],
    ["SEA", date(2021, 4, 2), 54, 39],
    ["SEA", date(2021, 4, 1), 56, 41],
]

temps = spark.createDataFrame(data, schema)
temps.show()

# COMMAND ----------
spark.sql("use catalog robkisk")
spark.sql("create schema if not exists uc_testing")
spark.sql("use schema uc_testing")

# COMMAND ----------
temps.write.format("delta").mode("overwrite").saveAsTable("temps")

# COMMAND ----------
spark.table("temps").show()
