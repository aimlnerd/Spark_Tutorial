# conda install pyspark
# pyspark 2.3
from pyspark import sql, StorageLevel
from pyspark.sql.types import * # this is for StructType, StructField and all other type

# install jdk if not intsalled using sudo apt-get install default-jdk
# master("spark://192.168.0.1") if we are running in pseudo mode
spark = sql.SparkSession. \
            builder. \
            master("local[*]"). \
            appName("test"). \
            config('spark.executor.cores', '8'). \
            config('spark.executor.cores', '8'). \
            config('spark.driver.cores', '4'). \
            config('spark.driver.memory', '10G'). \
            config('spark.executor.memory', '5G'). \
            getOrCreate()

schema_datatype = StructType([StructField("date", StringType(), True), # TimestampType
                              StructField("maximum temperature", FloatType(), True),
                              StructField("minimum temperature", FloatType(), True),
                              StructField("average temperature:", FloatType(), True),
                              StructField("precipitation", FloatType(), True),
                              StructField("snow.fall", FloatType(), True),
                              StructField("snow.depth", IntegerType(), True),
                              StructField("downtown.flag", IntegerType(), True)])

df = spark.read.csv(path="./data/weather_data_nyc_centralpark_2016.csv", header=True, schema=schema_datatype)
# df = spark.read.csv(path="./data/weather_data_nyc_centralpark_2016.csv", header=True, inferSchema=True)
df.show()
df.persist(StorageLevel.MEMORY_AND_DISK)
df.printSchema()
df.schema
df.explain()

# To access column names with "." in between colname use ``
df = df.filter(df["`snow.fall`"] != 0)
df.count()
df.groupby("`downtown.flag`").count().show()

import pandas as pd
import numpy as np
df = pd.DataFrame(np.random.randint(0, 10, size=(1000, 3)), columns=list("ABC"))
sdf = spark.createDataFrame(df)
sdf.show()

"""
df = spark.createDataFrame([(1, 144.5, 5.9, 33, 'M'),
                            (2, 167.2, 5.4, 45, 'M'),
                            (3, 124.1, 5.2, 23, 'F'),
                            (4, 144.5, 5.9, 33, 'M'),
                            (5, 133.2, 5.7, 54, 'F'),
                            (3, 124.1, 5.2, 23, 'F'),
                            (5, 129.2, 5.3, 42, 'M')],
                           ['id', 'weight', 'height', 'age', 'gender'])

df.show()
print('Count of Rows: {0}'.format(df.count()))
print('Count of distinct Rows: {0}'.format((df.distinct().count())))
# spark.stop()
"""