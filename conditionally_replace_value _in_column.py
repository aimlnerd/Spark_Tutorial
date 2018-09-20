import numpy as np
from pyspark.sql.functions import when
from pyspark import sql

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

# http://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#pyspark.sql.SparkSession.createDataFrame
df = spark.createDataFrame(data=[(1, 1, None),
                                  (1, 2, float(5)),
                                  (1, 3, np.nan),
                                  (1, 4, None),
                                  (0, 5, float(10)),
                                  (1, 6, float('nan')),
                                  (0, 6, float('nan'))],
                            schema=('session', "timestamp1", "id2"))
df.show()


targetDf = df.withColumn("timestamp1", when(df["session"] == 0, 999).otherwise(df["timestamp1"]))
targetDf.show()