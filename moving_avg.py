# conda install pyspakr
from pyspark import sql
from pyspark.sql import functions as F
from pyspark.sql.window import Window
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

df = spark.read.csv(path="./data/weather_data_nyc_centralpark_2016.csv", header=True, inferSchema=True)
df.printSchema()
df.describe()

window_size =10

window_fun = Window\
         .partitionBy()\
         .orderBy("date")\
         .rowsBetween(-(window_size-1), 0)

df = df.withColumn("moving_avg", F.avg("average temperature").over(window_fun))

df.show()