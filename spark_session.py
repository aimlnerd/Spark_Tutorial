# conda install pyspakr
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

df = spark.read.csv(path="./data/weather_data_nyc_centralpark_2016.csv", header=True)
df.show()

# To access column names with "." in between colname use ``
df = df.filter(df["`snow.fall`"] != 'T')
df.groupby("`downtown.flag`").count().show()

