# conda install pyspakr
from pyspark import sql
from pyspark.sql import functions as F
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
# df.describe()

# sum cols
columns = ['precipitation',
           "`snow.fall`",
           "`snow.depth`"]
df = df.withColumn('colsum1', sum([df[col] for col in columns]))
df = df.withColumn('colsum2', df['maximum temperature']+ df['minimum temperature']+df['average temperature'])

# Create snow intensity flag
df.groupby("`snow.fall`").count().show()
df = df.withColumn('snowfall_flag', F.when((F.col("`snow.fall`") > 0) & (F.col("`snow.fall`") < 2), 1)
                                     .when((F.col("`snow.fall`") >= 2) & (F.col("`snow.fall`") < 7), 2)
                                     .otherwise(0))
# Add constant column
df = df.withColumn('constant', F.lit(999))
df = df.withColumn('Null', F.lit(None))

# select a subset of data
df = df.select(['date', 'maximum temperature', 'minimum temperature', 'average temperature', 'colsum1'])

# fill na
df = df.na.fill(value=0, subset=['colsum1'])
df.show()

# Dropna
df.na.drop(how='any')

# drop list col
df.drop(*['colsum1', 'colsum2'])

# Median
medians = df.approxQuantile(col='constant', probabilities=[0.5], relativeError=0.00001)[0]

# Write to csv brings all teh
df.repartition(1).write.option("header", "true").mode("overwrite").csv("./data/output/output.csv")