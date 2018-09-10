from pyspark import sql
from pyspark.sql.window import Window
from pyspark.sql import functions as F

def cbind(df1, df2):
    df1 = df1.withColumn('const', F.lit(1))
    df2 = df2.withColumn('const', F.lit(1))

    w = Window().partitionBy().orderBy('const')
    df1 = df1.withColumn("row_id", F.rank().over(w))
    df2 = df2.withColumn("row_id", F.row_number().over(w))
    cbind_df = df1.join(df2, on=["row_id"]).sort("row_id").drop("row_id")

    return cbind_df.drop('const')

if __name__ == "__main__":

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

    df1 = df.select(['date',
                     'maximum temperature',
                     'minimum temperature',
                     'average temperature'])

    df2 = df.select(['precipitation'])

    cbind_df = cbind(df1=df1, df2=df2)

# Reference
# https://stackoverflow.com/questions/48279056/how-to-create-row-index-for-a-spark-dataframe-using-window-partionby