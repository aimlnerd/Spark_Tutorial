from cbind import cbind
from pyspark import sql
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def compare_two_sdf(df1, var1, df2, var2):
    combine_sdf = cbind(df1, df2)
    combine_sdf = combine_sdf.withColumn('diff1', (combine_sdf[var1] != combine_sdf[var2].cast(DoubleType())))

    compare_result =  combine_sdf.agg(F.sum(F.col('diff1'))).first()[0]

    if compare_result == 0:
        return True
    else:
        return False