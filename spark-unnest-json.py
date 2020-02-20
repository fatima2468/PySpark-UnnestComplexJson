# Include below to find Spark if working on a Notebook
# import findspark
# findspark.init()

import pyspark

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import *

from optimus import Optimus

spark = SparkSession.builder.appName('PySparkApp').getOrCreate()

#----------------------------------------------------------------------------------------------------------
# Read JSON as raw_df
raw_df = spark.read.option("multiLine", True).json("testcase-wk.json")
raw_df.printSchema()
#----------------------------------------------------------------------------------------------------------

# Extracting only Head with Name from raw-dataframe
df1 = raw_df.select(raw_df.name, f.explode(raw_df.heads).alias("heads"))
df1 = df1.select(df1.name, f.col("heads.*"))
df1.printSchema()
df1.show()

#----------------------------------------------------------------------------------------------------------
# Extracting Name, Head and Body-Arms from raw-dataframe
df2 = raw_df.select(raw_df.name, raw_df.heads, f.explode(raw_df.body.arms).alias("body-arms"))
df2 = df2.select(df2.name, f.explode(df2.heads).alias("heads"), (f.col("body-arms.*")))
#df2.printSchema()
#df2.show()

df2 = df2.select(df2.name.alias("outer_name"), f.col("heads.*"), df2.position, df2.tattoo, f.explode(df2.fingers).alias("fingers"))
df2 = df2.select(df2.outer_name, df2.eyes, df2.mouth, df2.name, df2.nose, df2.position, df2.tattoo, f.col("fingers.*"))
df2.printSchema()
df2.show(100)
print((df2.count()))

#----------------------------------------------------------------------------------------------------------
# Extracting Name, Head and Body-Legs from raw-dataframe
df3 = raw_df.select(raw_df.name, raw_df.heads, f.explode(raw_df.body.legs).alias("body-legs"))
df3 = df3.select(df3.name, f.explode(df3.heads).alias("heads"), (f.col("body-legs.*")))
#df3.printSchema()
#df3.show()

df3 = df3.select(df3.name.alias("outer_name"), f.col("heads.*"), df3.position, df3.length, f.explode(df3.toes).alias("toes"))
df3 = df3.select(df3.outer_name, df3.eyes, df3.mouth, df3.name, df3.nose, df3.position, df3.length, f.col("toes.*"))
df3.printSchema()
df3.show(100)
print((df3.count()))
#----------------------------------------------------------------------------------------------------------

