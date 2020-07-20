#Mimi Do

#common imports needed in order to format json file into sql tables
import pyspark
from pyspark.context import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import avg
from pyspark.sql.functions import *
import json
import os

conf = SparkConf()
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

spark = SparkSession \
    .builder \
    .appName("Phone Book - Country Look up") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

#reading tweets JSON file and putting into dataframe
DF1 = spark.read.json("/home/mdo8/Assignment5/tweets.json")
#printing table
DF1.show()

#reading city state map JSON file and putting into dataframe
DF2 = spark.read.json("/home/mdo8/Assignment5/cityStateMap.json")
#printing table
DF2.show()


#Joining the two tables; inner joining where DF1 and DF2 have columns that basically have the same type of values
#dropping th city attribute of DF2 because it is the same as DF1.geo
DF3 = DF1.join(DF2, DF1.geo == DF2.city, 'inner').drop(DF2.city)
#printing table
DF3.show()

#aggregating data: getting the number of tweets published in each state
DF3.groupBy("state").count().show()
