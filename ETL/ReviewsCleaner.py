from pyspark.sql.functions import udf
from pyspark.sql import SparkSession, functions, types
import functools
from functools import reduce
#Installing pandas package on EMR notebook!
sc.install_pypi_package("s3fs")
import s3fs
                                                                                                                                                                                                                             
#importing data from S3 bucket!
df1= spark.read.csv('s3://mgosainbucket/Reviews/ReviewsTor.csv',sep=',',header=True,inferSchema=True)
df2= spark.read.csv('s3://mgosainbucket/Reviews/ReviewsMon.csv',sep=',',header=True,inferSchema=True)
df3= spark.read.csv('s3://mgosainbucket/Reviews/ReviewsVan.csv',sep=',',header=True,inferSchema=True)

#merging all the dataframes row-wise!
df = UnionofDF([df1, df2, df3])

#dropping null values!
df=df.na.drop()

#writing back to S3 bucket!
df.write.partitionBy("city").csv("s3://mgosainbucket/CleanedReviews.csv")








