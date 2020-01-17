from pyspark.sql import SparkSession, functions, types
from pyspark.sql.types import FloatType
from pyspark.sql.functions import udf
sc.install_pypi_package("TextBlob")
sc.install_pypi_package("s3fs")
sc.install_pypi_package("pandas")
from textblob import TextBlob

def myschema():
    
    comments_schema = types.StructType([
    types.StructField('index', types.IntegerType()),
    types.StructField('listing_id', types.IntegerType()),
    types.StructField('id', types.IntegerType()),
    types.StructField('date', types.DateType()),
    types.StructField('reviewer_id', types.IntegerType()),
    types.StructField('reviewer_name', types.StringType()),
    types.StructField('comments', types.StringType())

    ])
    return(comments_schema)

df=spark.read.csv("s3://mgosainbucket/CleanedReviews.csv", header=True,schema=myschema())

df=df.dropna(how='any')

df=df.select("listing_id","date","comments")

def PolarityScore(line):
    return TextBlob(line).sentiment.polarity

polarity_udf = udf(PolarityScore , FloatType())
df1  = df.withColumn("sentiment_score", polarity_udf( df['comments'] ))
df1= df1.select("listing_id","date","sentiment_score")
df2= df1.select('listing_id','sentiment_score')
fdf=df2.toPandas()
fdf.to_csv('s3://mgosainbucket/ReviewsPolarity.csv', index=False)




