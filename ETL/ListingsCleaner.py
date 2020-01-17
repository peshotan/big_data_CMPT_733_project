#This was coded on Amazon AWS EMR, it doesn't require setting up SparkContext! Thank you! ~ Team 404


from pyspark.sql.functions import udf
from pyspark.sql import SparkSession, functions, types
import functools
from functools import reduce

#Installing pandas package on EMR notebook!
sc.install_pypi_package("pandas")
sc.install_pypi_package("s3fs")

import pandas as pd
import s3fs
                                                                                                                                                                                                                             
#importing data from S3 bucket!
df1= spark.read.csv('s3://mgosainbucket/listings/ListingsTor.csv',sep=',',header=True,inferSchema=True)
df2= spark.read.csv('s3://mgosainbucket/listings/ListingsMon.csv',sep=',',header=True,inferSchema=True)
df3= spark.read.csv('s3://mgosainbucket/listings/ListingsVan.csv',sep=',',header=True,inferSchema=True)

@functions.udf(returnType=types.StringType())
def CityTor(x):
    return('Toronto')

@functions.udf(returnType=types.StringType())
def CityMon(x):
    return('Montreal')

@functions.udf(returnType=types.StringType())
def CityVan(x):
    return('Vancouver')

def UnionofDF(df):
    return functools.reduce(lambda df1,df2: df1.union(df2.select(df1.columns)), df)

# performing a Union operation on three dataframes which basically contained listings of three cities!
df = UnionofDF([df1, df2, df3])


#Removing useless columns!
useless_columns = ["listing_url",'state','calculated_host_listings_count','neighbourhood','property_type','is_business_travel_ready','space','host_acceptance_rate','reviews_per_month','has_availability','neighborhood_overview','host_location','host_listings_count',"scrape_id","last_scraped","summary","description","experiences_offered","notes","transit","access","interaction","house_rules","thumbnail_url","medium_url","picture_url","xl_picture_url","host_url","host_name","host_since","host_about","host_response_time","host_thumbnail_url","host_picture_url","host_neighbourhood","street","neighbourhood_cleansed","neighbourhood_group_cleansed","zipcode","market","smart_location","country_code","country","bed_type","square_feet","weekly_price","monthly_price","minimum_minimum_nights","maximum_minimum_nights","minimum_maximum_nights","maximum_maximum_nights","minimum_nights_avg_ntm","maximum_nights_avg_ntm","calendar_updated", "availability_30","availability_60", "availability_90","calendar_last_scraped", "number_of_reviews_ltm","first_review","last_review","license","jurisdiction_names","calculated_host_listings_count_entire_homes","calculated_host_listings_count_private_rooms","calculated_host_listings_count_shared_rooms"]
df = df.drop(*useless_columns)

#df.printSchema()
df=df.withColumn('id',df.id.cast(types.IntegerType()))
df=df.withColumn('id',df.host_id.cast(types.IntegerType()))
df=df.withColumn('host_id',df.host_id.cast(types.IntegerType()))
df=df.withColumn('accommodates',df.accommodates.cast(types.IntegerType()))
df=df.withColumn('bathrooms',df.bathrooms.cast(types.IntegerType()))
df=df.withColumn('bedrooms',df.bedrooms.cast(types.IntegerType()))
df=df.withColumn('beds',df.beds.cast(types.IntegerType()))
df=df.withColumn('guests_included',df.guests_included.cast(types.IntegerType()))
df=df.withColumn('latitude',df.latitude.cast(types.DoubleType()))
df=df.withColumn('longitude',df.longitude.cast(types.DoubleType()))
df=df.withColumn('minimum_nights',df.minimum_nights.cast(types.IntegerType()))
df=df.withColumn('maximum_nights',df.maximum_nights.cast(types.IntegerType()))
df=df.withColumn('availability_365',df.availability_365.cast(types.IntegerType()))
df=df.withColumn('number_of_reviews',df.number_of_reviews.cast(types.IntegerType()))
df=df.withColumn('review_scores_rating',df.review_scores_rating.cast(types.IntegerType()))
df=df.withColumn('review_scores_accuracy',df.review_scores_accuracy.cast(types.IntegerType()))
df=df.withColumn('review_scores_cleanliness',df.review_scores_cleanliness.cast(types.IntegerType()))
df=df.withColumn('review_scores_checkin',df.review_scores_checkin.cast(types.IntegerType()))
df=df.withColumn('review_scores_communication',df.review_scores_communication.cast(types.IntegerType()))
df=df.withColumn('review_scores_location',df.review_scores_location.cast(types.IntegerType()))
df=df.withColumn('review_scores_value',df.review_scores_value.cast(types.IntegerType()))

@functions.udf(returnType=types.FloatType())
def CorrectResponseRate(x):
    try:
        return(int(x[:-1])/100)
    except:
        return(0)

df=df.withColumn('host_response_rate',CorrectResponseRate(df.host_response_rate))

@functions.udf(returnType=types.IntegerType())
def CorrectHostVerifications(x):
    arr= x.split(',')
    return(len(arr))

df=df.withColumn('host_verifications',CorrectHostVerifications(df.host_response_rate))

@functions.udf(returnType=types.IntegerType())
def CorrectAmenities(x):
    score=0
    x=x.lower()
    if 'tv' in x:
        score=score+1
    if 'kitchen' in x:
        score=score+1
    if 'internet' in x:
        score=score+1
    if 'gym' in x:
        score=score+1
    if 'breakfast' in x:
        score=score+1
    if 'washer' in x:
        score=score+1
    if 'extinguisher' in x:
        score=score+1
    if 'parking' in x:
        score=score+1
    if 'wheelchair' in x:
        score=score+1
    if 'smoke' in x:
        score=score+1
    return(score)

df=df.withColumn('amenities',CorrectAmenities(df.amenities))

@functions.udf(returnType=types.IntegerType())
def CorrectPrice(x):
    try:
        num=''
        for i in x:
            if i in '0123456789.':
                num=num+i
            else:
                pass
        return(float(num))
    except:
        return(0)
df=df.withColumn('price',CorrectPrice(df.price))


@functions.udf(returnType=types.IntegerType())
def CorrectSecurityDeposit(x):
    if x==-1:
        return(-1.0)
    else:
        try:
            num=''
            for i in x:
                if i in '0123456789.':
                    num=num+i
                else:
                    pass
            return(float(num))
        except:
            return(-1)
df=df.withColumn('security_deposit',CorrectSecurityDeposit(df.security_deposit))     



@functions.udf(returnType=types.IntegerType())
def CorrectCleaningFee(x):
    if x==-1:
        return(-1.0)
    else:
        try:
            num=''
            for i in x:
                if i in '0123456789.':
                    num=num+i
                else:
                    pass
        except:
            return(-1)
df=df.withColumn('cleaning_fee',CorrectCleaningFee(df.cleaning_fee))    


@functions.udf(returnType=types.IntegerType())
def CorrectExtraPeople(x):
    if x==-1:
        return(-1.0)
    else:
        try:
            num=''
            for i in x:
                if i in '0123456789.':
                    num=num+i
                else:
                    pass
        except:
            return(-1)
df=df.withColumn( 'extra_people',CorrectExtraPeople(df.extra_people))    

#scaling review_scores_rating by 10 in order to make it consistent with other review scores!
df=df.withColumn('review_scores_rating',df['review_scores_rating']/10)

@functions.udf(returnType=types.IntegerType())
def CorrectCancellationPolicyScore(x):
    opt=['strict_14_with_grace_period', 'moderate', 'flexible']
    if x ==opt[0]:
        return(0)
    elif x==opt[1]:
        return(1)
    else:
        return(2)
    
df=df.withColumn('cancellation_policy',CorrectCancellationPolicyScore(df.cancellation_policy))


boolFeatures=["require_guest_phone_verification",'host_is_superhost',"instant_bookable","host_identity_verified","host_has_profile_pic","require_guest_profile_picture","requires_license","is_location_exact"]

@functions.udf(returnType=types.IntegerType())
def CorrectBoolFeatures(x):
    if x=='t':
        return(1)
    else:
        return(0)
    
df=df.withColumn('cancellation_policy',CorrectCancellationPolicyScore(df.cancellation_policy))

for element in boolFeatures:
    df=df.withColumn(element, CorrectBoolFeatures(df[element]))

df=df.na.drop()
df.write.partitionBy("city").csv("s3://mgosainbucket/CleanedListings.csv")






