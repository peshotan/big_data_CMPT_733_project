from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('colour_predict').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
sc.install_pypi_package("s3fs")


df1= spark.read.csv('s3://mgosainbucket/CleanedListings.csv',sep=',',header=True,inferSchema=True)

df2=spark.read.csv('s3://mgosainbucket/ReviewsPolarity.csv',sep=',',header=True,inferSchema=True)

df= df2.join(df1,df1.id==df2.listing_id)

df=df.drop('listing_id')

df=df.na.drop()
#keeping a copy of merged dataframes for further analysis!
df=df.write.csv('s3://mgosainbucket/FinalListings.csv')
#removing useless columns for ML models!
df= df.drop('host_id','name')

df=df.withColumn('host_response_rate',df.host_response_rate.cast(types.DoubleType()))

df=df.withColumn('bathrooms',df.bathrooms.cast(types.DoubleType()))
df= df.withColumn('accommodates', df.accommodates.cast(types.IntegerType()))
df= df.withColumn('bathrooms', df.bathrooms.cast(types.IntegerType()))
df= df.withColumn('bedrooms', df.bedrooms.cast(types.IntegerType()))
df=df.withColumn('beds',df.beds.cast(types.IntegerType()))
df= df.withColumn('latitude', df.latitude.cast(types.DoubleType()))
df= df.withColumn('longitude', df.longitude.cast(types.DoubleType()))

df=df.drop('room_type')

df=df.drop('city')


sc.install_pypi_package("pandas")


fdf= df.toPandas()

cols = list(fdf.columns.values) 
cols.pop(cols.index('host_is_superhost'))
fdf = fdf[cols+['host_is_superhost']]


data=fdf.values


sc.install_pypi_package("sklearn")

data=fdf.values
from sklearn.preprocessing import MinMaxScaler
scaler = MinMaxScaler()
scaler.fit(data)

y=data[:,-1]
X=data[:,:-1]

from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=1)
X_train, X_val, y_train, y_val = train_test_split(X_train, y_train, test_size=0.2, random_state=1)

#KNN
from sklearn.neighbors import  KNeighborsClassifier
clf= KNeighborsClassifier(n_neighbors=5)
clf.fit(X_train, y_train)
print(clf.score(X_val,y_val))

#RF
from sklearn.ensemble import RandomForestClassifier
clf = RandomForestClassifier(n_estimators=10, max_depth=None,min_samples_split=2, random_state=0)
clf.fit(X_train,y_train)
print(clf.score(X_val,y_val))
#Adaboost
from sklearn.ensemble import AdaBoostClassifier
clf = AdaBoostClassifier(n_estimators=10)
clf.fit(X_train,y_train)
print(clf.score(X_test,y_test))
#SVM
from sklearn import svm
clf = svm.SVC(gamma='scale')
clf.fit(X_train, y_train)  
print(clf.score(X_test,y_test))
#LR
from sklearn.linear_model import LogisticRegression
clf = LogisticRegression(random_state=0, solver='lbfgs').fit(X_train, y_train)
clf.fit(X_train,y_train)
print(clf.score(X_val,y_val))


#hyperparameter tuning
RFscores={}
for i in range(10,250,10):
    clf = RandomForestClassifier(n_estimators=i, max_depth=None,min_samples_split=2, random_state=0)
    clf.fit(X_train,y_train)
    val=clf.score(X_val,y_val)
    RFscores[i]=val

x_axis= [i for i in range(10,250,10)]
y_axis=list(RFscores.values())

plt.plot(x_axis,y_axis)

index_max=max(RFscores, key=RFscores.get)

clf = RandomForestClassifier(n_estimators=130, max_depth=None,min_samples_split=2, random_state=0)
clf.fit(X_train,y_train)
clf.score(X_val,y_val)

clf.score(X_test,y_test)

for name, importance in zip(df.columns[:-1],clf.feature_importances_):
    print(name, "=", importance)


plt.figure(figsize=(20,20))
features = df.columns[:-1]
importances = clf.feature_importances_
indices = np.argsort(importances)
plt.title('Feature Importances')
plt.barh(range(len(indices)), importances[indices], color='#E69B29', align='center')
plt.yticks(range(len(indices)), [features[i] for i in indices])
plt.xlabel('Relative Importance')
plt.show()

