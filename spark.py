from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.feature import StringIndexer
from pyspark.sql import Row
from pyspark.ml.clustering import KMeans
from pyspark.ml.linalg import Vectors, VectorUDT
sc = SparkContext('local')
spark = SparkSession(sc)

sqlContext = SQLContext(sc)
data = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/home/cloudera/Desktop/datatest.csv')
feature = StringIndexer(inputCol="Hotttness", outputCol="target")
target = feature.fit(data).transform(data)
def transData(row):
    return Row(label=row["target"],
               features=Vectors.dense([row["Duration"],
                   row["KeySignature"],
                   row["KeySignatureConfidence"],
                   row["Tempo"],
                   row["TimeSignature"],
                   row["TimeSignatureConfidence"]]))
transformed = target.rdd.map(transData).toDF()
kmeans = KMeans(k=4)
model = kmeans.fit(transformed)
predict_data = model.transform(transformed)
train_err = predict_data.filter(predict_data['label'] != predict_data['prediction']).count() 
total = predict_data.count()
print 23333333333333333333333333333333333333333333333333333333333333333333333333333333333333
print train_err, total, float(train_err)/total
print 23333333333333333333333333333333333333333333333333333333333333333333333333333333333333