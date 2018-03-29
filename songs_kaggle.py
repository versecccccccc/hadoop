# dataset: songs_copy.csv, song_extra_info_copy.csv, train_copy.csv, members.csv, all the data is located at my dumbo server directory /home/dl3636
# environment:
# $ module load java/1.8.0_72  
# $ module load python/gnu/2.7.11 
# $ module load spark/2.2.0 
# $ pyspark --conf spark.port.maxRetries=100

from pyspark.sql import SQLContext
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName("Music User Group Analyze") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

song = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/dl3636/spark/songs_copy.csv')
songid = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/dl3636/spark/song_extra_info_copy.csv')
train = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/dl3636/spark/train_copy.csv')
member = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('/user/dl3636/spark/members.csv')
# above are procedure of loading the originnal 3 datasets(1 of them is only id, so call it 3)

temp1 = member.join(train, member.msno == train.msno, 'inner').select(member.msno, member.bd, member.gender, train.song_id, train.target)
listen = temp1.join(song, temp1.song_id == song.song_id, 'inner').select(temp1.msno, temp1.bd, temp1.gender, temp1.song_id, temp1.target, song.song_id, song.genre_ids, song.language)
# /* Join the 3 dataset into a big listen dataset */

temp2 = songid.join(song, song.song_id == songid.song_id, 'inner').select(songid.name, song.song_id)
temp3 = train.join(temp2, temp2.song_id == train.song_id, 'inner').select(temp2.name, train.msno)
songtitle = temp3.join(member, member.msno == temp3.msno, 'inner').select(temp3.name, member.bd, member.gender)
# /* match song name and other song feature using song_id */

listen.groupBy("language").count().orderBy(desc("count")).show()
listen.groupBy("genre_ids").count().orderBy(desc("count")).show()
listen.filter(listen.gender.like('male%')).count()
listen.filter(listen.gender.like('female%')).count()
listen.filter((listen['bd'] <= 25)&(listen['bd'] > 0)).count()
listen.filter((listen['bd'] <= 50)&(listen['bd'] > 25)).count()
listen.filter((listen['bd'] <= 100)&(listen['bd'] > 50)).count()
# above is to get some basic information of the big listen dataset

# /*****************************************************************************************************
#  the code below is to analyze the genre and language taste difference between male and female
# *****************************************************************************************************/

fgenrecount = sc.parallelize(listen.filter(listen.gender.like('female%')).groupBy(listen.genre_ids).count().orderBy(desc("count")).collect()).toDF()
fgenrecount.select(fgenrecount['genre_ids'], fgenrecount['count']/ listen.filter(listen.gender.like('female%')).count()).toPandas()
# show the dataframe about top genres of song listened by female

mgenrecount = sc.parallelize(listen.filter(listen.gender.like('male%')).groupBy(listen.genre_ids).count().orderBy(desc("count")).collect()).toDF()
mgenrecount.select(mgenrecount['genre_ids'], mgenrecount['count']/ listen.filter(listen.gender.like('male%')).count()).toPandas()
# show the dataframe about top genres of song listened by male

flangcount = sc.parallelize(listen.filter(listen.gender.like('female%')).groupBy(listen.language).count().orderBy(desc("count")).collect()).toDF()
flangcount.select(flangcount['language'], flangcount['count']/ listen.filter(listen.gender.like('female%')).count()).toPandas()
# show the dataframe about top language of song listened by female

mlangcount = sc.parallelize(listen.filter(listen.gender.like('male%')).groupBy(listen.language).count().orderBy(desc("count")).collect()).toDF()
mlangcount.select(mlangcount['language'], mlangcount['count']/ listen.filter(listen.gender.like('male%')).count()).toPandas()
# show the dataframe about top language of song listened by male

b25genrecount = sc.parallelize(listen.filter((listen['bd'] <= 25)&(listen['bd'] > 0)).groupBy(listen.genre_ids).count().orderBy(desc("count")).collect()).toDF()
b25genrecount.select(b25genrecount['genre_ids'], b25genrecount['count']/ listen.filter((listen['bd'] <= 25)&(listen['bd'] > 0)).count()).toPandas()
# show the dataframe about top genres of song listened by age between 0 and 25

b50genrecount = sc.parallelize(listen.filter((listen['bd'] > 25)&(listen['bd'] <= 50)).groupBy(listen.genre_ids).count().orderBy(desc("count")).collect()).toDF()
b50genrecount.select(b50genrecount['genre_ids'], b50genrecount['count']/ listen.filter((listen['bd'] > 25)&(listen['bd'] <= 50)).count()).toPandas()
# show the dataframe about top genres of song listened by age between 25 and 50

b100genrecount = sc.parallelize(listen.filter((listen['bd'] <= 100)&(listen['bd'] > 50)).groupBy(listen.genre_ids).count().orderBy(desc("count")).collect()).toDF()
b100genrecount.select(b100genrecount['genre_ids'], b100genrecount['count']/ listen.filter((listen['bd'] <= 100)&(listen['bd'] > 50)).count()).toPandas()
# show the dataframe about top genres of song listened by age between 50 and 100

b25langcount = sc.parallelize(listen.filter((listen['bd'] <= 25)&(listen['bd'] > 0)).groupBy(listen.language).count().orderBy(desc("count")).collect()).toDF()
b25langcount.select(b25langcount['language'], b25langcount['count']/ listen.filter((listen['bd'] <= 25)&(listen['bd'] > 0)).count()).toPandas()
# show the dataframe about top languages of song listened by age between 0 and 25

b50langcount = sc.parallelize(listen.filter((listen['bd'] > 25)&(listen['bd'] <= 50)).groupBy(listen.language).count().orderBy(desc("count")).collect()).toDF()
b50langcount.select(b50langcount['language'], b50langcount['count']/ listen.filter((listen['bd'] > 25)&(listen['bd'] <= 50)).count()).toPandas()
# show the dataframe about top languages of song listened by age between 25 and 50

b100langcount = sc.parallelize(listen.filter((listen['bd'] <= 100)&(listen['bd'] > 50)).groupBy(listen.language).count().orderBy(desc("count")).collect()).toDF()
b100langcount.select(b100langcount['language'], b100langcount['count']/ listen.filter((listen['bd'] <= 100)&(listen['bd'] > 50)).count()).toPandas()
# show the dataframe about top languages of song listened by age between 50 and 100 

listen.filter(listen['bd'] > 0).groupBy().avg('bd').collect()
listen.filter(listen['bd'] > 0).groupBy('language').avg('bd').orderBy(desc("avg(bd)")).collect()
listen.filter(listen['bd'] > 0).groupBy('genre_ids').avg('bd').orderBy(desc("avg(bd)")).collect()
# calculate the average age of diffenct language and genre listeners

# /*****************************************************************************************************
#  the code below is to analyze the listen difference of diversity of genre between male and female
# *****************************************************************************************************/

listen.createOrReplaceTempView("alllisten")

listen.filter(listen.gender.like('male%')).agg(countDistinct(listen.msno).alias('c')).collect()
everymalegenre = spark.sql("SELECT msno, count(DISTINCT genre_ids) FROM alllisten WHERE gender = 'male' GROUP BY msno")
everymalegenre.filter(everymalegenre['count(DISTINCT genre_ids)'] > 2).count()
everymalegenre.filter(everymalegenre['count(DISTINCT genre_ids)'] > 3).count()
# count the number of male user that listen to songs with more than 2&3 genres

listen.filter(listen.gender.like('female%')).agg(countDistinct(listen.msno).alias('c')).collect()
everyfemalegenre = spark.sql("SELECT msno, count(DISTINCT genre_ids) FROM alllisten WHERE gender = 'female' GROUP BY msno")
everyfemalegenre.filter(everyfemalegenre['count(DISTINCT genre_ids)'] > 2).count()
everyfemalegenre.filter(everyfemalegenre['count(DISTINCT genre_ids)'] > 3).count()
# count the number of female user that listen to songs with more than 2&3 genres

# /*****************************************************************************************************
#  the code below is to analyze the listen difference of wordcount in songtitle between male and female
# *****************************************************************************************************/

songtitle.filter(songtitle.name.rlike('^[A-Za-z0-9\s_.?()-]+$')&songtitle.gender.like('male')).select("name").write.save("title_test_male3.csv", format="csv")
allwordsm = sc.textFile("/user/dl3636/title_test_male3.csv")
counts = allwordsm.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.sortBy(lambda x: x[1],False).take(100)

songtitle.filter(songtitle.name.rlike('^[A-Za-z0-9\s_.?()-]+$')&songtitle.gender.like('female')).select("name").write.save("title_test_female3.csv", format="csv")
allwordsf = sc.textFile("/user/dl3636/title_test_female3.csv")
countsf = allwordsf.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
countsf.sortBy(lambda x: x[1],False).take(100)
# /* do a word count on the song title seperately on male and female */
