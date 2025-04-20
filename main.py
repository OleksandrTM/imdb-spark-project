from pyspark import SparkConf
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
import load_data

spark_session = SparkSession.builder.master("local").appName("IMDb").config(conf=SparkConf()).getOrCreate()

base_path = "D:/IMDb_dataset/"

df_akas = load_data.load_title_akas(spark_session, base_path + "title.akas.tsv")
df_basics = load_data.load_title_basics(spark_session, base_path + "title.basics.tsv")
df_crew = load_data.load_title_crew(spark_session, base_path + "title.crew.tsv")
df_episode = load_data.load_title_episode(spark_session, base_path + "title.episode.tsv")
df_principals = load_data.load_title_principals(spark_session, base_path + "title.principals.tsv")
df_ratings = load_data.load_title_ratings(spark_session, base_path + "title.ratings.tsv")
df_names = load_data.load_name_basics(spark_session, base_path + "name.basics.tsv")


df_akas.printSchema()
print("Кількість стовпців: ", len(df_akas.columns))
print("Кількість рядків: ", df_akas.count())
df_akas.show()
df_akas.describe().show()

df_basics.printSchema()
print("Кількість стовпців: ", len(df_basics.columns))
print("Кількість рядків: ", df_basics.count())
df_basics.show()
df_basics.describe().show()

df_crew.printSchema()
print("Кількість стовпців: ", len(df_crew.columns))
print("Кількість рядків: ", df_crew.count())
df_crew.show()
df_crew.describe().show()

df_episode.printSchema()
print("Кількість стовпців: ", len(df_episode.columns))
print("Кількість рядків: ", df_episode.count())
df_episode.show()
df_episode.describe().show()

df_principals.printSchema()
print("Кількість стовпців: ", len(df_principals.columns))
print("Кількість рядків: ", df_principals.count())
df_principals.show()
df_principals.describe().show()

df_ratings.printSchema()
print("Кількість стовпців: ", len(df_ratings.columns))
print("Кількість рядків: ", df_ratings.count())
df_ratings.show()
df_ratings.describe().show()

df_names.printSchema()
print("Кількість стовпців: ", len(df_names.columns))
print("Кількість рядків: ", df_names.count())
df_names.show()
df_names.describe().show()

df_names.select("birthYear", "deathYear").summary().show()
df_basics.select("startYear", "endYear", "runtimeMinutes").summary().show()
df_ratings.select("averageRating", "numVotes").summary().show()
df_episode.select("seasonNumber", "episodeNumber").summary().show()

# Фільми, які вийшли у 3+ країнах
q1 = df_akas.groupBy("titleId") \
    .agg(countDistinct("region").alias("country_count")) \
    .filter(col("country_count") >= 3) \
    .join(df_basics, df_akas.titleId == df_basics.tconst) \
    .select("primaryTitle", "country_count") \
    .distinct()

q1.show()
q1.write.csv("output/q1_movies_multi_country_release", header=True, mode="overwrite")

# Фільми після 2010 року, до 90 хв, з рейтингом > 8.0
q2 = df_basics.alias("b") \
    .join(df_ratings.alias("r"), "tconst") \
    .filter((col("titleType") == "movie") &
            (col("startYear").cast("int") >= 2010) &
            (col("runtimeMinutes").cast("int") <= 90) &
            (col("averageRating") > 8.0)) \
    .select("primaryTitle", "startYear", "runtimeMinutes", "averageRating")

q2.show()
q2.write.csv("output/q2_highly_rated_short_movies", header=True, mode="overwrite")

# ТОП-10 жанрів за кількістю фільмів
q3 = df_basics.filter(df_basics.titleType == "movie") \
    .withColumn("genre", explode(split(col("genres"), ","))) \
    .groupBy("genre") \
    .count() \
    .orderBy(desc("count")) \
    .limit(10)

q3.show()
q3.write.csv("output/q3_top_genres", header=True, mode="overwrite")

# Середній рейтинг фільмів за кожен рік (2005–2024)
q4 = df_basics.alias("b") \
    .join(df_ratings.alias("r"), "tconst") \
    .filter((col("startYear").cast("int").between(2005, 2024))) \
    .groupBy("startYear") \
    .agg(avg("averageRating").alias("avg_rating")) \
    .orderBy("startYear")

q4.show()
q4.write.csv("output/q4_avg_rating_2005_2024", header=True, mode="overwrite")

# Найкращий фільм кожного режисера
df_directors = df_crew.select("tconst", explode(split("directors", ",")).alias("director_id"))

q5_joined = df_directors.alias("d") \
    .join(df_basics.alias("b"), "tconst") \
    .join(df_ratings.alias("r"), "tconst") \
    .filter(col("titleType") == "movie") \
    .select("director_id", "primaryTitle", "averageRating")

window = Window.partitionBy("director_id").orderBy(desc("averageRating"))

q5 = q5_joined.withColumn("rank", row_number().over(window)) \
    .filter(col("rank") == 1)

q5.show()
q5.write.csv("output/q5_top_movie_per_director", header=True, mode="overwrite")

# Кумулятивна кількість епізодів по сезонах
q6_grouped = df_episode.filter(col("seasonNumber").isNotNull()) \
    .groupBy("parentTconst", "seasonNumber") \
    .count() \
    .withColumnRenamed("count", "episodes_in_season")

window1 = Window.partitionBy("parentTconst").orderBy("seasonNumber")

q6 = q6_grouped.withColumn("cumulative_episodes", sum("episodes_in_season").over(window1))

q6.show()
q6.write.csv("output/q6_series_cumulative_episodes", header=True, mode="overwrite")

spark_session.stop()
