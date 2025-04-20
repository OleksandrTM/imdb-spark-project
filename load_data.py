from pyspark.sql import SparkSession
from pyspark.sql.types import *


def load_title_akas(spark, path):
    schema = StructType([
        StructField("titleId", StringType(), True),
        StructField("ordering", IntegerType(), True),
        StructField("title", StringType(), True),
        StructField("region", StringType(), True),
        StructField("language", StringType(), True),
        StructField("types", StringType(), True),  # масив, але зчитуємо як string
        StructField("attributes", StringType(), True),
        StructField("isOriginalTitle", IntegerType(), True)
    ])
    return spark.read.option("header", True).option("sep", "\t").option("nullValue", "\\N").schema(schema).csv(path)


def load_title_basics(spark, path):
    schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("titleType", StringType(), True),
        StructField("primaryTitle", StringType(), True),
        StructField("originalTitle", StringType(), True),
        StructField("isAdult", IntegerType(), True),
        StructField("startYear", IntegerType(), True),
        StructField("endYear", IntegerType(), True),
        StructField("runtimeMinutes", IntegerType(), True),
        StructField("genres", StringType(), True)
    ])
    return spark.read.option("header", True).option("sep", "\t").option("nullValue", "\\N").schema(schema).csv(path)


def load_title_crew(spark, path):
    schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("directors", StringType(), True),
        StructField("writers", StringType(), True)
    ])
    return spark.read.option("header", True).option("sep", "\t").option("nullValue", "\\N").schema(schema).csv(path)


def load_title_episode(spark, path):
    schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("parentTconst", StringType(), True),
        StructField("seasonNumber", IntegerType(), True),
        StructField("episodeNumber", IntegerType(), True)
    ])
    return spark.read.option("header", True).option("sep", "\t").option("nullValue", "\\N").schema(schema).csv(path)


def load_title_principals(spark, path):
    schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("ordering", IntegerType(), True),
        StructField("nconst", StringType(), True),
        StructField("category", StringType(), True),
        StructField("job", StringType(), True),
        StructField("characters", StringType(), True)
    ])
    return spark.read.option("header", True).option("sep", "\t").option("nullValue", "\\N").schema(schema).csv(path)


def load_title_ratings(spark, path):
    schema = StructType([
        StructField("tconst", StringType(), True),
        StructField("averageRating", FloatType(), True),
        StructField("numVotes", IntegerType(), True)
    ])
    return spark.read.option("header", True).option("sep", "\t").option("nullValue", "\\N").schema(schema).csv(path)


def load_name_basics(spark, path):
    schema = StructType([
        StructField("nconst", StringType(), True),
        StructField("primaryName", StringType(), True),
        StructField("birthYear", IntegerType(), True),
        StructField("deathYear", IntegerType(), True),
        StructField("primaryProfession", StringType(), True),
        StructField("knownForTitles", StringType(), True)
    ])
    return spark.read.option("header", True).option("sep", "\t").option("nullValue", "\\N").schema(schema).csv(path)

