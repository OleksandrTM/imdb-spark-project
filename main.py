from pyspark.sql import SparkSession

# Створюємо сесію Spark
spark = SparkSession.builder.master("local").appName("TestApp").getOrCreate()

# Створюємо тестовий DataFrame
data = [("Alice", 34), ("Bob", 45), ("Charlie", 23)]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)

# Виводимо дані
df.show()

# Закриваємо сесію Spark
# spark.stop()
