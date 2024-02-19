from pyspark.sql import SparkSession
import os

spark = SparkSession.builder.config("spark.jars.packages", "org.apache.spark:spark-avro_2.12:3.3.1").getOrCreate()

users = spark.read.option("delimiter", ";").csv('./files/users.csv', header=True, inferSchema=True)
users.createOrReplaceTempView("users")

products = spark.read.option("delimiter", ";").csv('./files/products.csv', header=True, inferSchema=True)
products.createOrReplaceTempView("products")

buy = spark.read.option("delimiter", ";").csv('./files/buy.csv', header=True, inferSchema=True)
buy.createOrReplaceTempView("buy")

result = spark.sql("""
    SELECT
        us.id id_cliente,
        us.nome cliente,
        us.telefone contato,
        tbuy.id id_compra,
        prd.nome produto,
        tbuy.qt quantidade,
        (tbuy.qt * prd.preco) total
    FROM users us
    INNER JOIN buy tbuy
        ON us.id = tbuy.user
    INNER JOIN products prd
        ON prd.id = tbuy.product             
""")

result.show()

output_directory='./result/'
result.write.format("avro").save(output_directory)

spark.stop()