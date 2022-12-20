from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName("category-review-average")
sc = SparkContext(conf=conf)

directory = "/Users/auto/github/spark/data/01"
filename = "restaurant_reviews.csv"

lines = sc.textFile(f'file:///{directory}/{filename}')

print(lines.collect())
