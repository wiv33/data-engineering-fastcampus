from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName("category-review-average")
sc = SparkContext(conf=conf)

directory = "/Users/auto/github/spark/data/01"
filename = "restaurant_reviews.csv"

lines = sc.textFile(f'file:///{directory}/{filename}')

print(lines.collect())

print(lines.collect())
assert len(lines.collect()) > 1
header = lines.first()
filtered_lines = lines.filter(lambda line: line != header)

assert lines.count() - 1 == filtered_lines.count()
print(filtered_lines.collect())


def parse(row):
    fields = row.split(",")
    category = fields[2]
    reviews = int(fields[3])
    return category, reviews


category_reviews = filtered_lines.map(parse)
print("category_reviews", category_reviews.collect())
""" result
[
    ('중식', 125), ('중식', 235), ('분식', 32), 
    ('분식', 534), ('일식', 223), ('일식', 52), 
    ('일식', 12), ('아시안', 312), ('패스트푸드', 12), 
    ('패스트푸드', 23)
]
"""

category_reviews_count = category_reviews.mapValues(lambda x: (x, 1))
print("category_reviews_count", category_reviews_count.collect())
""" result 
[
    ('중식', (125, 1)), 
    ('중식', (235, 1)), 
    ('분식', (32, 1)), 
    ('분식', (534, 1)), 
    ('일식', (223, 1)), 
    ('일식', (52, 1)), 
    ('일식', (12, 1)), 
    ('아시안', (312, 1)), 
    ('패스트푸드', (12, 1)), 
    ('패스트푸드', (23, 1))
]
"""

reduced = category_reviews_count.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
print("reduced", reduced.collect())
""" result
[
    ('중식', (360, 2)), 
    ('분식', (566, 2)), 
    ('일식', (287, 3)),
    ('아시안', (312, 1)),
    ('패스트푸드', (35, 2))
]
"""

average = reduced.mapValues(lambda x: x[0] / x[1])
print("average", average.collect())
""" result
[
    ('중식', 180.0), 
    ('분식', 283.0), 
    ('일식', 95.66666666666667), 
    ('아시안', 312.0), 
    ('패스트푸드', 17.5)
]
"""

