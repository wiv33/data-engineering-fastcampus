from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("transformations_actions")
sc = SparkContext(conf=conf)

print(sc.getConf().getAll())

# sc가 있는 상태에서 또다른 sc를 만드는 경우 에러가 발생
sc.stop()

new_sc = SparkContext(conf=conf)

foods = new_sc.parallelize(["짜장면", "짬뽕", "김밥", "떡볶이", "라멘", "돈가스", "우동", "쌀국수", "햄버거", "치킨"])

print(foods)

print(foods.collect())

print(foods.countByValue())

print(foods.count())

