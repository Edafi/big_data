import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import time
from faker import Faker
from faker.providers.company.ru_RU import Provider
import random

spark = SparkSession.builder \
    .appName("TestSpark") \
    .config("spark.driver.host", "localhost") \
    .config("spark.security.manager", "false") \
    .config("spark.ui.port", "4040") \
    .appName("TestSpark") \
    .getOrCreate()

model = Faker('ru_RU')
model.add_provider(Provider)
fake_dataset1 = []
for i in range(1000000):
    k = random.randint(0, 1)
    if k == 1:
        name = model.company()
    else:
        name = model.name()
    bank_account = model.bban()
    fake_dataset1.append((i, name, bank_account))
     
v = spark.createDataFrame(fake_dataset1, schema = ['id', 'Client', 'Bank Account'])
rdd = spark.sparkContext.parallelize(fake_dataset1, 4)
rdd.filter(lambda company: company == 'Янлекс').collect()
#rdd.map(lambda company: company.upper()).collect()
grouped_rdd = rdd.groupBy(lambda company: company[0]).map(lambda x: (x[0], list(x[1])))
grouped_rdd.collect()
rdd.groupBy(lambda company: company[0]).collect()
from operator import add
b = rdd.map(lambda x: (x, 1)).reduceByKey(add).collect()
for (w, c) in b:    
    print("{}: {}".format(w, c))

fake_dataset2 = []

for i in range(200000):
    src = random.randint(0, 250)
    dst = random.randint(0, 300)
    summa = random.randint(0, 10000)
    fake_dataset2.append((src, dst, summa))

e = spark.createDataFrame(fake_dataset2, schema = ['src', 'dst', 'summa'])
#e.count()
#v.count()
v.show()
e.show()

try:
    while True:
        time.sleep(10)  
except KeyboardInterrupt:
    print("Stopping Spark application...")

spark.stop()
