import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
conf = SparkConf()
sc = SparkContext(conf=conf)

text = sc.textFile(r"c:\Users\Mohfly\Desktop\dataset.txt")
words = text.flatMap(lambda line: line.split())
letters = words.map(lambda word: word[0])
pairs = letters.map(lambda letter: (letter,1))
result = pairs.reduceByKey(lambda  i,j: i+j)
show = result.collect()
print(show)