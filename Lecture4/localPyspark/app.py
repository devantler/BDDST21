from pyspark import SparkConf, SparkContext
import locale
locale.getdefaultlocale()
locale.getpreferredencoding()

# Limit cores to 1, and tell each executor to use one core = only one executor is used by Spark
conf = SparkConf().set('spark.executor.cores', 1).set('spark.cores.max',1).set('spark.executor.memory', '1g').set('spark.driver.host', '127.0.0.1')
sc = SparkContext(master='local', appName='pyspark-local', conf=conf)

# Sort and collect to a Python list and print
print("It works!!")