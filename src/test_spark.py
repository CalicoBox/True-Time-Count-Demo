## Imports
from pyspark import SparkConf, SparkContext
 
## Module Constants
APP_NAME = "Test Spark Application"
 
## Closure Functions
 
## Main functionality
 
def main(sc):
	logFile = "test_log"
	rdd = sc.textFile(logFile).cache()

	counts = rdd.map(lambda line: line.split()[8]).map(lambda word: (word, 1)).reduceByKey(test).sortByKey(lambda x: x) 

	output = counts.collect()  
	for (word, count) in output:  
	    print "%s: %i" % (word, count)

	sc.stop()	
def test(a, b):
	print a + "|" + b
	return a + b

if __name__ == "__main__":
	# Configure Spark
	conf = SparkConf().setAppName(APP_NAME)
	conf = conf.setMaster("local[*]")
	sc   = SparkContext(conf=conf)
	# Execute Main functionality
	main(sc)