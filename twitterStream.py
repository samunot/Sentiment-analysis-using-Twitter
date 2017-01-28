from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import operator
import numpy as np
import matplotlib.pyplot as plt


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("Streamer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)   # Create a streaming context with batch interval of 10 sec
    ssc.checkpoint("checkpoint")

    pwords = load_wordlist("positive.txt")
    nwords = load_wordlist("negative.txt")

    counts = stream(ssc, pwords, nwords, 100)
 
    make_plot(counts)


def make_plot(counts):
	positive = []
	negative = []
	for l in counts:
		if l:
			positive.append(l[0][1])
			negative.append(l[1][1])
	print positive
	print negative
	print range(len(positive))
	plt.plot(range(len(positive)),positive,'b',marker='o',label="positive")
	plt.plot(range(len(negative)),negative,'g',marker='o',label="negative")
	plt.ylabel('Word Count')
	plt.xlabel('Time Step')
	plt.xticks(range(len(positive)))
	plt.legend(loc="upper left")
	plt.title('Sentiment Analysis of Tweets')
	plt.show()
	plt.savefig("tw.png")

def load_wordlist(filename):
    f = open(filename,'rU')
    a = (f.read()).split()
    f.close()
    return a

def seg(word,pwords,nwords):
	if word in pwords:
		return ("positive",1)
	elif word in nwords:
		return ("negative",1)		
	else:
		return ("none",1)


def stream(ssc, pwords, nwords, duration):
    kstream = KafkaUtils.createDirectStream(
        ssc, topics = ['twitterstream'], kafkaParams = {"metadata.broker.list": 'localhost:9092'})
    tweets = kstream.map(lambda x: x[1].encode("ascii","ignore"))
    words = tweets.flatMap(lambda x:x.split(" "))

    words = words.filter(lambda word: word in pwords or word in nwords)
    pairs = words.map(lambda word: seg(word,pwords,nwords))

    wordCounts = pairs.reduceByKey(lambda x,y:x+y)
 
    def updateFunction(newValues, runningCount):
    	if runningCount is None:
    		runningCount = 0
    	return sum(newValues, runningCount)

    runningCounts = wordCounts.updateStateByKey(updateFunction)
    runningCounts.pprint()
    counts = []
    wordCounts.foreachRDD(lambda t,rdd: counts.append(rdd.collect()))
    
    ssc.start()                         # Start the computation
    ssc.awaitTerminationOrTimeout(duration)
    ssc.stop(stopGraceFully=True)

    return counts

if __name__=="__main__":
    main()
