from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
import re

def map_phase(x):
    x = re.sub('--', ' ', x)
    x = re.sub("'", '', x)
    return re.sub('[?!@#$\'",.;:()]', '', x).lower().split(' ')

def split(line):
    words = line.split(" ")
    return [(words[i], words[i+1]) for i in range(len(words)-1)]


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: bigram <file>", file=sys.stderr)
        exit(-1)
    sc = SparkContext()
    lines = sc.textFile(sys.argv[1], 1)
    sentences = lines.flatMap(map_phase).glom() \
                  .map(lambda x: " ".join(x)) \
                  .flatMap(lambda x: x.split(".")) \
                  .map(lambda x: x.encode('utf-8'))

    bi_counts = sentences.flatMap(lambda line: split(line))\
        .map(lambda x: (x, 1))\
        .reduceByKey(add)

    output = sc.parallelize(bi_counts.map(lambda (k,v): (v,k)).sortByKey(False).take(100))


    output.coalesce(1,True).saveAsTextFile("bigram.out")


    sc.stop()
