# Bigram
Spark	program	to computes	the	frequency	of	each	bigram	in	sherlock.txt
and	outputs	the (bigram,	count)	pairs	for	the	top	100	most	frequent	bigrams

## Submitting a Spark Job
Putting data file on HDFS
```
hadoop fs -copyFromLocal sherlock.txt
```
Run the python program using spark
```
spark-submit bigram.py sherlock.txt
```
output
```
hadoop fs -getmerge bigram.out bigram.out
```
