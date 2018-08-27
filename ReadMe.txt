WordCount algorithm - MapReduce.

1. Remove stop words during the map phase, and consider only meaningful words i.e. words other than stop words.
2. Remove words that are less than 5 characters in length, special characters (e.g. "," or "."), and convert all words to lowercase.
3. In the reduce phase, generate a total count for each key i.e. word and output that to a HDFS file.