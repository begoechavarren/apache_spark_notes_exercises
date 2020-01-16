import sys
from pyspark import SparkContext
from pyspark import SparkConf

# spark-submit countingfiletypes.py "/tmp/curso/weblogs/*" 

def count(line):
    files = {"html":html, "css":css, "jpg": jpg}
    for e in files:
        if e in line:
            files[e] +=1

if __name__ == '__main__':
    sconf = SparkConf().setAppName("Counting file types App")
    sc = SparkContext(conf= sconf)
    
    logfile = sys.argv[1]

    html = sc.accumulator(0)
    css = sc.accumulator(0)
    jpg = sc.accumulator(0)

    logs = sc.textFile(logfile)
    logs.foreach(lambda line: count(line))

    print("""
    Total requests:\n
    .css --> {}\n
    .html --> {}\n
    .jpg --> {}""".format(css.value, html.value, jpg.value))
