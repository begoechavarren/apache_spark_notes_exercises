import sys
from pyspark import SparkContext
from pyspark import SparkConf

# spark-submit countingjpgs.py "/tmp/curso/weblogs/*" 

def count_jpgs(logfile, targetmodels):
    logs = sc.textFile(logfile)
    logs_jpg = logs.filter(lambda x: ".jpg" in x).map(lambda x: x.split(" ")[6][1:].split(".")[0])
    logs_tm = logs_jpg.filter(lambda x: x in targetmodels.value) # need to obtain value of broadcast object
    return logs_tm.count()

if __name__ == '__main__':
    sconf = SparkConf().setAppName("Counting JPGS App")
    sc = SparkContext(conf= sconf)
    
    logfile = sys.argv[1]

    targetmodels = sc.textFile("/home/bego/Documents/curso_spark/datasets/targetmodels.txt").map(lambda x: x.replace(" ", "_").lower())
    targetmodels = [e[0] for e in targetmodels.zipWithUniqueId().collect()]
    targetmodelsbc = sc.broadcast(targetmodels)

    print("************Number of JPG requests: {}*****************".format(count_jpgs(logfile, targetmodelsbc)))
