import sys 

from pyspark import SparkContext




if __name__ == "__main__":




  if len(sys.argv) < 2:
        print >> sys.stderr, "Usage: CountJPGs.py <logfile>"
        exit(-1)
        
    sc = SparkContext()
    file = sys.argv[1]
    count = sc.textFile(file).filter(lambda line: '.jpg' in line).count()
    print "Num JPG requests: ", count
sc.stop()






