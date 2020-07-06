import sys
import math
import time
import csv
from pyspark import SparkConf,SparkContext


def dist(a,b):
    sum=0
    for i in range(0,len(a)):
        sum = sum +  (a[i]-b[i])*(a[i]-b[i])
    sum = math.sqrt(sum)
    return sum

## calculate minimum distance between point and centroids
def minimum(a,b):
    small=sys.maxsize
    pointa=[]
    for i in b:
        if dist(a,i) < small:
            small = dist(a,i)
    smalllist=[]
    smalllist.append(small)
    smalllist.append(a)
    return smalllist

def mapping(w,b):
    close=sys.maxsize
    for i in range (0,len(b)):
        d=dist(w,b[i])
        if d < close:
            close = d
            cluster = i
    keyvalue=(w,cluster)
    return keyvalue

def find1(a,b):
    for i in range(0,len(b)):
        if a==b[i]:
          return i

start = time.time()
##read input file
point=[]
small=[]
label=[]
f=open(sys.argv[1],'r')
read = csv.reader(f)
check=0
for line in read:
    if check==0:
      check = 1
      continue
    small=[]
    for i in range(0,len(line)):
      if i==3:
        label.append(line[i])
      if i>=5:
	small.append(int(line[i]))
    point.append(small)

init_point=point[0]
k=int(sys.argv[2])

#set first centroid which was first point
centroid=[]
centroid.append(init_point)
point.remove(init_point)

genre=[]
genre.append(label[0])
label.remove(label[0])


#pick other k-1 centroid (if k is large it is most expensive)
while len(centroid)!=k:
    mid=[]
    test=[]
    for j in point:
        mid.append(minimum(j,centroid))
    ##pick new centroid
    for l in mid:
        test.append(l[0])
    for l in range(0,len(test)):
        if test[l] == max(test):
            break
    a=find1(mid[l][1],point)
    point.remove(mid[l][1])
    genre.append(label[a])
    label.remove(label[a])
    centroid.append(mid[l][1])

# to assign cluster use spark parallelize

conf = SparkConf()
sc = SparkContext(conf=conf)
points=sc.parallelize(point)
pairs=points.map(lambda w:mapping(w,centroid))
cluster=pairs.collect()
sc.stop()

hit = 4
for i in cluster:
    a=find1(i[0],point)
    if label[a]==genre[i[1]]:
      hit = hit + 1

accuracy = hit*2/(len(point)+4.0)
print(accuracy)
end = time.time()
print(end - start)
