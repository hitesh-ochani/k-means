
#parameters are csv file name, output file num
# As of now values of shingle length and centroids are stored in variables k and TotalCentroids

import sys
import csv
import random
import re

pattern = re.compile(r'\s+')

from pyspark import SparkConf, SparkContext
conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

name_of_file=sys.argv[2]	#sys.argv[2] #sys.argv[6]
k=5		#sys.argv[3] #sys.argv[2]		#shingle length  #5
TotalCentroids=4	#sys.argv[4] #sys.argv[5]
vectorsLength=100 #sys.argv[3]		#signature size #5
RandomAB=200	#sys.argv[4] # For random range of A, B and seed  
kMeansIteration=100


#rdd=sc.textFile(sys.argv[1])
# document id begins with zero
# shingle id begins with zero


#Reading file
list1=[]
#with  open('Articles100.csv', 'rU') as csvfile:
with  open(sys.argv[1], 'rU') as csvfile:
    spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
    for row in spamreader:
		#print("---->")
		#print ''.join(row[0])
		#sentence = re.sub(pattern, '', sentence)
		list1.append(re.sub(pattern,' ', ''.join(row[0])))
		#print("<----")



a=sc.parallelize(list1)
print(a.collect())
#a contains a list of articles

# to get shingles 
# [word[i:i + n] for i in range(len(word) - n + 1)]
# source http://tirsen.com/2011/08/26/shingles.html
def shingles(x,n):
	return [x[i:i + n] for i in range(len(x) - n + 1)]



#shingles("start",k)

b=a.map(lambda x: shingles(x,k))
#b contains shingles of length 5

c=b.zipWithIndex().map(lambda (x, y): (y,x))
#c contains a unique id 1,2,...n assumed as a document id, and corresponding list of shingles in that.
#c.collect()

#for permutation
def prime_greater_than(x):
	seed=x+1
	if(seed in (2,3,5,7)):
		return(seed)
	while True:
		flag=True
		for d in range(2,seed):
			if(seed % d == 0):
				seed=seed+1
				flag=False
				break
		if(flag):
			#print(str(seed)+" is prime")
			break
	return(seed)



#prime_greater_than(2)
#prime_greater_than(10)

#Working for two, later, t can be simply removed and used 2 instead
#t=sc.parallelize(c.collect())
t=c

def get_multi_keys(x):
	#[blu[0][0] for x in range(len(blu[0]))]
	list2=[]
	for elem in x[0]:
		tup=(x[1],elem)
		list2.append(tup)
		#print(x[1],elem)
	return (list2)



d=t.map(lambda (x,y):(y,x)).map(lambda x: get_multi_keys(x))
#contains [(doc,sh1),(doc,sh2)],[(doc2,sh1),...], ... or vice versa shingle first, document second
#d.collect()

e=d.flatMap(lambda xs: [x for x in xs])
#e has shingle,doc list

#temp1=sc.parallelize([(2,'Artic'), (2,'rticl'), (3,'ticle')])
#f=e.union(temp1)
#(shingle,document) list

f=e


#distinct (shingle,document)
g=f.distinct()
#distince (document,shingle) pairs

#make it like a tuple - redundant function as I am not using it anymore
def makeTuple(x):
	tuple=(x)
	return tuple

#logic to group by key,  I created a string with delimitter underscore.
def concatMe(x,y):
	return [str(x[0])+"_"+str(y[0])]

#removed underscore.
def unConcatMe(x,y):
	listx=[]
	mystrings = y[0].split("_")
	for sttrr in mystrings:
		listx.append(int(sttrr))
	return (x,listx)


my_h=g.map(lambda (x,y):(y,[str(x)])).reduceByKey(lambda x,y: concatMe(x,y))
my_h1=my_h.map(lambda (x,y): unConcatMe(x,y))


#h=g.map(lambda (x,y):(y,makeTuple(x))).reduceByKey(lambda (x),(y):(x,y))
#Unionshingles=g.map(lambda (x,y):(y,x)).reduceByKey(lambda x,y:(x,y))

h=my_h1
Unionshingles=my_h1
Unionshingles.coalesce(1).saveAsTextFile("Unionshingles"+str(name_of_file)+"docs.txt")

UnionshinglesCount=Unionshingles.count()

#UnionshinglesWithId=Unionshingles.zipWithIndex().map(lambda (x, y): (y,x))
#UnionshinglesWithId.collect()


JustIdAndShingle=Unionshingles.map(lambda (x,y):x).zipWithIndex().map(lambda (x, y): (y,x))
# user generated shingle id and shingle
#JustIdAndShingle.collect()


#used by signature
def function1(x):
	list2=[-1 for w in range(vectorsLength)]
	return (x,list2)



#Signature matrix
#signature=c.map(lambda (x,y):x).map(lambda x: function1(x) ).collect()
#returns next element but I am not using it anymore.
def my_hash(a,b,p,currValue):
	nextValue=(a*currValue+b)%p
	return nextValue




A=[random.randint(1,RandomAB) for x in range(vectorsLength)]
B=[random.randint(1,RandomAB) for x in range(vectorsLength)]
Prime=prime_greater_than(UnionshinglesCount+1)


#def fillSignatures():
#Signature contains elements of the form (doc id, [signature])
signature=c.map(lambda (x,y):x).map(lambda x: function1(x) ).collect()
TotalDocs=len(signature)-1
print("Total docs are "+str(TotalDocs))
print("Shingle count is "+str(UnionshinglesCount))

#JustIdAndShingle.cache()
#Unionshingles.cache()
for column in range(vectorsLength):
	seed=random.randint(1,RandomAB)
	#print("seed is "+str(seed))
	fullTracker=0
	currSignature=0
	for x in range(UnionshinglesCount):
		#print(seed)
		seed=(A[column]*seed+B[column])%Prime
		currSignature += 1
		shingg1=JustIdAndShingle.cache().lookup(seed)
		if(len(shingg1)>0):
			shingg=shingg1[0]
			doc_list=Unionshingles.cache().lookup(shingg)[0]
			#print(type(doc_list))
			if(isinstance(doc_list,list)):
				for a in doc_list:
					if(signature[a][1][column] == -1):	#1 just refers to second element in tuple which is signature list
						signature[a][1][column]=currSignature
						#print(str(signature[a][1][column]))
						if(a!=0):
							fullTracker = fullTracker + 1
					else:
						continue
			if(TotalDocs <= fullTracker):
				# makes it efficient, when signature is filled, it stops.
				print("Total Docs reached when currSignature is "+str(currSignature))
				break
	print("Done filling signature of column "+str(column))
	print("Full tracker is " + str(fullTracker))
	#print(signature)




signatureAsRDD=sc.parallelize(signature[1:])
signatureAsRDD.collect()
#overwriting original signature
signatureAsRDD.coalesce(1).saveAsTextFile("signature"+str(name_of_file)+"docs.txt")


kCentroids=[[]]
# range till UnionshinglesCount
#global variable
kCentroids=[[random.randint(1,RandomAB) for w1 in range(vectorsLength)] for w2 in range(TotalCentroids)]
kCentroidRDDBackup=sc.parallelize(kCentroids)
#kCentroids=[[random.randint(1,10) for w1 in range(vectorsLength)] for w2 in range(TotalCentroids)]



def meraSum(list1,list2):
	sum=[0 for x in range(len(list1))]
	for x in range(len(list1)):
		sum[x]=sum[x]+list1[x]+list2[x]
	return sum



def compuAvg(listOfDoc):
	a=[[]]
	for doc in listOfDoc:
		a.append(signature[doc][1])
	a=a[1:] #because 0th element is blank
	sumblu=[0 for x in range(vectorsLength)]
	for x in a:
		sumblu=meraSum(sumblu,x)
	avgblu=[float(format(1.0*x/len(a),".2f")) for x in sumblu]
	return avgblu



def meraDistance(list1,list2):
	sum=0
	for x in range(len(list1)):
		sum=sum+(list1[x]-list2[x])**2
	sum=sum**0.5
	return sum



#returns centroid number - e.g. 0,1,2,3,4,5
def distCentr(docc):
	#print("docc 1 is ")
	#print(docc[1])
	sum1=[]
	for x in range(len(kCentroids)):
		sum1.append(0)
		sum1[x]=meraDistance(kCentroids[x],docc[1])
	my_min=sum1.index(min(sum1))
	#print("my_min is ")
	#print(my_min)
	tuples=(my_min,docc[0])		#*****should be docc[0]
	return tuples
	#return(docc[0][1])



#Iterating for k-means
for num_of_Iter in range(kMeansIteration):
	centrRDD=signatureAsRDD.map(lambda x: distCentr(x))
	centrRDD.collect()
	#contains (centroid#,document#) i.e. centroid associated with document
	centrDocList=centrRDD.map(lambda (x,y):(x,[str(y)])).reduceByKey(lambda x,y: concatMe(x,y)).map(lambda (x,y): unConcatMe(x,y))
	cdl=centrDocList.collect()
	print(cdl)
	#contains (centroid, [doc list])
	for x in cdl:
		#x[0] #centroid
		kCentroids[x[0]]=compuAvg(x[1])
	kCentroidRDDBackup=sc.parallelize(kCentroids)


print("Final Centroids are ")
print(kCentroids)
kCentroidRDDBackup.coalesce(1).saveAsTextFile("last centroid list "+str(name_of_file)+"doc.txt")

centrDocList.coalesce(1).saveAsTextFile("centroid doc list "+str(name_of_file)+"doc.txt")


