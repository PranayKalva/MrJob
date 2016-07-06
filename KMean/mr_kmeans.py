# -*- coding: utf-8 -*-
"""
Created on Sat June 17 10:30:48 2016

@author: Pranay Kalva
"""
## To run
# python mr_kmeans.py input.txt > output.txt
##

import os
from mrjob.job import MRJob
from math import sqrt
import csv 

#CSV Reader to read the comma seperated input
def csv_readline(line):
     for row in csv.reader([line]):
        return row
     return 0
     
## To select centroids automatically, we need to add one more mapper/reducer so that the reducer can randomly select data points and write it to an intermediate file
## For simplicity sake, we initialize the cluster centroids manually
init_centroids = [[1.0, 2.5], \
                  [2.0, -1.0]]

#init_centroids = [[-1.0, 1.0], \
#                  [3.0, 3.0]]
#
#init_centroids = [[2.0, 2.0], \
#                  [0.0, 0.0]]

#init_centroids = [[2.0, 2.0], \
#                  [1.0, 1.0], \
#                  [-1.0, -1.0]]
                  
#Method to fetch the current path of the file                  
def GetCurrentPath():  
    return os.path.dirname(os.path.realpath(__file__))

def fileLength():
    num=0
    with open(GetCurrentPath() + '/' + 'input.txt') as openfileobject:
        for line in openfileobject:
            num=num+1
    return num
    
#Method myfunc to exit the loop once the data in the file is completed
def myfunc():
  myfunc.counter += 1
  return myfunc.counter
myfunc.counter = 0

#Method to find the euclidean distance between the two observations
def euclid_dist(x,y):
    sum = 0.0
    for i in range(len(x)):
        temp = x[i] - y[i]
        sum += temp * temp
    return sqrt(sum)

#Class MR_KMeans responsible to calculate centroids    
class MR_KMeans(MRJob):
#List to store data in classes. Sufficient for five classes., ie would work for 5 centroids
    classList=[[],[],[],[],[]]
#List to store distances.     
    dist=[None]*len(init_centroids)
#List to store the new centroids created after each iteration
    new_centroids=[]
    def __init__(self, *args, **kwargs):
        super(MR_KMeans, self).__init__(*args, **kwargs)
        self.fullPath = self.options.pathName

#Configuring path of temp file       
    def configure_options(self):
        super(MR_KMeans, self).configure_options()    
        self.add_passthrough_option('--pathName', dest='pathName', default="", type='str')
   
#Method Steps        
    def steps(self):
        return[self.mr(mapper= self.mapper,
                       reducer=self.reducer,
                       reducer_final=self.finalreducer)]
#Method to find new means ie new centroids
    def mean_newcentroid(self,values):
         sum_x=0
         sum_y=0
         n=0
         del self.new_centroids[:]
         for i in range (len(values)):
             for x,y in values[i]:
                 sum_x=sum_x+x
                 sum_y=sum_y+y
                 n=n+1
             if n!=0:                  
                self.new_centroids.append((sum_x/n,sum_y/n))
                j=self.new_centroids
#initializing all the variables
                n=0
                sum_x=0
                sum_y=0
         return j    

#Mapper Method
    def mapper(self,_,line):  
#Reading the input using csv format
         cell=csv_readline(line)
#Spliting the data and converting it into a list of float variables
         p=(float(cell[0].split( )[0]),float(cell[1].split( )[0]))
#Finding the euclidian distance between the input and the old centroids
         self.dist[0] = euclid_dist(p, init_centroids[0])
         for i in range(1, len(init_centroids)):
            self.dist[i] = euclid_dist(p, init_centroids[i])
#Finding the minimum distance from the list and its index
         x=min(self.dist)
         y=self.dist.index(x)
#Appropriately moving the data into corresponding class list
         self.classList[y].append(p)
#If end of file reached.
         if myfunc()==fileLength():
#Yield the new centroids
            yield(self.mean_newcentroid(self.classList),1)
#Initializing class list and counter variable
            for i in range(len(self.classList)):
                del self.classList[i][:]
            myfunc.counter = 0
            
#Passing the new centroids to the final reducer            
    def reducer(self,key,value):
        self.new_centroids=key

#Opening and writing into a temp file        
    def finalreducer(self):
        self.q=open(self.fullPath,'w')
        self.q.write(str(self.new_centroids))
        self.q.close()
        
#Method RunKMeans
def RunKMeans():
    dataFile = GetCurrentPath() + '/' + 'input.txt'
    tmpFile = GetCurrentPath() + '/' + 'intermediateResults.txt'   
#List for intermidiate centroids
    intermidiate_centroids=[]
    k = 1 
    dist=[None]*len(init_centroids)
    delta = 9999
    old_centroids=init_centroids
## loop through the Kmeans process until it converges
    while (delta > 0.0001 and k<100):
        kmeans = MR_KMeans(args=[dataFile] + ["--pathName", tmpFile])
        with kmeans.make_runner() as runner:
            runner.run()
            del intermidiate_centroids[:]
#Opening the intermidiate file in read mode 
            fileIn=open(tmpFile,'r')
            data_new= fileIn.read()
#Splitting then data to remove [ and ]
            data2= str(data_new).replace('[','').replace(']','').split(", ")
            i=0
            while(i<len(data2)):
                intermidiate_centroids.append((float(data2[i]),float(data2[i+1]))) 
                i=i+2
#Calculate the distance between current and the old centroids                
            dist[0] = euclid_dist(intermidiate_centroids[0], init_centroids[0])
            for i in range(1, len(init_centroids)):
                dist[i] = euclid_dist(intermidiate_centroids[i], init_centroids[i])
#Calculating the maximum distance and assigning it to delta
            delta=max(dist)
            print "Iteration >> "+str(k)+" "+str(delta)
            print "Old Centroids >> "+str(old_centroids)
            print "new_centroids >> "+str(intermidiate_centroids)
#Moving data of current centroids             
            init_centroids[0]=intermidiate_centroids[0]
            for i in range(1, len(init_centroids)):
                init_centroids[i]=intermidiate_centroids[i]
        
        k += 1    
    
if __name__ == '__main__':
    RunKMeans()

