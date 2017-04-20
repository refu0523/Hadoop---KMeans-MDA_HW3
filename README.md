# Hadoop - K-Means algorithmn

input: K-Means <in> <out> <init> <distance type> <K> <MaxIter>  
distance: 0 for euclidean, 1 for manhattan distance  

Read centroids data in main function   
KMeansMapper: read data point and assigned it to the nearest centroids and compute costs.
input: String  
output: use -1 as key to collect cost into same reducer.  
<centroids index, data point>  
<-1, cost>  

KMeansReducer:   
If key is -1, compute cost.  
If key is centrois index, reaasign the centroids.  






