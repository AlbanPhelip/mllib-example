#import numpy as np
#import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.clustering import KMeans, KMeansModel

def clustersInfo(model):
	centroids = model.clusterCenters
	for c in centroids:
		print "Class: ", int(c[0]), ", Age: ", int(c[1]), ", Fair: ", int(c[2])

def transformLine(line):
	values = line.split('|')
	pClass = float(values[0])
	if values[4] == 'NA':
		age=28.0
	else:
		age=float(values[4])
	sibsp = float(values[5])
	parch = float(values[6])
	if values[8] == 'NA':
		fair=14.45
	else:
		fair=float(values[8])
	numericalData = [pClass, age, fair, sibsp, parch]
	return Vectors.dense(numericalData)


def featureEngineering(data):
	return data.map(transformLine)


# Loading data
rawData = sc.textFile(".../data_titanic.csv")

# Feature Engineering
cleanData = featureEngineering(rawData)
cleanData.cache()

# Modelling
model = KMeans.train(cleanData, 3, 50)

# Inspect centroid of each cluster
println("Clusters description")
clustersInfo(model)


