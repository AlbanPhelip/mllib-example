import numpy as np
import scipy.sparse as sps
from pyspark.mllib.linalg import Vectors
from pyspark.mllib.clustering import KMeans, KMeansModel

def clustersInfo(model):
	centroids = model.clusterCenters
	for c in centroids:
		print "Class: ", int(c[0]), ", Age: ", int(c[1]), ", Fair: ", int(c[2])

def transformLine(line):
	values = line.split('|')

	# TODO: transform 'values' which is an array of String to an array of Double to be read by MLlib
	# Carefull: you need to deal with missing data (in this dataset all missing data are reprsented by 'NA')
	numericalData = ???
	return Vectors.dense(numericalData)


def featureEngineering(data):
	return data.map(transformLine)


# Loading data
# TODO: read file ./src/main/resources/data_titanic.csv

# Feature Engineering
# TODO: use the featureEngineering method to get the cleaned data.
	
# Modelling
# TODO: Train a KMeans model on the data set

# Inspect centroid of each cluster
print "Clusters description"
# TODO: For each cluster, print the centroid information. You can use the clustersInfo method in


