import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}

/**
 * 
 */
def clustersInfo(model: KMeansModel) = {
  val centroids = model.clusterCenters
  centroids.foreach(l => println(s"Class: ${l(0).toInt}, Age: ${l(1).toInt}, Fair: ${l(2).toInt}"))
}


def featureEngineering(data : RDD[String]): RDD[Vector] = {

	data.map(line => {

	  val values: Array[String] = line.split('|')

	  // TODO: transform 'values' which is an array of String to an array of Double to be read by MLlib

	  val numericalData: Array[Double] = ???

	  Vectors.dense(numericalData)
	})

}

// Loading data
// TODO: read file ./src/main/resources/data_titanic.csv

// Feature Engineering
// TODO: use the featureEngineering method to get the cleaned data.
	
// Modelling
// TODO: Train a KMeans model on the data set

// Inspect centroid of each cluster
println("Clusters description")
// TODO: For each cluster, print the centroid information. You can use the clustersInfo method in


