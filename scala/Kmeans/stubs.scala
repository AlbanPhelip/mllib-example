import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}


def clustersInfo(model: KMeansModel) = {
  val centroids = model.clusterCenters
  centroids.foreach(l => println(s"Class: ${l(0).toInt}, Age: ${l(1).toInt}, Fair: ${l(2).toInt}"))
}


def featureEngineering(data : RDD[String]): RDD[Vector] = {

	data.map(line => {

	  val values: Array[String] = line.split('|')

    // TODO: transform 'values' to create the following variables
	  val pClass = ???
	  val age = ???
	  val fair = ???
	  val sibsp = ???
	  val parch = ???

	  val numericalData: Array[Double] = Array(pClass, age, fair, sibsp, parch)

	  Vectors.dense(numericalData)
	})

}

// Loading data
// TODO: read file data_titanic.csv

// Feature Engineering
// TODO: use the featureEngineering method to get the cleaned data.
	
// Modelling
// TODO: Train a KMeans model on the data set

// Inspect centroid of each cluster
println("Clusters description")
// TODO: For each cluster, print the centroid information. You can use the clustersInfo method


