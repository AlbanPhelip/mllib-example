import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}

def clustersInfo(model: KMeansModel) = {

  val centroids = model.clusterCenters

  centroids.foreach(l => println(s"Class: ${l(0).toInt}, Age: ${l(1).toInt}, Fair: ${l(2).toInt}"))
}


def featureEngineering(data : RDD[String]): RDD[Vector] = {

	data.map(line => {

	  val values = line.split('|')

	  val pClass = values(0).toDouble
	  val age = values(4) match {
	    case "NA" => 28d
	    case l => l.toDouble
	  }
	  val sibsp = values(5).toDouble
	  val parch = values(6).toDouble
	  val fair = values(8) match {
	    case "NA" => 14.45
	    case l => l.toDouble
	  }

	  val numericalData: Array[Double] = Array(pClass, age, fair, sibsp, parch)

	  Vectors.dense(numericalData)
	})

}

 // Loading data
val rawData: RDD[String] = sc.textFile(".../data_titanic.csv")

// Feature Engineering
val cleanData: RDD[Vector] = featureEngineering(rawData)

// Modelling
val model: KMeansModel = KMeans.train(cleanData, 2, 50)

// Inspect centroid of each cluster
println("Clusters description")
clustersInfo(model)
