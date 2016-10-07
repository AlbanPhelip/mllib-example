import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.tree.model.DecisionTreeModel
 
def accuracyDecisionTree(model: DecisionTreeModel, data: RDD[LabeledPoint]): Double = {
  val predictionsAndLabels = data.map(l => (model.predict(l.features), l.label))
  val metrics: MulticlassMetrics = new MulticlassMetrics(predictionsAndLabels)
  val accuracy = 100d * metrics.precision
  accuracy
}

def featureEngineering(data : RDD[String]): RDD[LabeledPoint] = {

  data.map(line => {
    val values = line.split('|')

    val label = values(1).toDouble
    val pClass = values(0).toDouble
    val sex = values(3) match {
      case "\"male\"" => 0d
      case "\"female\"" => 1d
    }
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
    val embarked = values(10) match {
      case "\"\"" => 0d
      case "\"C\"" => 1d
      case "\"Q\"" => 2d
      case "\"S\"" => 3d
    }

    val cleanedData = Array(pClass, sex, age, sibsp, parch, fair, embarked)

    LabeledPoint(label, Vectors.dense(cleanedData))
  })
}


// Loading data
val rawData = sc.textFile(".../data_titanic.csv")

// Feature Engineering
val cleanData = featureEngineering(rawData)

// Splitting data
val Array(trainSet, testSet) = cleanData.randomSplit(Array(0.75, 0.25), seed = 54)

// Modelling
// -------- Tuning parameters
val numClass = 2
val categoricalFeaturesInfo = Map(1 -> 2, 6 -> 4)
val impurity: String = "entropy"
val maxDepth: Int = 2
val maxBins: Int = 12

// -------- Training the model
val model = DecisionTree.trainClassifier(trainSet, numClass, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

// Prediction & Evaluation
val accuracyTest = accuracyDecisionTree(model, testSet)

// Print results
println(s"Results for the test set")
println(s"\t Accuracy: $accuracyTest %")
