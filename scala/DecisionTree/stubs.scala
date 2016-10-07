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

    // TODO: extract the label from values (if the person survived or not) 
    val label: Double = ???

    // TODO: transform 'values' to create the following variables
    val pClass: Double = ???
    val sex: Double = ???
    val age: Double = ???
    val sibsp: Double = ???
    val parch: Double = ???
    val fair: Double = ???
    val embarked: Double = ???

    val cleanedData = Array(pClass, sex, age, sibsp, parch, fair, embarked)

    LabeledPoint(label, Vectors.dense(cleanedData))
  })
}


// Loading data
// TODO : read file data_titanic.csv

// Feature Engineering
// TODO : use the featureEngineering method to get the cleaned data
// Be carefull, you will get a RDD[LabeledPoint]

// Splitting data
// TODO : split the cleaned data into a train and test set (proportions 0.75, 0.25) using the 'randomSplit' method on the initial RDD

// Modelling
// -------- Tuning parameters
val numClass = 2
val categoricalFeaturesInfo = Map(1 -> 2, 6 -> 4)
val impurity: String = "entropy"
val maxDepth: Int = 2
val maxBins: Int = 12

// -------- Training the model
// TODO : Train a DecisionTree model on the training set (Use the parameters of your choice)

// Prediction & Evaluation
// TODO : get the precision for the prediction on the test set (You can use the accuracyDecisionTree method)

// Print results
println(s"Results for the test set")
// TODO : print the results obtained for the test set
