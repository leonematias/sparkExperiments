package com.leonematias.randonForest

import com.leonematias.common.Utils
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame


object TitanicRandomForest {

  def main(args: Array[String]): Unit = {
    val randSeed: Long = 123456
    val labelColOrig = "Survived"
    val labelCol = "SurvivedIndexed"


    val spark = Utils.initSpark()

    val basePath = "src/main/resources/data/"
    val origTrainData = Utils.loadCsvDataFrame(spark, basePath + "titanic_train.csv", ",")
    //val testData = Utils.loadCsvDataFrame(spark, basePath + "titanic_test.csv", ",")

    //Split in test and train
    val dataSplit = randomSplitWithStratifiedKFold(origTrainData, labelColOrig, Array(0, 1), 0.8, randSeed)
    val trainData = dataSplit._1
    val testData = dataSplit._2

    //Clean data
    val trainDataCleaned = cleanData(trainData)
    val testDataCleaned = cleanData(testData)


    //trainDataCleaned.printSchema()
    //trainDataCleaned.show(20)

    //Train classifier
    val classifier = new RandomForestClassifier()
      .setLabelCol(labelCol)
      .setFeaturesCol("Features")
      .setNumTrees(100)
    val model = classifier.fit(trainDataCleaned)

    //Predict test data
    val predictions = model.transform(testDataCleaned)

    //Evaluate quality of predictions
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol(labelCol)
      .setPredictionCol("prediction")
      .setMetricName("f1")
    val f1 = evaluator.evaluate(predictions)
    println(s"F1: $f1")

    //Show some prediction samples
    predictions.select("Age", "Embarked", "Sex", "Pclass", "SibSp", "Parch", "Survived", "prediction", "probability")
      .show(50)

  }

  def cleanData(origData: DataFrame): DataFrame = {
    var df: DataFrame = origData

    //df = df.drop("PassengerId", "Name", "Cabin", "Ticket")

    df = fillWithMean(df, "Age")
    df = df.na.fill("S", Array("Embarked"))
    df = stringIndexColumn(df, "Embarked")
    df = stringIndexColumn(df, "Sex")
    df = stringIndexColumn(df, "Pclass")
    df = stringIndexColumn(df, "SibSp")
    df = stringIndexColumn(df, "Parch")
    if(df.columns.contains("Survived")) {
      df = stringIndexColumn(df, "Survived")
    }

    df = new VectorAssembler()
      .setInputCols(Array("Age", "EmbarkedIndexed", "SexIndexed", "PclassIndexed", "SibSpIndexed", "ParchIndexed"))
      .setOutputCol("Features")
      .transform(df)

    df
  }

  def fillWithMean(df: DataFrame, column: String): DataFrame = {
    val mean = df.select(org.apache.spark.sql.functions.mean(df(column))).first()(0).asInstanceOf[Double]
    df.na.fill(mean, Array(column))
  }

  def stringIndexColumn(df: DataFrame, column: String): DataFrame = {
    new StringIndexer()
      .setInputCol(column)
      .setOutputCol(column + "Indexed")
      .fit(df)
      .transform(df)
  }

  /**
    * Split between train and test set using Stratified K Fold
    */
  def randomSplitWithStratifiedKFold(df: DataFrame, labelColumn: String, labels: Iterable[Int], trainWeight: Double, randSeed: Long): (DataFrame, DataFrame) = {
    val weightsArray = Array(trainWeight, 1 - trainWeight)
    var trainData: DataFrame = null
    var testData: DataFrame = null
    labels.foreach{label =>
      val dataSplit = df.filter(df(labelColumn) === label)
        .randomSplit(weightsArray, randSeed)
      if(trainData == null) {
        trainData = dataSplit(0)
        testData = dataSplit(1)
      } else {
        trainData = trainData.union(dataSplit(0))
        testData = testData.union(dataSplit(1))
      }
    }
    (trainData, testData)
  }

}
