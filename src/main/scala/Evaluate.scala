import org.apache.spark.rdd.RDD

class Evaluate(true_ratings : RDD[((Int, Int), Double)], predicts : RDD[((Int, Int), Double)]) {

  val full_join = fullJoinTruePredict()
  val true_predicts_project = truePredictProject()
  val diff = showDiff()
  val count_diff = countDiff()
  val RMSE = computeRMSE()
  val predict_mean = getPredictMean()

  protected def getPredictMean() : Double = {
    val pred_len = predicts.collect.length
    val sum = predicts.map{
      line => line._2
    }.reduce(_+_)
    //  NA replacement
    val predict_mean = sum / pred_len
    predict_mean
  }

  protected def fullJoinTruePredict() : RDD[(Int, Int, Double, Double)] = {
    val true_predicts = true_ratings.fullOuterJoin(predicts)
      .map(row => {
        if (row._2._2 == None)  (row._1._1, row._1._2, row._2._1.getOrElse(Double).asInstanceOf[Double], predict_mean)
        else (row._1._1, row._1._2, row._2._1.getOrElse(Double).asInstanceOf[Double], row._2._2.getOrElse(Double).asInstanceOf[Double])
      })
    true_predicts
  }

  protected def truePredictProject() : RDD[((Double, Double))] = {
    val true_predicts_project = full_join.map(line => (line._3, line._4))
    true_predicts_project
  }

  protected def showDiff() : RDD[Double] = {
    val diff = true_predicts_project.map {
      line => scala.math.abs(line._1 - line._2)
    }
    diff
  }

  protected def computeRMSE() : Double = {
    val MSE = diff.map {
        err => err * err
    }.mean()
    val RMSE = scala.math.sqrt(MSE)
    RMSE
  }

  protected def countDiff() : RDD[(String, Int)] = {
    val count_diff = diff.map( _ match {
      case x if (x >= 0 && x < 1) => (">=0 and <1:", 1)
      case x if (x >= 1 && x < 2) => (">=1 and <2:", 1)
      case x if (x >= 2 && x < 3) => (">=2 and <3:", 1)
      case x if (x >= 3 && x < 4) => (">=3 and <4:", 1)
      case x if (x >= 4) => (">=4:", 1)
    }).reduceByKey(_+_)
    count_diff
  }

}
