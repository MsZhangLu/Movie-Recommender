import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

class Output(full_join : RDD[(Int, Int, Double, Double)], count_diff : RDD[(String, Int)], RMSE : Double) {

  val file_predictions = full_join.collect.map(x => (x._1, x._2, x._4))
    .toSeq.sortWith(_._2 < _._2).sortWith(_._1 < _._1)
    .map(x => x._1 + "," + x._2 + "," + x._3)
  var file = new ArrayBuffer[Any]()
  val file_header = "UserId,MovieId,Pred_rating"
  file = file :+ file_header
  var count_f = 0
  val len_f =  file_predictions.length
  while(count_f < len_f) {
    file = file :+ file_predictions(count_f)
    count_f += 1
  }

  //    sc.parallelize(file).repartition(1).saveAsTextFile("./Lu_Zhang_result_task2")

  var output = new ArrayBuffer[Any]()
  val diff_sorted = count_diff.collect
    .toSeq.sortWith(_._1.substring(2,3).toInt < _._1.substring(2,3).toInt)
    .map(x => x._1 + "" + x._2)
  val len = diff_sorted.length
  var count = 0
  while(count < len) {
    output = output :+ diff_sorted(count)
    count += 1
  }
  output = output :+ RMSE
  output.foreach(println)

}
