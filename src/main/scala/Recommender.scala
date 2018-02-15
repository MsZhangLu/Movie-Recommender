import org.apache.spark.rdd.RDD

class Recommender(train_data : RDD [(Int, Int, Double)], test_data : RDD[(Int, Int)], avg_ratings : RDD[(Int, Double)],
                  avg_test : RDD[(Int, Double)]) {

  // (movie, numRaters)
//  val item0 = test_data.keyBy(_._2)
  val movies_raters = test_data.keyBy(_._2)
    .map(x => (x._1, 1)).reduceByKey(_+_)

  val item2 = test_data.join(train_data.keyBy(_._1))
    .map(x => (x._2._2._2, 1))
    .reduceByKey(_+_)
    .join(train_data.keyBy(_._2))
    .map(x => x._2._2).keyBy(_._2)
    .join(avg_ratings)
    .map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._2)).persist

  val grouped = movies_raters.collect.grouped(500).toList

  var lb = scala.collection.mutable.ListBuffer.empty[List[(Int, Int, Double)]]


  for(gr <- grouped) {
    val i = spark.SparkEnvironment.sc.parallelize(gr)

    val item1 = i.join(train_data.keyBy(_._2))
      .map(x => x._2._2).keyBy(_._2)
      .join(avg_ratings)
      .map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._2))

    val items = item1.keyBy(_._1).join(item2.keyBy(_._1))
      .map(x => ((x._2._1._2, x._2._2._2), (x._2._1._3, x._2._1._4, x._2._2._3, x._2._2._4)))

    val mapped = items
      .mapPartitions(iterator => {
        val item = iterator.toArray
        item.map(data => {
          val key = data._1
          val stat = (
            (data._2._1 - data._2._2) * (data._2._3 - data._2._4), // rate1-avg1 * rate2-avg2
            math.pow((data._2._1 - data._2._2), 2),
            math.pow((data._2._3 - data._2._4), 2)
          )
          (key, stat)
        }).iterator
      }).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2, x._3 + y._3))

    val pearson = mapped.map(f => {
      val key = f._1._1
      val comp = f._1._2
      val (numerator, denominator1, denominator2) = f._2
      val pearsonSim = pearsonCorrelation(numerator, scala.math.sqrt(denominator1), scala.math.sqrt(denominator2))
      (key, comp, pearsonSim)
    }).filter(_._3 <0.71).filter(_._3 > 0.38)

    lb += pearson.collect.toList
  }

  val l_rdd = spark.SparkEnvironment.sc.parallelize(lb.reduce(List.concat(_,_)))

  val predictions = test_data.join(avg_test)
    .map(x => (x._1, x._2._1, x._2._2)).keyBy(_._2)
    .join(l_rdd.keyBy(_._1))
    .map(x => (x._2._1._1, x._2._1._2, x._2._1._3, x._2._2._2, x._2._2._3))
    .keyBy(x => (x._1, x._4)).join(train_data.keyBy(x => (x._1, x._2)))
    .map(x => ((x._2._1._1, x._2._1._2, x._2._1._3), (x._2._1._4, x._2._1._5, x._2._2._3)))
    .groupByKey().map(data => {
    val key = (data._1._1, data._1._2)
    val m = data._1._3
    val weights = data._2.toSeq.sortWith(_._2 > _._2).take(100)
    val num = weights.map(x => (x._2 * x._3)).sum
    val den = weights.map(x => scala.math.abs(x._2)).sum
    val pred = if(den > 0) num/den else m
    (key, pred)
  })

  def pearsonCorrelation(dotProduct : Double, ratingNorm : Double, rating2Norm : Double) : Double= {
    if(ratingNorm * rating2Norm == 0) return  0
    else return dotProduct / (ratingNorm * rating2Norm)
  }


}
