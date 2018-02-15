package inputdata

import spark.SparkEnvironment

import org.apache.spark.rdd.RDD

class MovieLensDataHolder(dataDirectoryPath: String) extends Serializable {

  val ratings = loadRatingsFromADirectory()
  val test_data = loadTestsFromADirectory()
  val train_data = loadTrains()
  val avg_ratings = averageTrainsRatings()

  protected def loadRatingsFromADirectory() : RDD[((Int, Int), Double)] = {
    val ratings = SparkEnvironment.sc
      .textFile(dataDirectoryPath + "/ratings.csv")
      .mapPartitionsWithIndex(
        (idx, iter) => if (idx == 0) iter.drop(1) else iter
      ).map { line =>
      val fields = line.split(',')
      // format: ((userID, movieID), rating)
      ((fields(0).toInt, fields(1).toInt), fields(2).toDouble)
    }
    ratings
  }

  protected def loadTestsFromADirectory() : RDD[(Int, Int)] = {
    val test_data = SparkEnvironment.sc
      .textFile(dataDirectoryPath + "/testing_small.csv")
      .mapPartitionsWithIndex(
        (idx, iter) => if (idx == 0) iter.drop(1) else iter
      ).map { line =>
      val fields = line.split(',')
      // format: (userID, movieID)
      (fields(0).toInt, fields(1).toInt)
    }
    test_data
  }

  protected def loadTrains() : RDD [(Int, Int, Double)] = {
    val ratings_map = ratings.collectAsMap()
    val trainBroadcast = SparkEnvironment.sc.broadcast(ratings_map)
    val ground = test_data.mapPartitions{arr =>
      val m = trainBroadcast.value
      for{
        (key1, key2) <- arr
        if(m.contains(key1, key2))
      } yield ((key1, key2), m.get(key1, key2).getOrElse(Double).asInstanceOf[Double])
    }

    val train_data = ratings.subtractByKey(ground)
      .map (_ match {
        // format: (userID, movieID, rating)
        case ((userId, movieId), rating) => (userId.toInt, movieId.toInt, rating.toDouble)
      })
    train_data
  }

  protected def averageTrainsRatings() : RDD[(Int, Double)] = {
    val avg = train_data.map(x => (x._2, x._3))
      .groupByKey().map { data =>
      val movieID = data._1
      val rating = data._2
      val avg = rating.sum / rating.size
      (movieID, avg)
    }
    avg
  }

}
