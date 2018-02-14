import org.apache.spark.{SparkConf, SparkContext}

object SparkEnvironment {
  val conf = new SparkConf()
    .setAppName("cs_test")
    .setMaster("local[*]")
  val sc = new SparkContext(conf)
}
