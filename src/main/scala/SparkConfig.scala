import org.apache.spark.SparkConf

object CommonSparkConfig {
  val sparkConfig = new SparkConf

  def get() = {
    sparkConfig
      .set("spark.eventLog.enabled", "true")
      .set("spark.eventLog.dir", "file:///Users/grufino/spark-logs")
      .set("spark.history.fs.logDirectory", "file:///Users/grufino/spark-logs")
      .setAppName("SparkPractice")
      .setMaster("local")
  }
}
