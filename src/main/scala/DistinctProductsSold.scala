import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
// How many distinct products have been sold in each day?
object DistinctProductsSold {
  def main(args: Array[String]): Unit = {
    val session =
      SparkSession
        .builder()
        .config(CommonSparkConfig.get())
        .config("spark.executor.memory", "2000mb")
        .getOrCreate()

    val countSales =
      session
        .read
        .parquet("./datafiles/sales_parquet")
        .groupBy("product_id")


    println(countSales.count().count())

    val orderedProductSales =
      countSales
          .count().orderBy(desc("count"))

    println(orderedProductSales.show())
  }
}
