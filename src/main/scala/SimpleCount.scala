import org.apache.spark.sql.SparkSession

object SimpleCount {
  def main(args: Array[String]): Unit = {
    val session =
      SparkSession
        .builder()
        .config(CommonSparkConfig.get())
        .config("spark.executor.memory", "500mb")
        .getOrCreate()

    val countProducts =
    session
      .read
      .parquet("./datafiles/products_parquet")
      .count()

    val countSales =
      session
        .read
        .parquet("./datafiles/sales_parquet")
        .count()

    val countSellers =
      session
        .read
        .parquet("./datafiles/sellers_parquet")
        .count()

    println("sales: " + countSales)
    println("sellers: " + countSellers)
    println("products: " + countProducts)

    session.close()
  }
}
