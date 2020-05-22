import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{
  IntegerType,
  StringType,
  StructField,
  StructType
}

import scala.collection.mutable.ListBuffer
//What is the average revenue of the orders?
object AverageRevenue {
  def main(args: Array[String]): Unit = {

    val session =
      SparkSession
        .builder()
        .config(CommonSparkConfig.get())
        .config("spark.executor.memory", "500mb")
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .getOrCreate()

    var salesDf =
      session.read
        .parquet("./datafiles/sales_parquet")

    var productsDf =
      session.read
        .parquet("./datafiles/products_parquet")

    val LRU = salesDf
      .groupBy(salesDf("product_id"))
      .count()
      .orderBy(desc("count"))
      .limit(100)
      .collect()

    val REPLICATION_FACTOR = 101
    var l = ListBuffer[(String, Int)]()
    var replicatedProducts = ListBuffer[String]()

    LRU.foreach(sale => {
      val productId = sale.getString(0)
      replicatedProducts += productId
      for (i <- 0 to REPLICATION_FACTOR) {
        val tuple = (productId, i)
        l += tuple
      }
    })

    val rdd = session.sparkContext.parallelize(l).map(x => { Row(x._1, x._2) })

    val replicatedProductsDf = session.createDataFrame(
      rdd,
      StructType(
        Array(
          StructField("product_id", StringType, true),
          StructField("replication", IntegerType, true)
        )
      )
    )

    productsDf = productsDf
      .join(
        broadcast(replicatedProductsDf),
        productsDf("product_id") === replicatedProductsDf("product_id"),
        "left"
      )
      .withColumn(
        "saltedJoinKey",
        when(
          replicatedProductsDf("replication").isNull,
          productsDf("product_id")
        ).otherwise(
          concat(
            replicatedProductsDf("product_id"),
            lit("-"),
            replicatedProductsDf("replication")
          )
        )
      )

    salesDf = salesDf.withColumn(
      "saltedJoinKey",
      when(
        salesDf("product_id").isin(replicatedProducts: _*),
        concat(
          salesDf("product_id"),
          lit("-"),
          round(rand() * (REPLICATION_FACTOR - 1), 0).cast(IntegerType)
        )
      ).otherwise(salesDf("product_id"))
    )

    salesDf
      .join(broadcast(productsDf), salesDf("saltedJoinKey") === productsDf("saltedJoinKey"), "inner")
      .agg(avg(productsDf("price") * salesDf("num_pieces_sold")))
      .show(100)

  }

}
