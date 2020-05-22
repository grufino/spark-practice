import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object ProductsMostLessSold {
//  Who are the second most selling and the least selling persons (sellers)
  //  for each product? Who are those for product with `product_id = 0

  def main(args: Array[String]): Unit = {
    val session =
      SparkSession
        .builder()
        .config(CommonSparkConfig.get())
        .config("spark.executor.memory", "3gb")
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .getOrCreate()

    val salesDf =
      session.read
        .parquet("./datafiles/sales_parquet")

    val groupedDf =
      salesDf
        .groupBy("seller_id", "product_id")
        .agg(sum("num_pieces_sold").alias("num_pieces_sold"))

    val window_desc =
      Window.partitionBy(col("product_id")).orderBy(desc("num_pieces_sold"))
    val window_asc =
      Window.partitionBy(col("product_id")).orderBy(asc("num_pieces_sold"))

    val salesWithRank = salesDf
      .withColumn("rank_asc", dense_rank().over(window_asc))
      .withColumn("rank_desc", dense_rank().over(window_desc))

//    # Get products that only have one row OR the products in which multiple sellers sold the same amount
//    # (i.e. all the employees that ever sold the product, sold the same exact amount)
    val singleSeller = salesWithRank
      .where("rank_asc == rank_desc")
      .select(
        col("product_id").alias("single_seller_product_id"),
        col("seller_id").alias("single_seller_seller_id"),
        lit("Only seller or multiple sellers with the same results")
          .alias("type")
      )

    val secondSeller = salesWithRank.where("rank_desc == 2").select(
      col("product_id").alias("second_seller_product_id"), col("seller_id").alias("second_seller_seller_id"),
      lit("Second top seller").alias("type")
    )

    val leastSeller = salesWithRank.where("rank_asc == 1").select(
      col("product_id"), col("seller_id"),
      lit("Least Seller").alias("type")
    ).join(singleSeller, salesWithRank("seller_id") === singleSeller("single_seller_seller_id") && (
      salesWithRank("product_id") == singleSeller("single_seller_product_id")), "leftanti")
    .join(secondSeller, salesWithRank("seller_id") === secondSeller("second_seller_seller_id") && (
      salesWithRank("product_id") == secondSeller("second_seller_product_id")), "leftanti")

    val unionTable = leastSeller.select(
      col("product_id"),
      col("seller_id"),
      col("type")
    ).union(secondSeller.select(
      col("second_seller_product_id").alias("product_id"),
      col("second_seller_seller_id").alias("seller_id"),
      col("type")
    )).union(singleSeller.select(
      col("single_seller_product_id").alias("product_id"),
      col("single_seller_seller_id").alias("seller_id"),
      col("type")
    ))


//    # Which are the second top seller and least seller of product 0?
//      unionTable.where("product_id == 0").show()
  }

}
