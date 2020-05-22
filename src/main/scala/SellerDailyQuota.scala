import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
//For each seller, what is the average % contribution of an order to the seller's daily quota?
//# Example
//If Seller_0 with `quota=250` has 3 orders:
//  Order 1: 10 products sold
//Order 2: 8 products sold
//Order 3: 7 products sold
//The average % contribution of orders to the seller's quota would be:
//  Order 1: 10/105 = 0.04
//Order 2: 8/105 = 0.032
//Order 3: 7/105 = 0.028
//Average % Contribution = (0.04+0.032+0.028)/3 = 0.03333

object SellerDailyQuota {
  def main(args: Array[String]): Unit = {
    val session =
      SparkSession
        .builder()
        .config(CommonSparkConfig.get())
        .config("spark.sql.autoBroadcastJoinThreshold", -1)
        .getOrCreate()

    val sales =
      session
        .read
        .parquet("./datafiles/sales_parquet")


    val sellers =
      session
        .read
        .parquet("./datafiles/sellers_parquet")

    val result =
      sales.select(
        sales.col("seller_id"),
        sales.col("num_pieces_sold").cast("integer"),
        )
        .join(sellers, "seller_id")
        .groupBy("seller_id", "daily_target")
        .avg("num_pieces_sold")
        .withColumn("avg_quota_contrib", col("avg(num_pieces_sold)") / col("daily_target"))

    println(result.show())
  }
}
