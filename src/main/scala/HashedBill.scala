//Create a new column called "hashed_bill" defined as follows:
//  - if the order_id is even: apply MD5 hashing iteratively to the bill_raw_text field, once for each 'A' (capital 'A') present in the text. E.g. if the bill text is 'nbAAnllA', you would apply hashing three times iteratively (only if the order number is even)
//- if the order_id is odd: apply SHA256 hashing to the bill text
//Finally, check if there are any duplicate on the new column
import java.nio.charset.Charset
import java.security.MessageDigest
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object HashedBill {
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

    val algoUdf =
      session.udf
        .register("algo", (orderId: String, billText: String) => {
          var ret = billText
          val orderIdInt = orderId.toInt
          if (orderIdInt % 2 == 0) {
            if (orderIdInt % 2 == 0) {
              val cntA: Int = billText
                .count(_ == 'A')

              for (c <- 0 to cntA) {
                val md = MessageDigest.getInstance("MD5")
                val digest =
                  md.digest(billText.getBytes(Charset.forName("UTF-8")))

                ret = new String(digest, "UTF-8")
              }
            }
          } else {
            val md = MessageDigest.getInstance("SHA-256")
            val digest = md.digest(billText.getBytes())
            ret = new String(digest)
          }
          ret
        })

    val salesDfWithHashedBill = salesDf
      .withColumn(
        "hashed_bill",
        algoUdf(salesDf("order_id"), salesDf("bill_raw_text"))
      )
    salesDfWithHashedBill
      .groupBy(salesDfWithHashedBill("hashed_bill"))
      .agg(count("*").alias("cnt"))
      .where(col("cnt") > 1)
      .show()
  }
}
