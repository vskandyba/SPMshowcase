import WriteSaveUtilities.{getDataFrameFromParquet, getDataFromJSON, getValueFromJSON, saveDataFrameAsParquet}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

object CorporateAccountBuilder {

  val logger: Logger = LogManager.getLogger("userLogger")

  val spark: SparkSession = SparkSession.builder()
    .appName("CorporateAccountBuilder")
    .master("yarn")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val settings = getDataFromJSON(args(0))

    val inPath  = getValueFromJSON(settings, "working_paths.parquet_out_path") // для таблиц clientDF, accountDF
    val outPath = getValueFromJSON(settings, "working_paths.corporateAccount_path") // сюда будет записываться сформированная витрина

    val clientDF  = getDataFrameFromParquet(spark, inPath + "Client")
    val accountDF = getDataFrameFromParquet(spark, inPath + "Account")

    //
    val corporatePaymentsDF = getDataFrameFromParquet(
      spark,
      getValueFromJSON(settings, "working_paths.corporatePayments_path")
    )

    val corporateAccountDF = {
      logger.info("Building CorporateAccount...")
      val startTime = System.nanoTime()

      val corporateAccountDF = corporatePaymentsDF
        .join(
          accountDF,
          corporatePaymentsDF.col("AccountId") === accountDF.col("AccountId"),
          "left"
        )
        .select(
          corporatePaymentsDF.col("AccountId"),
          accountDF.col("AccountNum"),
          accountDF.col("DateOpen"),
          corporatePaymentsDF.col("ClientId"),
          corporatePaymentsDF.col("PaymentAmt"),
          corporatePaymentsDF.col("EnrollmentAmt"),
          corporatePaymentsDF.col("CutoffDt")
        )
        .join(
          clientDF,
          clientDF.col("ClientId") === corporatePaymentsDF.col("ClientId"),
          "left"
        )
        .select(
          corporatePaymentsDF.col("AccountId"),
          accountDF.col("AccountNum"),
          accountDF.col("DateOpen"),
          corporatePaymentsDF.col("ClientId"),
          clientDF.col("ClientName"),
          (corporatePaymentsDF.col("PaymentAmt") + corporatePaymentsDF.col("EnrollmentAmt")).as("TotalAmt"),
          corporatePaymentsDF.col("CutoffDt")
        )
        //.orderBy(col("AccountId").cast("int"))

      corporateAccountDF.show(false)
      logger.info("The CorporateAccount is built!!!")
      val endTime = System.nanoTime()
      val totalTime = {(endTime - startTime) / 1000 / 1000}

      logger.info(s"Time building is: $totalTime ms")

      corporateAccountDF
    }

    {
      logger.info("Writing to parquet...")
      val startTime = System.nanoTime()

      val numPartitions = 24
      val partitionKey = "CutoffDt"

      saveDataFrameAsParquet(
        corporateAccountDF,
        numPartitions,
        partitionKey,
        outPath
      )
      val endTime = System.nanoTime()
      val totalTime = {(endTime - startTime) / 1000 / 1000}

      logger.info(s"Time writing to parquet is: $totalTime ms")
    }
  }
}
