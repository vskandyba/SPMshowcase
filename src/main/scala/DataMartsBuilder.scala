import WriteSaveUtilities.{getDataFrameFromCSV, getDataFrameFromParquet, getDataFrameFromSQL, getDataFromJSON, getValueFromJSON, saveDataFrameAsParquet}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

object DataMartsBuilder {

  val logger: Logger = LogManager.getLogger("userLogger")

  val spark: SparkSession = SparkSession.builder()
    .appName("DataMartsBuilder")
    .master("yarn")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val settings = getDataFromJSON(args(0))

    val inPath     = getValueFromJSON(settings, "working_paths.parquet_out_path") // данные для витрин берём из созданных паркетов
    val outPath    = getValueFromJSON(settings, "working_paths.dataMarts_out_path") // сюда будут записываться сформированные витрины
    val configFile = getValueFromJSON(settings, "working_paths.tech_file") // 4 пункт тз
    val sqlPath    = getValueFromJSON(settings, "working_paths.sql_path") // здесь лежат sql-запросы для формирования витрин

    val clientDF    = getDataFrameFromParquet(spark, inPath + "Client")
    val accountDF   = getDataFrameFromParquet(spark, inPath + "Account")
    val operationDF = getDataFrameFromParquet(spark, inPath + "Operation")
    val rateDF      = getDataFrameFromParquet(spark, inPath + "Rate")
    val techDF      = getDataFrameFromCSV(spark, configFile) // дата фрейм технологической таблицы

    clientDF.createOrReplaceTempView("clients")
    accountDF.createOrReplaceTempView("accounts")
    operationDF.createOrReplaceTempView("operations")
    rateDF.createOrReplaceTempView("rates")
    techDF.createOrReplaceTempView("tech_params") // темп вью для технологической таблицы

    val corporatePayments = {
      logger.info("Building CorporatePayments...")
      val startTime = System.nanoTime()

      val corporatePaymentsDF = getDataFrameFromSQL(spark, sqlPath + "CreateCorporatePaymentsV2.sql")
      corporatePaymentsDF.show(false)
      logger.info("The CorporatePayments is built!!!")

      val endTime = System.nanoTime()
      val totalTime = {(endTime - startTime) / 1000 / 1000}
      logger.info(s"Time building is: $totalTime ms")

      corporatePaymentsDF
    }
    corporatePayments.createOrReplaceTempView("corporate_payments")

    val corporateAccount = {
      logger.info("Building CorporateAccount...")
      val startTime = System.nanoTime()

      val corporateAccountDF = getDataFrameFromSQL(spark, sqlPath + "CreateCorporateAccount.sql")
      corporateAccountDF.show(false)

      logger.info("The CorporateAccount is built!!!")
      val endTime = System.nanoTime()
      val totalTime = {(endTime - startTime) / 1000 / 1000}
      logger.info(s"Time building is: $totalTime ms")

      corporateAccountDF
    }
    corporateAccount.createOrReplaceTempView("corporate_account")

    val corporateInfo = {
      logger.info("Building CorporateInfo...")
      val startTime = System.nanoTime()

      val corporateInfoDF = getDataFrameFromSQL(spark, sqlPath + "CreateCorporateInfo.sql")
      corporateInfoDF.show(false)

      logger.info("The CorporateInfo is built!!!")
      val endTime = System.nanoTime()
      val totalTime = {(endTime - startTime) / 1000 / 1000}
      logger.info(s"Time building is: $totalTime ms")
      corporateInfoDF
    }

    {
      logger.info("Saving to parquet...")
      val startTime = System.nanoTime()

      val numPartitions = 16
      val partitionKey = "CutoffDt"

      saveDataFrameAsParquet(corporatePayments,
        numPartitions,
        partitionKey,
        outPath + "corporate_payments"
      )

      saveDataFrameAsParquet(corporateAccount,
        numPartitions,
        partitionKey,
        outPath + "corporate_account"
      )

      saveDataFrameAsParquet(corporateInfo,
        numPartitions,
        partitionKey,
        outPath + "corporate_info"
      )

      val endTime = System.nanoTime()
      val totalTime = {(endTime - startTime) / 1000 / 1000}
      logger.info(s"Time writing to parquet is: $totalTime ms")
    }
  }
}
