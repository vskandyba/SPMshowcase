import WriteSaveUtilities.{getDataFrameFromParquet, getDataFromJSON, getValueFromJSON, saveDataFrameToDB}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

import java.util.Properties

object DBSender {

  val logger: Logger = LogManager.getLogger("userLogger")

  val spark: SparkSession = SparkSession.builder()
    .appName("DBSender")
    .master("yarn")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val settings = getDataFromJSON(args(0))

    logger.info("Reading Data Marts from parquet")

    val corporatePaymentsDF = getDataFrameFromParquet(spark, getValueFromJSON(settings, "working_paths.corporatePayments_path"))
    val corporateAccountDF  = getDataFrameFromParquet(spark, getValueFromJSON(settings, "working_paths.corporateAccount_path"))
    val corporateInfoDF     = getDataFrameFromParquet(spark, getValueFromJSON(settings, "working_paths.corporateInfo_path"))

    val driver   = getValueFromJSON(settings, "db_connecting.driver")
    val url      = getValueFromJSON(settings, "db_connecting.url")
    val user     = getValueFromJSON(settings, "db_connecting.user")
    val password = getValueFromJSON(settings, "db_connecting.password")

    val properties = new Properties()
    properties.put("user", user)
    properties.put("password", password)

    logger.info("Sending Data Marts to DataBase")

    saveDataFrameToDB(
      corporatePaymentsDF,
      driver,
      url,
      tableName = "CorporatePayments",
      properties
    )

    saveDataFrameToDB(
      corporateAccountDF,
      driver,
      url,
      tableName = "CorporateAccount",
      properties
    )

    saveDataFrameToDB(
      corporateInfoDF,
      driver,
      url,
      tableName = "CorporateInfo",
      properties
    )
  }
}
