import WriteSaveUtilities.{getDataFrameFromParquet, getDataFromJSON, getValueFromJSON, saveDataFrameAsParquet}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, sum}

object CorporateInfoBuilder {

  val logger: Logger = LogManager.getLogger("userLogger")

  val spark: SparkSession = SparkSession.builder()
    .appName("CorporateInfoBuilder")
    .master("yarn")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val settings = getDataFromJSON(args(0))

    val inPath  = getValueFromJSON(settings, "working_paths.parquet_out_path") // для таблиц clientDF
    val outPath = getValueFromJSON(settings, "working_paths.corporateInfo_path") // сюда будет записываться сформированная витрина

    val clientDF = getDataFrameFromParquet(spark, inPath + "Client")

    val corporateAccountDF = getDataFrameFromParquet(
      spark,
      getValueFromJSON(settings, "working_paths.corporateAccount_path")
    )

    val corporateInfoDF = {

      logger.info("Building CorporateInfo...")
      val startTime = System.nanoTime()

      val corporateInfoDF = clientDF
        .join(corporateAccountDF,
          clientDF.col("ClientId") === corporateAccountDF.col("ClientId")
        )
        .groupBy(
          clientDF.col("ClientId"), clientDF.col("ClientName"),
          clientDF.col("Type"), clientDF.col("Form"), clientDF.col("RegisterDate"),
          corporateAccountDF.col("CutoffDt")
        )
        .agg(
          sum(corporateAccountDF.col("TotalAmt")).as("TotalAmt")
        )
        .select(
          col("ClientId"),
          col("ClientName"),
          col("Type"),
          col("Form"),
          col("RegisterDate"),
          col("TotalAmt"),
          col("CutoffDt")
        )
        //.orderBy(col("ClientId").cast("int"))

      corporateInfoDF.show(false)
      logger.info("The CorporateInfo is built!!!")

      val endTime = System.nanoTime()
      val totalTime = {(endTime - startTime) / 1000 / 1000}
      logger.info(s"Time building is: $totalTime ms")

      corporateInfoDF
    }

    {
      logger.info("Writing to parquet...")
      val startTime = System.nanoTime()

      val numPartitions = 24
      val partitionKey = "CutoffDt"

      saveDataFrameAsParquet(
        corporateInfoDF,
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