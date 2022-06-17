import WriteSaveUtilities.{getDataFromJSON, getValueFromJSON}
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object ParquetCreator {

  val logger: Logger = LogManager.getLogger("userLogger")

  val spark: SparkSession = SparkSession.builder()
    .appName("ParquetCreator")
    .master("yarn")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val settings = getDataFromJSON(args(0))

    logger.info("Reading csv files...")
    val inPath  = getValueFromJSON(settings, "working_paths.csv_in_path") // на вход csv
    val outPath = getValueFromJSON(settings, "working_paths.parquet_out_path") // на выход паркет в hdfs

    val partitionKeys = Map(
      "Clients.csv" 	  -> "RegisterDate",
      "Accounts.csv" 	  -> "DateOpen",
      "Operations.csv"  -> "DateOp",
      "Rates.csv" 		  -> "RateDate")

    logger.info("Writing csv as parquet...")

    {
      val startTime = System.nanoTime()

      for (
        fileName <- partitionKeys.keys
      ) {
        spark.read.option("delimiter", ",")
          .option("header", "true")
          .csv(inPath + fileName)
          .write.mode(SaveMode.Overwrite)
          .partitionBy(partitionKeys(fileName))
          .parquet(outPath + fileName.substring(0, fileName.length - 5)) // отсечь 's.csv'
      }

      val endTime = System.nanoTime()
      val totalTime = {(endTime - startTime) / 1000 / 1000}

      logger.info(s"Time writing to parquet is: $totalTime ms")
    }
  }
}
