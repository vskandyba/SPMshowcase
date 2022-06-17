import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import ujson.Value

import java.io.{BufferedReader, InputStreamReader}
import java.util.Properties
// import scala.io.Source

object WriteSaveUtilities {

  def getDataFrameFromParquet(spark: SparkSession, path: String): DataFrame =
    spark.read.parquet(path)

  def getDataFrameFromCSV(spark: SparkSession, path: String): DataFrame =
    spark.read.option("delimiter", ",")
      .option("header", "true")
      .csv(path)

  def getDataFrameFromSQL(spark: SparkSession, path: String): DataFrame = {
    //spark.sql(Source.fromFile(path).mkString)
    //spark.sql(getTextFromFile(spark, path))
      spark.sql(getTextFromFile(path))
  }

  def getValueFromJSON(df: DataFrame, key: String): String = {
    df.select(key).collectAsList().get(0).mkString
  }

  def getValueFromJSON(dataJSON: Value, key: String): String = {
    val subKeys = key.split('.') // key.sub_key.sub_sub_key

    subKeys.foldLeft(dataJSON)((data, k) => data(k)).value.toString
  }

  def getDataFromJSON(path: String): Value = {
    ujson.read(getTextFromFile(path))
  }

  def getTextFromFile(spark: SparkSession, path: String): String = {
    spark.read
      .option("wholetext", "true")
      .text(path)
      .collectAsList()
      .get(0)
      .mkString
  }

  def getTextFromFile(filePath: String): String = {

    val path: Path = new Path(filePath)
    val fs: FileSystem = path.getFileSystem(new Configuration())
    val br: BufferedReader = new BufferedReader(new InputStreamReader(fs.open(path)))

    val stringBuilder: StringBuilder = new StringBuilder

    var temp: String = br.readLine()
    while (temp != null){

      stringBuilder ++= temp
      stringBuilder += '\n'
      temp = br.readLine()
    }
    br.close()

    stringBuilder.mkString
  }

  def saveDataFrameAsParquet(df: DataFrame, numPartitions: Int, partitionKey: String, path: String): Unit ={
    df.coalesce(numPartitions)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .partitionBy(partitionKey)
      .parquet(path)
  }

  def saveDataFrameAsParquet(df: DataFrame, numPartitions: Int, path: String): Unit ={
    df.repartition(numPartitions)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .parquet(path)
  }

  def saveDataFrameAsCSV(df: DataFrame, numPartitions: Int, path: String): Unit ={ // для отладки
    df.repartition(numPartitions)
      .write
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .csv(path)
  }

  def saveDataFrameToDB(df: DataFrame, driver: String, url: String, tableName: String, properties: Properties): Unit = {
    df
      .write
      .option("driver", driver)
      .mode(SaveMode.Overwrite)
      .jdbc(url, tableName, properties)
  }
}
