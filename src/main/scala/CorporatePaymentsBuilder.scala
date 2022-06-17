import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, rank, regexp_replace, round, sum, trim}
import WriteSaveUtilities.{getDataFrameFromCSV, getDataFrameFromParquet, getDataFromJSON, getValueFromJSON, saveDataFrameAsParquet}

object CorporatePaymentsBuilder {

  val logger: Logger = LogManager.getLogger("userLogger")

  val spark: SparkSession = SparkSession.builder()
    .appName("CorporatePaymentsBuilder")
    .master("yarn")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val settings = getDataFromJSON(args(0))

    val inPath  = getValueFromJSON(settings, "working_paths.parquet_out_path") // для таблиц clientDF
    val outPath = getValueFromJSON(settings, "working_paths.corporatePayments_path") // сюда будет записываться сформированная витрина

    val configFile = getValueFromJSON(settings, "working_paths.tech_file") // "config/calculation_params_tech.csv" 4 пункт тз

    val clientDF    = getDataFrameFromParquet(spark, inPath + "Client")
    val accountDF   = getDataFrameFromParquet(spark, inPath + "Account")
    val operationDF = getDataFrameFromParquet(spark, inPath + "Operation")
    val rateDF      = getDataFrameFromParquet(spark, inPath + "Rate")
    val techDF      = getDataFrameFromCSV(spark, configFile) // дата фрейм технологической таблицы

    val rawList1 = techDF.filter(col("List_1").isNotNull).select("List_1").collect()
    val rawList2 = techDF.filter(col("List_2").isNotNull).select("List_2").collect()

    val list1 = for (elem <- rawList1) yield {elem.mkString.trim()}
    val list2 = for (elem <- rawList2) yield {elem.mkString.trim()}

    val list1Expr = list1.mkString("|") // word_1|word_2|...|word_n
    val list2Expr = list2.mkString("|")

    val corporatePayments = {

      val startTime = System.nanoTime()
      logger.info("Building CorporatePayments...")

      // измененная таблица Operation
      // - валюта переведена в национальную
      // - убрано поле currency
      val tempOperationDF = operationDF
        .withColumn("Amount", regexp_replace(operationDF
          .col("Amount"), ",", ".").cast("decimal(10,2)"))
        .join(rateDF, (operationDF.col("Currency") === rateDF.col("Currency")) and
          (operationDF.col("Currency") >= rateDF.col("rateDate")))
        .withColumn("rank",
          rank.over(Window.partitionBy(
            operationDF.col("Currency")
          ).orderBy(col("rateDate").desc))
        ).filter(col("rank") === 1)
        .withColumn("TodayCurrency", regexp_replace(rateDF
          .col("Rate"), ",", ".").cast("decimal(10,2)"))
        .select(
          col("AccountDB"), // счёт дебета
          col("AccountCR"), // счёт кредита
          col("DateOp"), // дата операции
          round(col("Amount") * col("TodayCurrency"), 2).as("Amount"), // сумма операции в нац. валюте
          col("Comment")
        ).cache()

      // здесь определяются все уникальные записи из таблицы Operation
      // зависит от даты
      val uniqueIdDF = operationDF
        .select(
          col("AccountDB").as("AccountId"),
          col("DateOp").as("CutoffDt"))
        .union(operationDF.select("AccountCR", "DateOp")).distinct()

      // kernel
      val idDF = uniqueIdDF
        .join(
          accountDF,
          uniqueIdDF.col("AccountId") === accountDF.col("AccountId"),
          "left"
        )
        .select(
          uniqueIdDF.col("AccountId"),
          accountDF.col("ClientId"),
          uniqueIdDF.col("CutoffDt")
        ).cache()

      // таблица операция, где счёт клиента указан в дебете
      // AccountId = accountDB
      // ClientId
      // CutoffDt
      // AccountDB
      // AccountCR
      // Amount
      // Comment
      val accountDB = idDF
        .join(
          tempOperationDF,
          idDF.col("AccountId") === tempOperationDF.col("AccountDB") and
            idDF.col("CutoffDt") === tempOperationDF.col("DateOp")
        ).drop(tempOperationDF.col("DateOp"))

      // таблица операция, где счёт клиента указан в кредите
      val accountCR = idDF
        .join(
          tempOperationDF,
          idDF.col("AccountId") === tempOperationDF.col("AccountCR") and
            idDF.col("CutoffDt") === tempOperationDF.col("DateOp")
        ).drop(tempOperationDF.col("DateOp"))

      // сумма операций по счету, где счет клиента указан в дебете проводки
      val PaymentAmt = accountDB
        .groupBy(
          col("AccountId").as("PaymentAmtAccountId"),
          col("CutoffDt").as("PaymentAmtCutoffDt")
        )
        .agg(
          sum(col("Amount")).as("PaymentAmt")
        )

      // сумма операций по счету, где счет клиента указан в  кредите проводки
      val EnrollmentAmt = accountCR
        .groupBy(
          col("AccountId").as("EnrollmentAmtAccountId"),
          col("CutoffDt").as("EnrollmentAmtCutoffDt")
        )
        .agg(
          sum(col("Amount")).as("EnrollmentAmt")
        )

      // для того чтобы не возникало проблем из-за naming при join (df.col("colName") не могло разрезолвиться)
      val tempAccountDF = accountDF.select(col("AccountId").as("AccountDFAccountId"),
        col("AccountNum"))

      // сумма операций, где счет клиента указан в дебете, и счет кредита 40702
      val TaxAmt = accountDB
        .join(
          tempAccountDF,
          accountDB.col("AccountCR") === col("AccountDFAccountId")
        )
        .where(
          col("AccountNum").substr(0, 5) === "40702"
        )
        .groupBy(
          accountDB.col("AccountId").as("TaxAmtAccountId"),
          accountDB.col("CutoffDt").as("TaxAmtCutoffDt")
        )
        .agg(
          sum(col("Amount")).as("TaxAmt")
        )

      // сумма операций, где счет клиента указан в кредите, и счет дебета 40802
      val ClearAmt = accountCR
        .join(
          tempAccountDF,
          accountDB.col("AccountDB") === col("AccountDFAccountId")
        )
        .where(
          col("AccountNum").substr(0, 5) === "40802"
        )
        .groupBy(
          accountDB.col("AccountId").as("ClearAmtAccountId"),
          accountDB.col("CutoffDt").as("ClearAmtCutoffDt")
        )
        .agg(
          sum(col("Amount")).as("ClearAmt")
        )

      // сумма операций, где все операции без ключевых слов списка list1
      // и счёт клиента указан в дебете
      val CarsAmt = tempOperationDF.except(
        tempOperationDF
          .filter(
            trim(col("Comment")).rlike(list1Expr)
          )
          .select(
            col("AccountDB"),
            col("AccountCR"),
            col("DateOp"),
            col("Amount"),
            col("Comment")
          )
          .distinct()
      )
        .groupBy(
          col("AccountDB").as("CarsAmtAccountId"),
          col("DateOp").as("CarsAmtCutoffDt")
        )
        .agg(
          sum(col("Amount")).as("CarsAmt")
        )

      // сумма операций, которые содержат ключевые слова списка list2
      // и счёт клиента указан в кредите
      val FoodAmt = tempOperationDF
        .filter(
          trim(col("Comment")).rlike(list2Expr)
        )
        .select(
          col("AccountDB"),
          col("AccountCR"),
          col("DateOp"),
          col("Amount")
        )
        .distinct()
        .groupBy(
          col("AccountCR").as("FoodAmtAccountId"),
          col("DateOp").as("FoodAmtCutoffDt")
        )
        .agg(
          sum(col("Amount")).as("FoodAmt")
        )

      // сумма операций с физ. лицами. Счет клиента указан в дебете проводки, а клиент в кредите проводки – ФЛ.
      val FLAmt = idDF
        .join(
          tempOperationDF,
          idDF.col("AccountId") === tempOperationDF.col("AccountDB") and
            idDF.col("CutoffDt") === tempOperationDF.col("DateOp"),
          "left"
        )
        .select(
          idDF.col("AccountId"),
          idDF.col("CutoffDt"),
          tempOperationDF.col("Amount"),
          tempOperationDF.col("AccountCR")
        )
        .join(
          accountDF,
          tempOperationDF.col("AccountCR") === accountDF.col("AccountID")
        )
        .select(
          idDF.col("AccountId"),
          idDF.col("CutoffDt"),
          tempOperationDF.col("Amount"),
          accountDF.col("ClientId")
        )
        .join(
          clientDF,
          accountDF.col("ClientId") === clientDF.col("ClientId") //and
          //  and clientDF.col("Type") === "Ф"
        )
        .filter(
          clientDF.col("Type") === "Ф"
        )
        .select(
          idDF.col("AccountId"),
          idDF.col("CutoffDt"),
          tempOperationDF.col("Amount")
        )
        .groupBy(
          idDF.col("AccountId").as("FLAmtAccountId"),
          idDF.col("CutoffDt").as("FLAmtCutoffDt")
        )
        .agg(
          sum(tempOperationDF.col("Amount")).as("FLAmt")
        )

      // составляем итоговую витрину
      val corporatePayments = idDF
        .join(
          PaymentAmt,
          idDF.col("AccountId") === PaymentAmt.col("PaymentAmtAccountId") and
            idDF.col("CutoffDt") === PaymentAmt.col("PaymentAmtCutoffDt"),
          "left"
        ).drop(col("PaymentAmtAccountId")).drop(col("PaymentAmtCutoffDt"))
        .join(
          EnrollmentAmt,
          idDF.col("AccountId") === EnrollmentAmt.col("EnrollmentAmtAccountId") and
            idDF.col("CutoffDt") === EnrollmentAmt.col("EnrollmentAmtCutoffDt"),
          "left"
        ).drop(col("EnrollmentAmtAccountId")).drop(col("EnrollmentAmtCutoffDt"))
        .join(
          TaxAmt,
          idDF.col("AccountId") === TaxAmt.col("TaxAmtAccountId") and
            idDF.col("CutoffDt") === TaxAmt.col("TaxAmtCutoffDt"),
          "left"
        ).drop(col("TaxAmtAccountId")).drop(col("TaxAmtCutoffDt"))
        .join(
          ClearAmt,
          idDF.col("AccountId") === ClearAmt.col("ClearAmtAccountId") and
            idDF.col("CutoffDt") === ClearAmt.col("ClearAmtCutoffDt"),
          "left"
        ).drop(col("ClearAmtAccountId")).drop(col("ClearAmtCutoffDt"))
        .join(
          CarsAmt,
          idDF.col("AccountId") === CarsAmt.col("CarsAmtAccountId") and
            idDF.col("CutoffDt") === CarsAmt.col("CarsAmtCutoffDt"),
          "left"
        ).drop(col("CarsAmtAccountId")).drop(col("CarsAmtCutoffDt"))
        .join(
          FoodAmt,
          idDF.col("AccountId") === FoodAmt.col("FoodAmtAccountId") and
            idDF.col("CutoffDt") === FoodAmt.col("FoodAmtCutoffDt"),
          "left"
        ).drop(col("FoodAmtAccountId")).drop(col("FoodAmtCutoffDt"))
        .join(
          FLAmt,
          idDF.col("AccountId") === FLAmt.col("FLAmtAccountId") and
            idDF.col("CutoffDt") === FLAmt.col("FLAmtCutoffDt"),
          "left"
        ).drop(col("FLAmtAccountId")).drop(col("FLAmtCutoffDt"))
        .select(
          col("AccountId"),
          col("ClientId"),
          col("PaymentAmt"),
          col("EnrollmentAmt"),
          col("TaxAmt"),
          col("ClearAmt"),
          col("CarsAmt"),
          col("FoodAmt"),
          col("FLAmt"),
          col("CutoffDt")
        )
        .na.fill(0)
        //.orderBy(col("AccountId").cast("int"))

      corporatePayments.show(false)
      logger.info("The CorporatePayments is built!!!")

      val endTime = System.nanoTime()
      val totalTime = {(endTime - startTime) / 1000 / 1000}
      logger.info(s"Time building is: $totalTime ms")

      corporatePayments // return
    }

    {
      val startTime = System.nanoTime()

      logger.info("Writing to parquet...")

      val numPartitions = 24
      val partitionKey = "CutoffDt"

      saveDataFrameAsParquet(
        corporatePayments,
        numPartitions,
        partitionKey,
        outPath
      )

      val endTime = System.nanoTime()
      val totalTime = {(endTime - startTime) / 1000 / 1000}
      logger.info(s"Time writing is: $totalTime ms")
    }
  }
}
