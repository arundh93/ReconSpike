package app

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    val posSchema = StructType(
      Array(
        StructField("positionId", StringType),
        StructField("accountKey", StringType),
        StructField("positionValue", DoubleType),
        StructField("positionDate", StringType)
      )
    )

    val tranSchema = StructType(
      Array(
        StructField("transactionId", StringType),
        StructField("accountKey", StringType),
        StructField("transactionValue", DoubleType),
        StructField("transactionDate", StringType)
      )
    )

    val dateSchema = StructType(
      Array(
        StructField("busdate", StringType)
      )
    )

    val posDf: Dataset[Position] = spark.read
      .option("header", "true")
      .schema(posSchema)
      .csv("src/main/scala/data/sample/sept01/position.csv", "src/main/scala/data/sample/sept02/position.csv")
      .as[Position]
      .alias("pos")

    val tranDf = spark.read
      .option("header", "true")
      .schema(tranSchema)
      .csv("src/main/scala/data/sample/sept01/transaction.csv", "src/main/scala/data/sample/sept02/transaction.csv")
      .as[Transaction]

    val busDate: Array[String] = spark.read
      .option("header", "true")
      .schema(dateSchema)
      .csv("src/main/scala/data/sample/date.csv")
      .as[String]
      .collect()

    val busDateBroadcasted: Broadcast[Array[String]] = spark.sparkContext.broadcast(busDate)

    import org.apache.spark.sql.functions._
    val window = Window.partitionBy("accountKey").orderBy("positionDate")

    val positionDiffDf = posDf
      .withColumn("previous_position", lag("positionValue", 1, 0).over(window))
      .withColumn("diff", col("positionValue") - col("previous_position"))

    val aggTransactions: DataFrame = tranDf
      .groupBy("accountKey", "transactionDate")
      .sum("transactionValue")
      .withColumnRenamed("sum(transactionValue)", "aggTransactions")
      .alias("tran")

    val resultDf = aggTransactions
      .join(positionDiffDf, aggTransactions.col("tran.accountKey") === posDf.col("pos.accountKey"))
      .filter($"positionDate" === "02/09/2018")
      .withColumn("ReconPassed", $"aggTransactions" <=> $"diff")
    posDf.show()
    tranDf.show()
    positionDiffDf.show()
    aggTransactions.show()
    val hbaseResult = resultDf
      .select("tran.accountKey", "positionValue", "ReconPassed")

    hbaseResult.show()
    hbaseResult.write
      .options(Map(HBaseTableCatalog.tableCatalog -> reconCatalog, HBaseTableCatalog.newTable -> "5"))
      .format("org.apache.spark.sql.execution.datasources.hbase")
      .save()

  }

  def reconCatalog: String =
    s"""{
       |"table":{"namespace":"default", "name":"ReconResult"},
       |"rowkey":"accountKey",
       |"columns":{
       |"accountKey":{"cf":"rowkey", "col":"accountKey", "type":"string"},
       |"positionValue":{"cf":"rkey", "col":"positionValue", "type":"double"},
       |"ReconPassed":{"cf":"r1key", "col":"ReconPassed", "type":"boolean"}
       |}
       |}""".stripMargin
}

case class Position(positionId: String, accountKey: String, positionValue: Double, positionDate: String)

case class Transaction(transactionId: String, accountKey: String, transactionValue: Double, transactionDate: String)

case class BusDate(busdate: String)
