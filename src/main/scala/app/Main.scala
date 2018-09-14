package app

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    val posSchema = StructType(
      StructField("positionId", StringType) :: StructField("accountKey", StringType) :: StructField("positionValue", DoubleType) :: StructField(
        "positionDate",
        StringType
      ) :: Nil
    )
    val tranSchema = StructType(
      StructField("transactionId", StringType) :: StructField("accountKey", StringType) :: StructField(
        "transactionValue",
        DoubleType
      ) :: StructField("transactionDate", StringType) :: Nil
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

    val window = Window.partitionBy("accountKey").orderBy("positionDate")

    import org.apache.spark.sql.functions._

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
    resultDf
      .select("tran.accountKey", "previous_position", "aggTransactions", "positionValue", "ReconPassed")
      .show()

  }
}

case class Position(positionId: String, accountKey: String, positionValue: Double, positionDate: String)

case class Transaction(transactionId: String, accountKey: String, transactionValue: Double, transactionDate: String)
