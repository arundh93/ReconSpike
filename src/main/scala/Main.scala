import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[*]").getOrCreate()
    import spark.implicits._
    val posSchema = StructType(StructField("posId", StringType) :: StructField("accountKey", StringType) :: StructField("positionValue", DoubleType) :: StructField("date", StringType) :: Nil)
    val tranSchema = StructType(StructField("tranId", StringType) :: StructField("accountKey", StringType) :: StructField("transactionValue", DoubleType) :: StructField("date", StringType) :: Nil)

    val posDf: Dataset[Position] = spark.read.option("header", "true").schema(posSchema).csv("/Users/arundh/Documents/position.csv").as[Position]
    val tranDf = spark.read.option("header", "true").schema(tranSchema).csv("/Users/arundh/Documents/transaction.csv").as[Transaction]

    val aggTransactions: DataFrame = tranDf.groupBy("accountKey", "date").sum("transactionValue")

    posDf.printSchema()
    aggTransactions.show()
    //    posDf.show()
    //    tranDf.show()
  }
}

//recon: yday position + today trans = today position

case class Position(posId: String, accountKey: String, positionValue: Double, date: String)

case class Transaction(tranId: String, accountKey: String, transactionValue: Double, date: String)

