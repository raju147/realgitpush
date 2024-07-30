package newpackage

import org.apache.spark.sql.SparkSession

object ascendas {
  val spark: SparkSession = sparkcommon.createSparkSession.get

  def fun(spark: SparkSession): Unit = {
    import spark.implicits._

    val dept = Seq(
      ("50000.0#0#0#", "#"),
      ("0@1000.0@", "@"),
      ("1$", "$"),
      ("1000.00^Test_string", "^")
    ).toDF("VALUES", "Delimiter")

    // Show original dataframe
    dept.show(false)

    // Use the delimeter object to extract values
    val result = delimeter.delimextractor(dept)

    // Show the transformed dataframe
    result.show(false)
  }

  def main(args: Array[String]): Unit = {
    fun(spark)
  }
}
