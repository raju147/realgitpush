package newpackage

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object prioritylogic {

  def logic(df: DataFrame)(implicit spark:SparkSession): DataFrame = {
    import spark.implicits._

    val windowSpec = Window.partitionBy("id").orderBy("id")
    val withRowNumber = df.withColumn("row_number", row_number().over(windowSpec))
    val output = withRowNumber.filter($"row_number" === 1).select("id","value").withColumnRenamed("value","name")
    output
  }
}
