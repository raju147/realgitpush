package newpackage

import org.apache.spark.sql.SparkSession

object priorityone {

  val spark:SparkSession=sparkcommon.createSparkSession().get

  def sess(spark:SparkSession):Unit={
    import spark.implicits._
    val input = Seq(
      (1, "MV1"),
      (1, "MV2"),
      (2, "VPV"),
      (2, "Others")).toDF("id", "value")

    input.show()

    val result=prioritylogic.logic(input)(spark)

    result.show(false)
  }

  def main(args:Array[String]):Unit={
    sess(spark)
  }

}
