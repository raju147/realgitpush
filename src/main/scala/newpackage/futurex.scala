
import newpackage.sparkcommon

import java.util.Properties
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object FutureXSparkTransformer {
  private val logger=LoggerFactory.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    try {
      logger.info("main method started")
     logger.warn("This is a warning")

     val spark:SparkSession=sparkcommon.createSparkSession().get

      //create course hive table
      sparkcommon.createFutureCourseHiveTable(spark)
     //df.write.format("csv").save("samplesq")

     //Create a DataFrame from Postgres Course Catalog table
     //println("Creating Dataframe from Postgres")

     //val pgTable = "futureschema.futurex_table_catalog"
     //server:port/database_name
     //val pgCourseDataframe=PostgresCommon.fetchDataFrameFromPgTable(spark,pgTable).get
     //logger.info("main method completed")
     //pgCourseDataframe.show()
     //println("Shown")
    }catch
      {
        case e:Exception=>
          logger.error("An error has occured in the main method"+e.printStackTrace())
      }


  }
}
