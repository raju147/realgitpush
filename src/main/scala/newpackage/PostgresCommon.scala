package newpackage

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.Properties
import org.slf4j.LoggerFactory

object PostgresCommon {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def getPostgresCommonProps(): Properties = {
    logger.info("getPostgresServerDatabase")
    val pgConnectionProperties = new Properties()
    pgConnectionProperties.put("user", "postgres")
    pgConnectionProperties.put("password", "admin")
    logger.info("getPostgresCommonProps method completed")
    pgConnectionProperties
  }

  def getPostgressServerDatabase(): String = {
    val pgURL = "jdbc:postgresql://localhost:5432/futurex"
    pgURL
  }

  def fetchDataFrameFromPgTable(spark: SparkSession, pgTable: String): Option[DataFrame] = {
    try {
      logger.info("fetchDataFrameFromPgTable method started")
      val pgCourseDataframe = spark.read.jdbc(getPostgressServerDatabase(), pgTable, getPostgresCommonProps())
      logger.info("fetchDataFrameFromPgTable method ended")
      Some(pgCourseDataframe)
    } catch{
      case e:Exception=>
        logger.error("An error has occured in fetchDataFramePgTable"+e.printStackTrace())
        None
    }
  }
}
