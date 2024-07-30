package newpackage

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object sparkcommon {
  private val logger = LoggerFactory.getLogger(getClass.getName)

  def createSparkSession(): Option[SparkSession] = {
    try {
      logger.info("createSparkSession() started")
      // Set Hadoop home directory for Windows
      System.setProperty("hadoop.home.dir", "C:\\hadoop")

      val spark = SparkSession
        .builder
        .appName("HelloSpark")
        .config("spark.master", "local")
        // Set custom warehouse directory
        .config("spark.sql.warehouse.dir", "file:///C:/hadoop/spark-warehouse")
        .enableHiveSupport()
        .getOrCreate()
      logger.info("Spark session is created")

      Some(spark)
    } catch {
      case e: Exception =>
        logger.error("SparkSession creation failed", e)
        None
    }
  }

  def createFutureCourseHiveTable(spark: SparkSession): Unit = {
    logger.warn("createFutureCourseHiveTable method started")
    try {
      spark.sql("CREATE DATABASE IF NOT EXISTS fxxcoursedb")
      spark.sql("""
        CREATE TABLE IF NOT EXISTS fxxcoursedb.fx_course_table (
          course_id STRING,
          course_name STRING,
          author_name STRING,
          no_of_reviews STRING
        )
      """)

      spark.sql("INSERT INTO fxxcoursedb.fx_course_table VALUES ('1', 'Java', 'FutureX', '45')")
      spark.sql("INSERT INTO fxxcoursedb.fx_course_table VALUES ('2', 'Python', 'FutureX', '100')")
      spark.sql("INSERT INTO fxxcoursedb.fx_course_table VALUES ('3', 'Scala', 'FutureX', '70')")
      spark.sql("INSERT INTO fxxcoursedb.fx_course_table VALUES ('4', 'Spark', 'FutureX', '50')")
      spark.sql("INSERT INTO fxxcoursedb.fx_course_table VALUES ('5', 'Big Data', 'FutureX', '30')")
      spark.sql("INSERT INTO fxxcoursedb.fx_course_table VALUES ('6', 'Machine Learning', 'FutureX', '90')")
      spark.sql("INSERT INTO fxxcoursedb.fx_course_table VALUES ('7', 'Deep Learning', 'FutureX', '80')")
      spark.sql("INSERT INTO fxxcoursedb.fx_course_table VALUES ('8', 'Data Science', 'FutureX', '110')")
      spark.sql("INSERT INTO fxxcoursedb.fx_course_table VALUES ('9', 'Artificial Intelligence', 'FutureX', '120')")
      spark.sql("INSERT INTO fxxcoursedb.fx_course_table VALUES ('10', 'Cloud Computing', 'FutureX', '60')")

      spark.sql("ALTER TABLE fxxcoursedb.fx_course_table SET TBLPROPERTIES ('serialization.null.format'='')")

      logger.info("Hive table and data insertion completed successfully")
    } catch {
      case e: Exception =>
        logger.error("Error creating Hive table or inserting data", e)
    }
  }
}
