package newpackage

import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.functions.{col, udf}

object delimeter {

  val splitByDelimiter = udf((str: String, delimiter: String) => {
    // Ensure delimiter is treated correctly even if part of the value string
    if (delimiter.isEmpty) Array(str)
    else str.split(java.util.regex.Pattern.quote(delimiter),-1)
  })

  def delimextractor(df: DataFrame): DataFrame = {
    df.withColumn("split_values", splitByDelimiter(col("VALUES"), col("Delimiter")))
  }
}
