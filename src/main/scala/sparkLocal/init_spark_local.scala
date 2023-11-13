package datasets

import org.apache.spark.sql.SparkSession

object initSparkSession {

  def initSpark: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("SparkLocal")
      .getOrCreate()
  }
}
