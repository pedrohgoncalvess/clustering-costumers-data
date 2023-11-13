package datasets

import environment.projectConfigs.{create_treated_datasets_dir, rootPath, treatedFilesPath}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql

object initSparkSession {

  def initSpark: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("SparkLocal")
      .getOrCreate()
  }

  def write_parquet_files(dataframe: sql.DataFrame, columnToPartition: String = "None", subDir: String): Unit = {
    create_treated_datasets_dir
    val treatedDir = s"$rootPath\\$treatedFilesPath\\$subDir"
    if (columnToPartition == "None") {
      dataframe.write.mode("overwrite").parquet(treatedDir)
    }
    else {
      dataframe.write.mode("overwrite").partitionBy(columnToPartition).parquet(treatedDir)
    }
  }
}
