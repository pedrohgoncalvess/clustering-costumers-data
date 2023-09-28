package treatment

import org.apache.spark.sql
import projectConfigs._
import org.apache.spark.sql.SparkSession

object initSparkSession {

  def initSpark: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("SparkLocal")
      .getOrCreate()
  }
}


object manageFiles {
  val ss = initSparkSession.initSpark

  def read_csv_files(columns:Seq[String], subDir:String, delimiter:String=";"): sql.DataFrame = {
    val dirPath: String = s"$rootPath\\$subDir\\"
    var dfCsv: sql.DataFrame = ss.read.option("delimiter", delimiter).csv(dirPath)

    for (idx <- Range(0, columns.length)) {
      dfCsv = dfCsv.withColumnRenamed(s"_c$idx", columns(idx))
    }
    dfCsv
  }

  def write_parquet_files(dataframe: sql.DataFrame, columnToPartition:String = "None", subDir: String): Unit = {
    create_treated_datasets_dir(s"$treatedFilesDir\\$subDir")
    val treatedDir = s"$rootPath\\$treatedFilesDir\\$subDir"
    if (columnToPartition == "None") {
      dataframe.write.mode("overwrite").parquet(treatedDir)
    }
    else {
      dataframe.write.mode("overwrite").partitionBy(columnToPartition).parquet(treatedDir)
    }
  }

}

