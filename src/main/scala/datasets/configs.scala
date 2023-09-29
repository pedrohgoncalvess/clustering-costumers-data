package datasets

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql

import java.nio.file.{Files, Paths}

object projectConfigs{

  private val config: Config = ConfigFactory.load()
  val estabelecSubDir: String = config.getString("ESTABELEC_SUB_DIR")
  val sociosSubDir: String = config.getString("SOCIOS_SUB_DIR")
  val empresasSubDir: String = config.getString("EMPRESAS_SUB_DIR")
  val treatedFilesDir: String = config.getString("TREATED_DIR")
  val rootPath: String = config.getString("DIR_DATASETS")

  def create_treated_datasets_dir(subDir: String): Unit = {
    val path = Paths.get(f"$rootPath\\$subDir")
    if (!Files.exists(path))
      Files.createDirectories(path)
  }

  def write_parquet_files(dataframe: sql.DataFrame, columnToPartition: String = "None", subDir: String): Unit = {
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
