package datasets

import com.typesafe.config.{Config, ConfigFactory}

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
}
