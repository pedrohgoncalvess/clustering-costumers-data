package environment

import com.typesafe.config.{Config, ConfigFactory}

import java.nio.file.{Files, Paths}

object projectConfigs{

  private val config: Config = ConfigFactory.load()
  val treatedFilesPath: String = config.getString("TREATED_DIR")
  val rootPath: String = config.getString("DIR_DATASETS")

  def create_treated_datasets_dir: Unit = {
    val path = Paths.get(f"$treatedFilesPath")
    if (!Files.exists(path))
      Files.createDirectories(path)
  }
}
