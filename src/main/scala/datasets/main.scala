package treatment

import treatment.projectConfigs.{estabelecColumns, estabelecSubDir}
import treatment.manageFiles.{read_csv_files, write_parquet_files}

object main extends App{

  val dataframeEstabelec = read_csv_files(estabelecColumns, estabelecSubDir)
  write_parquet_files(dataframe=dataframeEstabelec, columnToPartition="uf", subDir=estabelecSubDir)

}
