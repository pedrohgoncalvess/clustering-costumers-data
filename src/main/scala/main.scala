package datasets

import datasets.initSparkSession.write_parquet_files
import datasets.projectConfigs.estabelecSubDir
import estabelecimentos_frame.read_csv_estabelec_files

object main extends App{

  val dataframeEstabelec = read_csv_estabelec_files
  write_parquet_files(dataframe=dataframeEstabelec, columnToPartition="uf", subDir=estabelecSubDir)

}
