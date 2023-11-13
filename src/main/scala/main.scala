import datasets.EstabelecimentosFrame.estabelecimentosTreatment
import datasets.EmpresasFrame.empresasTreatment
import datasets.initSparkSession.write_parquet_files

object main extends App{

  //val dataFrameEstabelec = estabelecimentosTreatment
  //write_parquet_files(dataframe=dataFrameEstabelec, columnToPartition="uf", subDir="estabelecimentos")

  val dataFrameEmpresas = empresasTreatment
  write_parquet_files(dataframe=dataFrameEmpresas, columnToPartition="uf", subDir="empresas")

}
