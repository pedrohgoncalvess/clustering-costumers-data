package datasets

import datasets.EstabelecimentosFrame.estabelecimentosTreatment
import environment.projectConfigs.rootPath
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.{FloatType, StringType}

object EmpresasFrame {

  def empresasTreatment: sql.DataFrame = {

    val ss = initSparkSession.initSpark
    val empresasColumns: Seq[String] = Seq ("cnpj_basico", "razao", "natureza_juridica", "qualificacao", "capital", "porte", "ente")
    val empresaDf = estabelecimentosTreatment.select ("cnpj_basico", "uf")

    val dirPath: String = s"$rootPath\\empresas\\"
    var mainDataframe: sql.DataFrame = ss.read.option ("delimiter", ";").csv(dirPath)

    for (idx <- Range (0, empresasColumns.length) ) {
    mainDataframe = mainDataframe.withColumnRenamed (s"_c$idx", empresasColumns (idx) )
  }

    val filteredDf = mainDataframe.filter ("razao is not null")

    val treatedDf = filteredDf.withColumn ("capital", regexp_replace (col ("capital"), ".", ",") )
    val castedTypesDf = treatedDf.withColumn ("cnpj_basico", col ("cnpj_basico").cast (StringType) )
    .withColumn ("razao", col ("razao").cast (StringType) )
    .withColumn ("natureza_juridica", col ("natureza_juridica").cast (StringType) )
    .withColumn ("capital", col ("capital").cast (FloatType) )
    .withColumn ("porte", col ("porte").cast (StringType) )
    .withColumn ("ente", col ("ente").cast (StringType) )

    val joinedFrame = castedTypesDf.join (empresaDf, castedTypesDf("cnpj_basico") === empresaDf("cnpj_basico"), "inner")

    joinedFrame
  }
}
