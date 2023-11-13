package datasets

import org.apache.spark.sql
import environment.projectConfigs.rootPath
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{ByteType, IntegerType, StringType}

object EstabelecimentosFrame {
  private val ss = initSparkSession.initSpark

  def estabelecimentosTreatment: sql.DataFrame = {
    val dirPath: String = s"$rootPath\\estabelecimentos\\"
    val mainDataframe: sql.DataFrame = ss.read.option("delimiter", ";").csv(dirPath)

    val estabelecColumns: Seq[String] = Seq(
      "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identif_matriz_filial", "nome_fantasia", "situ_cadastral",
      "dt_situacao_cadastral", "motiv_situ_cadastral", "nm_cidade_ext", "pais", "data_inicio_atv",
      "cnae_fiscal_prim", "cnae_fiscal_sec", "tp_lograd", "lograd", "numero", "complemento",
      "bairro", "cep", "uf", "cd_municipio", "ddd1", "telefone1", "ddd2", "telefone2", "ddd_fax", "fax",
      "email", "situ_espec", "data_situ_especial"
    )

    var dataFrameRenamedColumns = mainDataframe

    for (idx <- Range(0, estabelecColumns.length)) {
      dataFrameRenamedColumns = dataFrameRenamedColumns.withColumnRenamed(s"_c$idx", estabelecColumns(idx))
    }

    val dataFrameRemovedColumns = dataFrameRenamedColumns.drop("ddd2", "telefone2", "ddd_fax", "fax", "nm_cidade_ext", "tp_lograd", "numero", "complemento", "bairro","cd_municipio", "ddd1", "telefone1", "email")

    val dataFrameUfTreated = dataFrameRemovedColumns.withColumn("uf",
      when(functions.length(col("uf")) === 2, col("uf"))
        .otherwise(lit("NN"))
    )

    val dfCastedTypes = dataFrameUfTreated.withColumn("cnpj_basico", col("cnpj_basico").cast(IntegerType))
      .withColumn("cnpj_ordem",col("cnpj_ordem").cast(IntegerType))
      .withColumn("cnpj_dv", col("cnpj_dv").cast(ByteType))
      .withColumn("identif_matriz_filial",col("identif_matriz_filial").cast(IntegerType))
      .withColumn("nome_fantasia",col("nome_fantasia").cast(StringType))
      .withColumn("situ_cadastral",col("situ_cadastral").cast(ByteType))
      .withColumn("dt_situacao_cadastral",col("dt_situacao_cadastral").cast(IntegerType))
      .withColumn("motiv_situ_cadastral", col("motiv_situ_cadastral").cast(ByteType))
      .withColumn("pais", col("pais").cast(ByteType))
      .withColumn("data_inicio_atv", col("data_inicio_atv").cast(IntegerType))
      .withColumn("cnae_fiscal_prim", col("cnae_fiscal_prim").cast(IntegerType))
      .withColumn("cnae_fiscal_sec", col("cnae_fiscal_sec").cast(StringType))
      .withColumn("cep", col("cep").cast(IntegerType))
      .withColumn("situ_espec", col("situ_espec").cast(StringType))

    dfCastedTypes
  }
}