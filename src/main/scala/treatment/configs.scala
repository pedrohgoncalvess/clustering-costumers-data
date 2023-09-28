package treatment

import com.typesafe.config.{Config, ConfigFactory}
import java.nio.file.{Files, Paths}

object projectConfigs{

  private val config: Config = ConfigFactory.load()
  val estabelecSubDir: String = config.getString("ESTABELEC_SUB_DIR")
  val sociosSubDir: String = config.getString("SOCIOS_SUB_DIR")
  val empresasSubDir: String = config.getString("EMPRESAS_SUB_DIR")
  val treatedFilesDir: String = config.getString("TREATED_DIR")
  val rootPath: String = config.getString("DIR_DATASETS")

  def create_treated_datasets_dir(subDirName:String): Unit = {
    val path = Paths.get(f"$rootPath\\$subDirName")
    if (!(Files.exists(path) && Files.isDirectory(path)))
      Files.createDirectory(path)
  }

  val estabelecColumns:Seq[String] = Seq(
    "cnpj_basico", "cnpj_ordem", "cnpj_dv", "identif_matriz_filial", "nome_fantasia", "situ_cadastral",
    "dt_situacao_cadastral", "motiv_situ_cadastral", "nm_cidade_ext", "pais", "data_inicio_atv",
    "cnae_fiscal_prim", "cnae_fiscal_sec", "tp_lograd", "lograd", "numero", "complemento",
    "bairro", "cep", "uf", "municipio", "ddd1", "telefone1", "ddd2", "telefone2", "ddd_fax", "fax",
    "correio_eletronico", "situ_espec", "data_situ_especial"
  )

  val empresasColumns: Seq[String] = Seq("cnpj_basico", "razao", "natureza_juridica", "qualificacao", "capital", "porte", "ente")

  val sociosColumns: Seq[String] = Seq(
    "cnpj_basico", "identific_socio", "nome_socio",
    "cnpj-cpf", "qualif_socio", "dt_entrada_socie",
    "pais", "represent_legal", "nm_representant",
    "qualif_represent_legal", "faixa_etaria"
  )
}
