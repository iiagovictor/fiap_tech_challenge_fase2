# Glue Data Catalog

resource "aws_glue_catalog_database" "main" {
  name = "${var.project_name}_db"
}

resource "aws_glue_catalog_table" "spec_table" {
  name          = var.spec_table_name
  database_name = aws_glue_catalog_database.main.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location = "s3://${var.project_name}-spec-${data.aws_caller_identity.current.account_id}/${var.spec_table_name}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "ParquetHiveSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    # NOVA COLUNA 1: Nome Completo da Empresa/Ativo
    columns {
      name = "nome_completo"
      type = "string"
      comment = "O nome completo da companhia ou fundo de investimento proprietário do ativo. Ex: Petróleo Brasileiro S.A. - Petrobras, Vale S.A."
    }
    
    # NOVA COLUNA 2: Setor de Atuação
    columns {
      name = "setor"
      type = "string"
      comment = "O segmento da economia em que a empresa opera. Ajuda a categorizar e comparar empresas. Ex: Energia Elétrica, Bancos, Mineração, Tecnologia."
    }

    # NOVA COLUNA 3: Capitalização de Mercado
    columns {
      name = "capitalizao_mercado"
      type = "bigint"
      comment = "O valor total de mercado da empresa. É calculado multiplicando o regularMarketPrice (preço atual) pelo número total de ações em circulação."
    }

    # NOVA COLUNA 4: Volume de Negociação
    columns {
      name = "volume"
      type = "bigint"
      comment = "O número total de ações ou contratos do ativo que foram negociados (comprados e vendidos) durante um determinado período (geralmente o dia atual). É uma medida da liquidez do ativo."
    }
    
    # NOVA COLUNA 5: Tipo de Cotação
    columns {
      name = "tipo_acao"
      type = "string"
      comment = "Define a natureza do ativo. Pode indicar se é uma Ação (Equity), um Índice (Index), um Fundo Imobiliário (FII), etc." 
    }

    # NOVA COLUNA 6: Preço de Mercado Atual
    columns {
      name = "preco_mercado"
      type = "decimal(18, 4)"
      comment = "O último preço pelo qual o ativo foi negociado. É o preço de referência atual."
    }

    # NOVA COLUNA 7: Preço de Abertura
    columns {
      name = "abertura"
      type = "decimal(18, 4)"
      comment = "O preço pelo qual a primeira negociação do ativo ocorreu no início do dia de negociação."
    }

    # NOVA COLUNA 8: Mínima do Dia
    columns {
      name = "minimo_dia"
      type = "decimal(18, 4)"
      comment = "O preço mais baixo que o ativo atingiu em qualquer momento durante o dia de negociação."
    }

    # NOVA COLUNA 9: Máxima do Dia
    columns {
      name = "maximo_dia"
      type = "decimal(18, 4)"
      comment = "O preço mais alto que o ativo atingiu em qualquer momento durante o dia de negociação."
    }

    # NOVA COLUNA 9: Máxima do Dia
    columns {
      name = "delta_variacao_do_dia"
      type = "decimal(18, 4)"
      comment = "Variação do maior e menor valor da ação no dia"
    }

    # NOVA COLUNA 9: Máxima do Dia
    columns {
      name = "delta_variacao_dia_anterior"
      type = "decimal(18, 4)"
      comment = "Variação do preço da ação entre os dias"
    }
  }

  partition_keys {
    name = "dat_ano_rffc"
    type = "string"
  }
  partition_keys {
    name = "dat_mes_rffc"
    type = "string"
  }
  partition_keys {
    name = "dat_dia_rffc"
    type = "string"
  }
  partition_keys {
    name = "ticker"
    type = "string"
  }
}