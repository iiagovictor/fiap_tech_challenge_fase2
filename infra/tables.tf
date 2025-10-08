# Glue Data Catalog

resource "aws_glue_catalog_database" "main" {
  name = "${var.project_name}_db"
}

resource "aws_glue_catalog_table" "raw_table" {
  name          = var.raw_table_name
  database_name = aws_glue_catalog_database.main.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location = "s3://${var.project_name}-raw-${data.aws_caller_identity.current.account_id}/${var.raw_table_name}/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"

    ser_de_info {
      name                  = "ParquetHiveSerDe"
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }

    columns {
      name = "id"
      type = "string"
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