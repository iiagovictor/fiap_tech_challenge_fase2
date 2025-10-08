# AWS Glue Infrastructure

resource "aws_glue_job" "data_extraction" {
  name              = "${var.project_name}_data_extraction_job_${var.environment}"
  role_arn          = aws_iam_role.glue_role.arn
  command {
    name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.artifacts.bucket}/glue-scripts/data_extraction.py"
    python_version  = var.python_version
  }
  max_capacity      = var.glue_max_capacity
  timeout           = var.glue_timeout
  glue_version      = var.glue_version
  tags              = local.glue_tags

  depends_on = [ 
    aws_s3_bucket.artifacts,
    aws_iam_role.glue_role
    ]
}

resource "aws_glue_job" "data_ingestion" {
  name              = "${var.project_name}_data_ingestion_job_${var.environment}"
  role_arn          = aws_iam_role.glue_role.arn
  command {
    name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.artifacts.bucket}/glue-scripts/data_ingestion.py"
    python_version  = var.python_version
  }
  max_capacity      = var.glue_max_capacity
  timeout           = var.glue_timeout
  glue_version      = var.glue_version
  tags              = local.glue_tags

  depends_on = [ 
    aws_s3_bucket.artifacts,
    aws_iam_role.glue_role
    ]
}

# Glue Data Catalog

resource "aws_glue_catalog_database" "main" {
  name = "${var.project_name}_db"
}

resource "aws_glue_catalog_table" "raw_table" {
  name          = "tb_${var.project_name}_raw_data"
  database_name = aws_glue_catalog_database.main.name

  table_type = "EXTERNAL_TABLE"

  parameters = {
    classification = "parquet"
  }

  storage_descriptor {
    location = "s3://${var.project_name}-raw-${aws_caller_identity.current.account_id}/${aws_glue_catalog_table.raw_table.name}/"
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