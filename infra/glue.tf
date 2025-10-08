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