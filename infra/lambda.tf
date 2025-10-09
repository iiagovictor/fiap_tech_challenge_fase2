resource "null_resource" "install_lambda_dependencies" {
  triggers = {
    requirements = fileexists("${path.module}/../app/lambda/requirements.txt") ? filemd5("${path.module}/../app/lambda/requirements.txt") : ""
    source_code  = sha256(join("", [for f in fileset("${path.module}/../app/lambda", "*.py") : filemd5("${path.module}/../app/lambda/${f}")]))
  }

  provisioner "local-exec" {
    command = <<-EOT
      cd ${path.module}/../app/lambda
      if [ -f requirements.txt ]; then
        pip install -r requirements.txt -t . --upgrade
      fi
    EOT
  }
}

data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/../app/lambda"
  output_path = "${path.module}/../app/lambda/main.zip"
  excludes    = ["main.zip", "__pycache__", "*.pyc", "*.dist-info", "*.egg-info"]
  
  depends_on = [null_resource.install_lambda_dependencies]
}

resource "aws_lambda_function" "lambda_trigger" {
  function_name    = "${var.project_name}_lambda_trigger"
  role             = aws_iam_role.lambda_role.arn
  handler          = "main.lambda_handler"
  runtime          = "python3.9"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256
  timeout          = 30

  environment {
    variables = {
      ENV = var.environment
    }
  }

  tags = local.lambda_tags
}

resource "aws_lambda_permission" "allow_s3_raw" {
  statement_id  = "AllowExecutionFromS3Raw"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.lambda_trigger.arn
  principal     = "s3.amazonaws.com"
  source_arn    = aws_s3_bucket.raw.arn
}

resource "aws_s3_bucket_notification" "raw_lambda_trigger" {
  bucket = aws_s3_bucket.raw.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.lambda_trigger.arn
    events              = ["s3:ObjectCreated:*"]
  }

  depends_on = [aws_lambda_permission.allow_s3_raw]
}