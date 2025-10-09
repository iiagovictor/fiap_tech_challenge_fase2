resource "aws_lambda_function" "lambda_trigger" {
  function_name    = "${var.project_name}_lambda_trigger"
  role             = aws_iam_role.lambda_role.arn
  handler          = "main.lambda_handler"
  runtime          = "python3.9"
  filename         = "${path.module}/../app/lambda/main.zip"
  source_code_hash = filebase64sha256("${path.module}/../app/lambda/main.zip")
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