resource "aws_cloudwatch_event_rule" "trigger_data_extraction" {
  name                = "${var.project_name}_trigger_data_extraction_rule_${var.environment}"
  description         = "Regra para disparar o pipeline de Data Extraction via Step Functions"
  schedule_expression = "cron(${var.cron_expression})"
}

resource "aws_cloudwatch_event_target" "trigger_data_extraction_target" {
  rule      = aws_cloudwatch_event_rule.trigger_data_extraction.name
  target_id = "StepFunctionsDataExtractionPipeline"
  arn       = aws_sfn_state_machine.data_extraction_pipeline.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
  
  input_transformer {
    input_paths = {
      time = "$.time"
    }
    input_template = <<EOF
    {
    "Arguments": {
        "--ENV": "${var.environment}",
        "--DT_REF": "<time>",
        "--BUCKET_TARGET": "${aws_s3_bucket.raw.bucket}",
        "--BASE_DIR": "yfinance/data"
    }
    }
    EOF
    }
}