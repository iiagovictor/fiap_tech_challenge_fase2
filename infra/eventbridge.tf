resource "aws_cloudwatch_event_rule" "trigger_data_extraction" {
  name                = "${var.project_name}_trigger_data_extraction_rule_${var.environment}"
  description         = "Regra para disparar o Glue Job de Data Extraction"
  schedule_expression = "cron(${var.cron_expression})"
}

resource "aws_cloudwatch_event_target" "trigger_data_extraction_target" {
  rule      = aws_cloudwatch_event_rule.trigger_data_extraction.name
  target_id = "GlueDataExtractionJob"
  arn       = aws_glue_job.data_extraction.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
  
  input = jsonencode({
    JobName = aws_glue_job.data_extraction.name
    Arguments = {
      "--ENV" = var.environment
    }
  })
}