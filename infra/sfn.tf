# Step Functions State Machine para orquestrar o Glue Job
resource "aws_sfn_state_machine" "data_extraction_pipeline" {
  name     = "${var.project_name}_data_extraction_pipeline_${var.environment}"
  role_arn = aws_iam_role.sfn_role.arn

  definition = jsonencode({
    Comment = "Pipeline de extração de dados com AWS Glue"
    StartAt = "StartGlueJob"
    States = {
      StartGlueJob = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.data_extraction.name
          "Arguments.$" = "$.Arguments"
        }
        End = true
      }
    }
  })

  tags = local.sfn_tags
}

# Output para usar no EventBridge
output "sfn_arn" {
  value       = aws_sfn_state_machine.data_extraction_pipeline.arn
  description = "ARN da Step Functions State Machine"
}