# Lambda Trigger IAM Role and Policy

resource "aws_iam_role_policy" "lambda_policy" {
  name   = "${var.project_name}_lambda_trigger_policy_${var.environment}"
  role   = aws_iam_role.lambda_role.id
  policy = file("${path.module}/iam/policies/policy_lambda.json")
}

resource "aws_iam_role" "lambda_role" {
  name = "${var.project_name}_lambda_trigger_role_${var.environment}"
  description = "Role para execução da Lambda Trigger do projeto ${var.project_name}"
  assume_role_policy = file("${path.module}/iam/trust/trust_lambda.json")
}

# Glue IAM Role and Policy

resource "aws_iam_role_policy" "glue_policy" {
  name   = "${var.project_name}_glue_job_policy_${var.environment}"
  role   = aws_iam_role.glue_role.id
  policy = file("${path.module}/iam/policies/policy_glue.json")
}

resource "aws_iam_role" "glue_role" {
  name = "${var.project_name}_glue_job_role_${var.environment}"
  description = "Role para execução dos Glue Jobs do projeto ${var.project_name}"
  assume_role_policy = file("${path.module}/iam/trust/trust_glue.json")
}

# EventBridge IAM Role and Policy

resource "aws_iam_role_policy" "eventbridge_policy" {
  name   = "${var.project_name}_eventbridge_policy_${var.environment}"
  role   = aws_iam_role.eventbridge_role.id
  policy = file("${path.module}/iam/policies/policy_eventbridge.json")
}

resource "aws_iam_role" "eventbridge_role" {
  name = "${var.project_name}_eventbridge_role_${var.environment}"
  description = "Role para EventBridge do projeto ${var.project_name}"
  assume_role_policy = file("${path.module}/iam/trust/trust_eventbridge.json")
}