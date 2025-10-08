resource "aws_iam_role_policy" "lambda_policy" {
  name   = "${var.project_name}_lambda_trigger_policy_${var.envenvironment}"
  role   = aws_iam_role.lambda_role.id
  policy = file("${path.module}/iam/policy_lambda.json")
}

resource "aws_iam_role" "lambda_role" {
  name = "${var.project_name}_lambda_trigger_role_${var.envenvironment}"
  assume_role_policy = file("${path.module}/iam/trust_lambda.json")
}