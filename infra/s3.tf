resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-raw-${data.aws_caller_identity.current.account_id}"
  tags   = local.s3_tags
}

resource "aws_s3_bucket" "spec" {
  bucket = "${var.project_name}-spec-${data.aws_caller_identity.current.account_id}"
  tags   = local.s3_tags
}

resource "aws_s3_bucket" "artifacts" {
  bucket = "${var.project_name}-artifacts-${data.aws_caller_identity.current.account_id}"
  tags   = local.s3_tags
}