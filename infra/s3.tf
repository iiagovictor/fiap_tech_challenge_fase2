resource "aws_s3_bucket" "raw" {
  bucket = "${var.project_name}-raw-${aws_caller_identity.current.account_id}"
  tags   = local.s3_tags
}

resource "aws_s3_bucket" "spec" {
  bucket = "${var.project_name}-spec-${aws_caller_identity.current.account_id}"
  tags   = local.s3_tags
}

resource "aws_s3_bucket" "artifacts" {
  bucket = "${var.project_name}-artifacts-${aws_caller_identity.current.account_id}"
  tags   = local.s3_tags
}