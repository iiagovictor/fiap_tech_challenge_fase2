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

resource "aws_s3_object" "app_files" {
  for_each = { 
    for f in fileset("${path.module}/../app", "**/*") : f => f 
    if fileexists("${path.module}/../app/${f}") && 
       !endswith(f, "/")
  }
  
  bucket = aws_s3_bucket.artifacts.id
  key    = "${var.project_name}/${each.value}"
  source = "${path.module}/../app/${each.value}"
  etag   = filemd5("${path.module}/../app/${each.value}")
  
  tags = local.s3_tags
}