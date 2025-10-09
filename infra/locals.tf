locals {
  glue_tags = {
    projeto        = "techchallenge2"
    finopsCategory = "etl"
  }
  s3_tags = {
    projeto        = "techchallenge2"
    finopsCategory = "storage"
  }
  lambda_tags = {
    projeto        = "techchallenge2"
    finopsCategory = "compute"
  }
  sfn_tags = {
    projeto        = "techchallenge2"
    finopsCategory = "orchestration"
  }
}