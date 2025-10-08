variable "aws_region" {
  description = "AWS Region"
  type        = string
  default     = "us-east-1"
}

variable "s3_bucket" {
  description = "S3 Bucket for Glue scripts"
  type        = string
}

variable "project_name" {
  description = "Project name for tagging"
  type        = string
  default     = "techchallenge2"
}

variable "envenvironment" {
  description = "Deployment environment (e.g., dev, hom, prod)"
  type        = string
}

## Variables for Glue Jobs

variable "glue_max_capacity" {
  description = "Maximum capacity for Glue jobs"
  type        = number
  default     = 2
}

variable "glue_timeout" {
  description = "Timeout in minutes for Glue jobs"
  type        = number
  default     = 10
}

variable "glue_version" {
  description = "Glue version"
  type        = string
  default     = "3.0"
}

variable "python_version" {
  description = "Python version for Glue jobs"
  type        = string
  default     = "3.9"
}