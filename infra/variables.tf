variable "project_name" {
  description = "Project name for tagging"
  type        = string
  default     = "techchallenge2"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "environment" {
  description = "Deployment environment (e.g., dev, hom, prod)"
  type        = string
}

## Table Variables

variable "spec_table_name" {
  description = "Name of the spec data table"
  type        = string
  default     = "tb_techchallenge2_spec_data"
}

## Cron Expression Variable

variable "cron_expression" {
  description = "Cron expression for scheduling the Glue Job Data Extraction"
  type        = string
  default     = "0 12 * * ? *"
}

## Variables for Glue Jobs

variable "glue_max_capacity" {
  description = "Maximum capacity for Glue jobs"
  type        = number
  default     = 1
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