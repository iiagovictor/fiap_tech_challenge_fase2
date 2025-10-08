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

variable "raw_table_name" {
  description = "Name of the raw data table"
  type        = string
  default     = "tb_techchallenge2_raw_data"
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