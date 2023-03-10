variable "app_name" {
  type        = string
  description = "Application name"
  default     = "generate"
}

variable "app_version" {
  type        = number
  description = "The application version number"
}

variable "aws_region" {
  type        = string
  description = "AWS region to deploy to"
  default     = "us-west-2"
}

variable "default_tags" {
  type    = map(string)
  default = {}
}

variable "environment" {
  type        = string
  description = "The environment in which to deploy to"
}

variable "prefix" {
  type        = string
  description = "Prefix to add to all AWS resources as a unique identifier"
}

variable "profile" {
  type        = string
  description = "Named profile to build infrastructure with"
}

variable "sns_topic_email" {
  type        = string
  description = "Email to send SNS Topic messages to"
}