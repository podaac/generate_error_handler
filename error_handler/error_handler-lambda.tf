# AWS Lambda function
resource "aws_lambda_function" "aws_lambda_error_handler" {
  filename         = "error_handler.zip"
  function_name    = "${var.prefix}-error-handler"
  role             = aws_iam_role.aws_lambda_execution_role.arn
  handler          = "error_handler.error_handler"
  runtime          = "python3.9"
  source_code_hash = filebase64sha256("error_handler.zip")
  environment {
    variables = {
      TOPIC = "${var.prefix}-batch-job-failure"
    }
  }
}

resource "aws_lambda_permission" "aws_lambda_error_handler_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.aws_lambda_error_handler.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.aws_eventbridge_batch_job_failure.arn
}

# AWS Lambda role and policy
resource "aws_iam_role" "aws_lambda_execution_role" {
  name = "${var.prefix}-lambda-error-handler-execution-role"
  assume_role_policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "lambda.amazonaws.com"
        },
        "Action" : "sts:AssumeRole"
      }
    ]
  })
  permissions_boundary = "arn:aws:iam::${local.account_id}:policy/NGAPShRoleBoundary"
}

resource "aws_iam_role_policy_attachment" "aws_lambda_execution_role_policy_attach" {
  role       = aws_iam_role.aws_lambda_execution_role.name
  policy_arn = aws_iam_policy.aws_lambda_execution_policy.arn
}

resource "aws_iam_policy" "aws_lambda_execution_policy" {
  name        = "${var.prefix}-lambda-error-handler-execution-policy"
  description = "Write to CloudWatch logs, publish and list SNS Topics."
  policy = jsonencode({
    "Version" : "2012-10-17",
    "Statement" : [
      {
        "Sid" : "AllowCreatePutLogs",
        "Effect" : "Allow",
        "Action" : [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        "Resource" : "arn:aws:logs:*:*:*"
      },
      {
        "Sid" : "AllowPublishToTopic",
        "Effect" : "Allow",
        "Action" : [
          "sns:Publish"
        ],
        "Resource" : "${aws_sns_topic.aws_sns_topic_batch_job_failure.arn}"
      },
      {
        "Sid" : "AllowListTopics",
        "Effect" : "Allow",
        "Action" : [
          "sns:ListTopics"
        ],
        "Resource" : "*"
      }
    ]
  })
}

# AWS EventBridge rule triggered by Batch job failure
resource "aws_cloudwatch_event_rule" "aws_eventbridge_batch_job_failure" {
  name        = "${var.prefix}-error-handler"
  description = "Detect AWS Batch job failure for Generate workflow."
  event_pattern = jsonencode({
    "account" : ["${local.account_id}"],
    "detail-type" : ["Batch Job State Change"],
    "source" : ["aws.batch"],
    "detail" : {
      "jobName" : [{ "prefix" : "${var.prefix}" }],
      "status" : ["FAILED"]
    }
  })
}

resource "aws_cloudwatch_event_target" "aws_eventbridge_batch_job_failure_target" {
  rule = aws_cloudwatch_event_rule.aws_eventbridge_batch_job_failure.name
  arn  = aws_lambda_function.aws_lambda_error_handler.arn
}

# SNS Batch job failure topic
resource "aws_sns_topic" "aws_sns_topic_batch_job_failure" {
  name         = "${var.prefix}-batch-job-failure"
  display_name = "${var.prefix}-batch-job-failure"
}

resource "aws_sns_topic_policy" "aws_sns_topic_batch_job_failure_policy" {
  arn = aws_sns_topic.aws_sns_topic_batch_job_failure.arn
  policy = jsonencode({
    "Version" : "2008-10-17",
    "Id" : "__default_policy_ID",
    "Statement" : [
      {
        "Sid" : "__default_statement_ID",
        "Effect" : "Allow",
        "Principal" : {
          "AWS" : "*"
        },
        "Action" : [
          "SNS:GetTopicAttributes",
          "SNS:SetTopicAttributes",
          "SNS:AddPermission",
          "SNS:RemovePermission",
          "SNS:DeleteTopic",
          "SNS:Subscribe",
          "SNS:ListSubscriptionsByTopic",
          "SNS:Publish"
        ],
        "Resource" : "${aws_sns_topic.aws_sns_topic_batch_job_failure.arn}",
        "Condition" : {
          "StringEquals" : {
            "AWS:SourceOwner" : "${local.account_id}"
          }
        }
      }
    ]
  })
}

resource "aws_sns_topic_subscription" "aws_sns_topic_batch_job_failure_subscription" {
  endpoint  = var.sns_topic_email
  protocol  = "email"
  topic_arn = aws_sns_topic.aws_sns_topic_batch_job_failure.arn
}

# SNS Lambda error handler failure topic
resource "aws_sns_topic" "aws_sns_topic_lambda_error_handler_failure" {
  name         = "${var.prefix}-lambda-error-handler-failure"
  display_name = "${var.prefix}-lambda-error-handler-failure"
}

resource "aws_sns_topic_policy" "aws_sns_topic_lambda_error_handler_failure_policy" {
  arn = aws_sns_topic.aws_sns_topic_lambda_error_handler_failure.arn
  policy = jsonencode({
    "Version" : "2008-10-17",
    "Id" : "__default_policy_ID",
    "Statement" : [
      {
        "Sid" : "__default_statement_ID",
        "Effect" : "Allow",
        "Principal" : {
          "AWS" : "*"
        },
        "Action" : [
          "SNS:GetTopicAttributes",
          "SNS:SetTopicAttributes",
          "SNS:AddPermission",
          "SNS:RemovePermission",
          "SNS:DeleteTopic",
          "SNS:Subscribe",
          "SNS:ListSubscriptionsByTopic",
          "SNS:Publish"
        ],
        "Resource" : "${aws_sns_topic.aws_sns_topic_lambda_error_handler_failure.arn}",
        "Condition" : {
          "StringEquals" : {
            "AWS:SourceOwner" : "${local.account_id}"
          }
        }
      },
      {
        "Sid" : "AllowPublishAlarms",
        "Effect" : "Allow",
        "Principal" : {
          "Service" : "cloudwatch.amazonaws.com"
        },
        "Action" : "sns:Publish",
        "Resource" : "${aws_sns_topic.aws_sns_topic_lambda_error_handler_failure.arn}"
      }
    ]
  })
}

resource "aws_sns_topic_subscription" "aws_sns_topic_lambda_error_handler_failure_subscription" {
  endpoint  = var.sns_topic_email
  protocol  = "email"
  topic_arn = aws_sns_topic.aws_sns_topic_lambda_error_handler_failure.arn
}

# Cloudwatch alarm for Lambda error handler failure
resource "aws_cloudwatch_metric_alarm" "aws_cloudwatch_lambda_error_handler_alarm" {
  alarm_name          = "${var.prefix}-lambda-error-handler-alarm"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = "1"
  metric_name         = "Errors"
  namespace           = "AWS/Lambda"
  period              = "60"
  statistic           = "Sum"
  threshold           = "1"
  alarm_description   = "Alarm when Lambda error handler function fails."
  alarm_actions       = [aws_sns_topic.aws_sns_topic_lambda_error_handler_failure.arn]
  treat_missing_data  = "notBreaching"
  dimensions = {
    "FunctionName" = "${aws_lambda_function.aws_lambda_error_handler.function_name}"
  }
}