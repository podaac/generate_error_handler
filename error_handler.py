"""AWS Lambda that handles AWS Batch job failure events from EventBridge.

Logs the error message.
Pusblishes error message to SNS Topic.
"""

# Standard imports
import logging
import os

# Third-party imports
import boto3
import botocore

def error_handler(event, context):
    """Handles error events delivered from EventBridge."""
    
    logger = get_logger()
    if len(event['detail']['attempts']) > 0:
        error_msg = event['detail']['attempts'][0]['statusReason']
    else:
        error_msg = event['detail']['statusReason']
    log_event(event, error_msg, logger)
    publish_event(event, error_msg, logger)
    if len(event['detail']['attempts']) > 0:
        error_msg = event['detail']['attempts'][0]['statusReason']
    else:
        error_msg = event['detail']['statusReason']
    log_event(event, error_msg, logger)
    publish_event(event, error_msg, logger)
    
def get_logger():
    """Return a formatted logger object."""
    
    # Remove AWS Lambda logger
    logger = logging.getLogger()
    for handler in logger.handlers:
        logger.removeHandler(handler)
    
    # Create a Logger object and set log level
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    # Create a handler to console and set level
    console_handler = logging.StreamHandler()

    # Create a formatter and add it to the handler
    console_format = logging.Formatter("%(asctime)s - %(module)s - %(levelname)s : %(message)s")
    console_handler.setFormatter(console_format)

    # Add handlers to logger
    logger.addHandler(console_handler)

    # Return logger
    return logger

def log_event(event, error_msg, logger):
    """Log event details in CloudWatch."""
    
    logger.info(f"Event - {event}")
    logger.info(f"Failed job account - {event['account']}")
    logger.info(f"Failed job name - {event['detail']['jobName']}")
    logger.info(f"Failed job id - {event['detail']['jobId']}")
    logger.info(f"Failed job queue - {event['detail']['jobQueue']}")
    logger.info(f"Error message - '{error_msg}'")
    if len(event['detail']['attempts']) > 0: logger.info(f"Log file - {event['detail']['attempts'][0]['container']['logStreamName']}")
    logger.info(f"Container command - {event['detail']['container']['command']}")
    
def publish_event(event, error_msg, logger):
    """Publish event to SNS Topic."""
    
    sns = boto3.client("sns")
    
    # Get topic ARN
    try:
        topics = sns.list_topics()
    except botocore.exceptions.ClientError as e:
        logger.error("Failed to list SNS Topics.")
        logger.error(f"Error - {e}")
        exit(1)
    for topic in topics["Topics"]:
        if os.environ.get("TOPIC") in topic["TopicArn"]:
            topic_arn = topic["TopicArn"]
            
    # Publish to topic
    subject = f"Generate Batch Job Failure: {event['detail']['jobName']}"
    message = f"A Generate AWS Batch job has failed: {event['detail']['jobName']}.\n" \
        + f"Job Identifier: {event['detail']['jobId']}.\n" \
        + f"Error message: '{error_msg}'\n" \
        + f"Container command: {event['detail']['container']['command']}\n"
    if len(event['detail']['attempts']) > 0: message += f"Log file: {event['detail']['attempts'][0]['container']['logStreamName']}\n"
    try:
        response = sns.publish(
            TopicArn = topic_arn,
            Message = message,
            Subject = subject
        )
    except botocore.exceptions.ClientError as e:
        logger.error(f"Failed to publish to SNS Topic: {topic_arn}.")
        logger.error(f"Error - {e}")
        exit(1)
    
    logger.info(f"Message published to SNS Topic: {topic_arn}.")