"""AWS Lambda that handles AWS Batch job failure events from EventBridge.

Logs the error message.
Pusblishes error message to SNS Topic.
"""

# Standard imports
import logging
import os
import sys
import time

# Third-party imports
import boto3
import botocore

def error_handler(event, context):
    """Handles error events delivered from EventBridge."""
    
    # Log and publish event
    logger = get_logger()
    if len(event['detail']['attempts']) > 0:
        error_msg = event['detail']['attempts'][0]['statusReason']
    else:
        error_msg = event['detail']['statusReason']
    log_event(event, error_msg, logger)
    publish_event(event, error_msg, logger)
    
    # Return reserved licenses
    unique_id = get_unique_id(event['detail']['container']['command'])
    prefix = '-'.join(event['detail']['jobName'].split('-')[0:3])
    dataset = event['detail']['jobQueue'].split('-')[-1]
    try:
        return_licenses(unique_id, prefix, dataset, logger)
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error trying to restore reserved IDL licenses.")
        logger.error(e)
        sys.exit(1)
    
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
        sys.exit(1)
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
        sys.exit(1)
    
    logger.info(f"Message published to SNS Topic: {topic_arn}.")
    
def get_unique_id(command):
    """Parse and return unique ID from container command."""
    
    unique_id = ""
    for arg in command:
        if "json" in arg:
            unique_id = arg.split('.')[0].split('_')[-1]
        
    if unique_id == "":    # License returner
        unique_id = command[0]
        
    return unique_id
    
def return_licenses(unique_id, prefix, dataset, logger):
    """Return licenses that were reserved for current workflow."""
    
    ssm = boto3.client("ssm", region_name="us-west-2")
    try:
        # Get number of licenses that were used in the workflow
        dataset_lic = ssm.get_parameter(Name=f"{prefix}-idl-{dataset}-{unique_id}-lic")["Parameter"]["Value"]
        floating_lic = ssm.get_parameter(Name=f"{prefix}-idl-{dataset}-{unique_id}-floating")["Parameter"]["Value"]
        
        # Wait until no other process is updating license info
        retrieving_lic =  ssm.get_parameter(Name=f"{prefix}-idl-retrieving-license")["Parameter"]["Value"]
        while retrieving_lic == "True":
            logger.info("Watiing for license retrieval...")
            time.sleep(3)
            retrieving_lic =  ssm.get_parameter(Name=f"{prefix}-idl-retrieving-license")["Parameter"]["Value"]
        
        # Place hold on licenses so they are not changed
        hold_license(ssm, prefix, "True", logger)  
        
        # Return licenses to appropriate parameters
        write_licenses(ssm, dataset_lic, floating_lic, prefix, dataset, logger)
        
        # Release hold as done updating
        hold_license(ssm, prefix, "False", logger)
        
        # Delete unique parameters
        response = ssm.delete_parameters(
            Names=[f"{prefix}-idl-{dataset}-{unique_id}-lic",
                    f"{prefix}-idl-{dataset}-{unique_id}-floating"]
        )
        logger.info(f"Deleted parameter: {prefix}-idl-{dataset}-{unique_id}-lic")
        logger.info(f"Deleted parameter: {prefix}-idl-{dataset}-{unique_id}-floating")
        
    except botocore.exceptions.ClientError as e:
        raise e

def hold_license(ssm, prefix, on_hold, logger):
        """Put parameter license number ot use indicating retrieval in process."""
        
        try:
            response = ssm.put_parameter(
                Name=f"{prefix}-idl-retrieving-license",
                Type="String",
                Value=on_hold,
                Tier="Standard",
                Overwrite=True
            )
        except botocore.exceptions.ClientError as e:
            hold_action = "place" if on_hold == "True" else "remove"
            logger.error(f"Could not {hold_action} a hold on licenses...")
            raise e
        
def write_licenses(ssm, dataset_lic, floating_lic, prefix, dataset, logger):
    """Write license data to indicate number of licenses ready to be used."""
    
    try:
        current = ssm.get_parameter(Name=f"{prefix}-idl-{dataset}")["Parameter"]["Value"]
        total = int(dataset_lic) + int(current)
        response = ssm.put_parameter(
            Name=f"{prefix}-idl-{dataset}",
            Type="String",
            Value=str(total),
            Tier="Standard",
            Overwrite=True
        )
        current_floating = ssm.get_parameter(Name=f"{prefix}-idl-floating")["Parameter"]["Value"]
        floating_total = int(floating_lic) + int(current_floating)
        response = ssm.put_parameter(
            Name=f"{prefix}-idl-floating",
            Type="String",
            Value=str(floating_total),
            Tier="Standard",
            Overwrite=True
        )
        logger.info(f"Wrote {dataset_lic} license(s) to {dataset}.")
        logger.info(f"Wrote {floating_lic} license(s)to floating.")
    except botocore.exceptions.ClientError as e:
        logger.error(f"Could not return {dataset} and floating licenses...")
        raise e
