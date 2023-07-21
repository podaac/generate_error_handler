"""AWS Lambda that handles AWS Batch job failure events from EventBridge.

Logs the error message.
Pusblishes error message to SNS Topic.
"""

# Standard imports
import logging
import os
import random
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
    
    # Sleep for a random amount of time for multiple job failures
    random.seed(a=event['detail']['jobId'], version=2)
    rand_float = random.uniform(1,10)
    logger.info(f"Sleeping for {rand_float} seconds.")
    time.sleep(rand_float)
    
    # Return licenses
    try:
        return_licenses(unique_id, prefix, dataset, logger)
    except botocore.exceptions.ClientError as e:
        if "(ParameterNotFound)" in str(e):
            logger.info(e)
            logger.info("No unique licenses were tracked in the parameter store for this execution.")
        elif "(TooManyUpdates)" in str(e):
            logger.info(e)
            logger.info("Trying to update the parameter store at the same time as another lambda.")
        else:
            logger.error(f"Error trying to restore reserved IDL licenses to the parameter store.")
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
    console_format = logging.Formatter("%(module)s - %(levelname)s : %(message)s")
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
    message = f"A Generate AWS Batch job has FAILED. Manual intervention required.\n\n" \
        + "JOB INFORMATION:\n" \
        + f"Job Name: {event['detail']['jobName']}.\n" \
        + f"Job Identifier: {event['detail']['jobId']}.\n" \
        + f"Job Queue: {event['detail']['jobQueue']}.\n\n"
    message += "ERROR INFORMATION:\n" \
        + f"Error message: '{error_msg}'\n" \
        + f"Container command: {event['detail']['container']['command']}\n"
    if len(event['detail']['attempts']) > 0: message += f"Log file: {event['detail']['attempts'][0]['container']['logStreamName']}\n"
    message += "\nThis indicates that a job has failed and manual intervention is required to resubmit OBPG files associated with the failure to the Generate workflow.\n\n"
    message += "Please follow these steps to diagnose and recover from the failure: https://wiki.jpl.nasa.gov/pages/viewpage.action?pageId=771470900#GenerateCloudErrorDetection&Recovery-AWSBatchJobFailures\n\n\n"
    try:
        response = sns.publish(
            TopicArn = topic_arn,
            Message = message,
            Subject = subject
        )
        logger.info(f"Published error message to: {topic_arn}.")
    except botocore.exceptions.ClientError as e:
        logger.error(f"Failed to publish to SNS Topic: {topic_arn}.")
        logger.error(f"Error - {e}")
        sys.exit(1)
    
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
        quicklook_lic = check_existence(ssm, f"{prefix}-idl-{dataset}-{unique_id}-ql", logger)
        refined_lic = check_existence(ssm, f"{prefix}-idl-{dataset}-{unique_id}-r", logger)
        floating_lic = check_existence(ssm, f"{prefix}-idl-{dataset}-{unique_id}-floating", logger)
        
        # Wait until no other process is updating license info
        retrieving_lic =  ssm.get_parameter(Name=f"{prefix}-idl-retrieving-license")["Parameter"]["Value"]
        while retrieving_lic == "True":
            logger.info("Waiting for license retrieval...")
            time.sleep(3)
            retrieving_lic =  ssm.get_parameter(Name=f"{prefix}-idl-retrieving-license")["Parameter"]["Value"]
        
        # Place hold on licenses so they are not changed
        hold_license(ssm, prefix, "True", logger)
        
        # Return licenses to appropriate parameters
        write_licenses(ssm, quicklook_lic, refined_lic, floating_lic, prefix, dataset, logger)
        
        # Delete unique parameters
        response = ssm.delete_parameters(
            Names=[f"{prefix}-idl-{dataset}-{unique_id}-ql",
                   f"{prefix}-idl-{dataset}-{unique_id}-r",
                   f"{prefix}-idl-{dataset}-{unique_id}-floating"]
        )
        if quicklook_lic != 0: logger.info(f"Deleted parameter: {prefix}-idl-{dataset}-{unique_id}-ql")
        if refined_lic != 0: logger.info(f"Deleted parameter: {prefix}-idl-{dataset}-{unique_id}-r")
        if floating_lic != 0: logger.info(f"Deleted parameter: {prefix}-idl-{dataset}-{unique_id}-floating")
        
        # Release hold as done updating
        hold_license(ssm, prefix, "False", logger)
        
    except botocore.exceptions.ClientError as e:
        raise e
    
def check_existence(ssm, parameter_name, logger):
        """Check existence of SSM parameter and return value if it exists.
        
        Returns 0 if does not exist.
        """
        
        try:
            parameter = ssm.get_parameter(Name=parameter_name)["Parameter"]["Value"]
            logger.info(f"Located {parameter_name} with {parameter} reserved IDL licenses.")
        except botocore.exceptions.ClientError as e:
            if "(ParameterNotFound)" in str(e) :
                parameter = 0
            else:
                logger.error(f"Error - {e}")
                logger.info("System exit.")
                exit(1)
        return parameter   

def hold_license(ssm, prefix, on_hold, logger):
        """Put parameter license number ot use indicating retrieval in process."""
        
        hold_action = "place" if on_hold == "True" else "remove"        
        try:
            response = ssm.put_parameter(
                Name=f"{prefix}-idl-retrieving-license",
                Type="String",
                Value=on_hold,
                Tier="Standard",
                Overwrite=True
            )
            logger.info(f"{hold_action.capitalize()}d a hold on licenses...")
        except botocore.exceptions.ClientError as e:
            logger.error(f"Could not {hold_action} a hold on licenses...")
            raise e
        
def write_licenses(ssm, quicklook_lic, refined_lic, floating_lic, prefix, dataset, logger):
    """Write license data to indicate number of licenses ready to be used."""
    
    try:
        current = ssm.get_parameter(Name=f"{prefix}-idl-{dataset}")["Parameter"]["Value"]
        total = int(quicklook_lic) + int(refined_lic) + int(current)
        if total > 0:
            response = ssm.put_parameter(
                Name=f"{prefix}-idl-{dataset}",
                Type="String",
                Value=str(total),
                Tier="Standard",
                Overwrite=True
            )
        logger.info(f"Wrote {int(quicklook_lic) + int(refined_lic)} license(s) to {prefix}-idl-{dataset}.")
        
        current_floating = ssm.get_parameter(Name=f"{prefix}-idl-floating")["Parameter"]["Value"]
        floating_total = int(floating_lic) + int(current_floating)
        if floating_total > 0:
            response = ssm.put_parameter(
                Name=f"{prefix}-idl-floating",
                Type="String",
                Value=str(floating_total),
                Tier="Standard",
                Overwrite=True
            )
        logger.info(f"Wrote {floating_lic} license(s) to {prefix}-idl-floating.")
    except botocore.exceptions.ClientError as e:
        logger.error(f"Could not return {int(quicklook_lic) + int(refined_lic)} {prefix}-idl-{dataset} and {floating_lic} {prefix}-idl-floating licenses...")
        raise e
