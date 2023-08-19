#!/bin/bash
#
# Script to create a zipped deployment package for a Lambda function.
#
# Command line arguments:
# [1] app_name: Name of application to create a zipped deployment package for
# 
# Example usage: ./delpoy-lambda.sh "my-app-name"

APP_NAME=$1

# Zip script
zip $APP_NAME.zip $APP_NAME.py
echo "Created: $APP_NAME.zip."
