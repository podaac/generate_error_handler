#!/bin/bash
#
# Script to create a zipped deployment package for a Lambda function.
#
# Command line arguments:
# [1] app_name: Name of application to create a zipped deployment package for
# 
# Example usage: ./delpoy-lambda.sh "my-app-name"

APP_NAME=$1
ROOT_PATH="$PWD"

ZIP_PATH=$ROOT_PATH/$APP_NAME.zip
APP_PATH=$ROOT_PATH/$APP_NAME.py

zip $ZIP_PATH $APP_PATH
echo "Created: $ZIP_PATH."