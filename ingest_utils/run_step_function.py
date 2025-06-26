#!/usr/bin/env python3
"""
Script to run a Step Function execution using the ARN from config.
Automatically checks and creates OpenSearch index if needed before running.
"""

import argparse
import boto3
import json
import yaml
import sys
from os_index_creator import check_create_index

def load_config():
    """Load configuration from config.yaml file."""
    try:
        with open("../config.yaml", "r") as config_file:
            return yaml.safe_load(config_file)
    except FileNotFoundError:
        print("Error: config.yaml file not found in the parent directory.")
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing config.yaml: {e}")
        sys.exit(1)

def check_create_opensearch_index(config):
    """Check/create OpenSearch index using the function from os_index_creator."""
    print("Checking OpenSearch index...")
    domain_index = config["opensearch_index_name"]
    check_create_index(domain_index)

def run_step_function(step_function_arn, input_data):
    """Run a Step Function execution with the provided ARN and input data."""
    client = boto3.client('stepfunctions')
    
    try:
        response = client.start_execution(
            stateMachineArn=step_function_arn,
            input=json.dumps(input_data)
        )
        return response
    except Exception as e:
        print(f"Error starting Step Function execution: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='Run a Step Function execution with automatic OpenSearch index creation.')
    parser.add_argument('--arn', type=str, help='Step Function ARN (optional, otherwise uses config)')
    parser.add_argument('--reset-cache', action='store_true', help='Use {"cache": "reset"} as input')
    parser.add_argument('--input', '-i', type=str, help='Custom JSON input string')
    parser.add_argument('--skip-index-check', action='store_true', help='Skip OpenSearch index check/creation')
    args = parser.parse_args()
    
    # Load configuration
    config = load_config()
    
    # Check/create OpenSearch index unless skipped
    if not args.skip_index_check:
        check_create_opensearch_index(config)
    
    # Determine step function ARN
    step_function_arn = args.arn
    if not step_function_arn:
        if 'step_function_arn' not in config:
            print("Error: 'step_function_arn' not found in config.yaml and no ARN provided")
            sys.exit(1)
        step_function_arn = config['step_function_arn']
    
    # Determine input data
    if args.reset_cache:
        input_data = {"cache": "reset"}
    elif args.input:
        try:
            input_data = json.loads(args.input)
        except json.JSONDecodeError:
            print("Error: Input is not valid JSON")
            sys.exit(1)
    else:
        input_data = {}
    
    # Run the Step Function
    print(f"\nStarting Step Function execution...")
    print(f"ARN: {step_function_arn}")
    print(f"Input: {json.dumps(input_data)}")
    
    response = run_step_function(step_function_arn, input_data)
    
    print(f"\n✓ Step Function execution started successfully!")
    print(f"Execution ARN: {response['executionArn']}")
    print(f"\nYou can monitor progress in the AWS Console or start testing now.")
    print(f"Note: Response quality will improve as more documents are processed.")

if __name__ == "__main__":
    main()
