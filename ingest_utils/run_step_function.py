#!/usr/bin/env python3
"""
Script to run a Step Function execution using the ARN from config.
"""

import argparse
import boto3
import json
import yaml
import sys

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
    parser = argparse.ArgumentParser(description='Run a Step Function execution.')
    parser.add_argument('--arn', type=str, help='Step Function ARN (optional, otherwise uses config)')
    parser.add_argument('--reset-cache', action='store_true', help='Use {"cache": "reset"} as input')
    parser.add_argument('--input', '-i', type=str, help='Custom JSON input string')
    args = parser.parse_args()
    
    # Determine step function ARN
    step_function_arn = args.arn
    if not step_function_arn:
        config = load_config()
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
    print(f"Starting Step Function execution with ARN: {step_function_arn}")
    print(f"Input data: {json.dumps(input_data)}")
    
    response = run_step_function(step_function_arn, input_data)
    
    print(f"Step Function execution started successfully!")
    print(f"Execution ARN: {response['executionArn']}")

if __name__ == "__main__":
    main()
