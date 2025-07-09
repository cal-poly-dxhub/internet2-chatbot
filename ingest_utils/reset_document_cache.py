"""Script to reset the document cache held in DynamoDB."""

import boto3
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

def reset_dynamodb_cache():
    """
    Reset the DynamoDB cache by deleting all items in the table.
    
    Returns:
        bool: True if successful, False otherwise
    """
    config = load_config()
    
    if 'processed_files_table' not in config:
        print("Error: 'processed_files_table' not found in config.yaml")
        return False
    
    table_name = config['processed_files_table']
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    
    print(f"Resetting DynamoDB cache table: {table_name}")
    
    # Scan the table to get all items
    try:
        response = table.scan()
        items = response.get('Items', [])
        
        if not items:
            print("Cache is already empty. No items to delete.")
            return True
            
        # Delete each item
        with table.batch_writer() as batch:
            for item in items:
                batch.delete_item(Key={'s3_uri': item['s3_uri']})
        
        print(f"Successfully deleted {len(items)} items from the cache table")
        return True
    except Exception as e:
        print(f"Error resetting DynamoDB cache: {str(e)}")
        return False

if __name__ == "__main__":
    reset_dynamodb_cache()
