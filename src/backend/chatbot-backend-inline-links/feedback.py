import json
import logging
import os
import time
from typing import Any, Dict

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource("dynamodb")


def save_feedback(session_id: str, timestamp: int, rating: str, feedback_text: str = "") -> None:
    """Save feedback for a message."""
    table = dynamodb.Table(os.getenv("CONVERSATION_TABLE"))
    
    if rating in ["thumbs_up", "thumbs_down"]:
        # Save thumb feedback
        table.update_item(
            Key={
                "session_id": session_id,
                "timestamp": timestamp
            },
            UpdateExpression="SET thumb_rating = :rating",
            ExpressionAttributeValues={
                ":rating": rating
            }
        )
    else:
        # Save text feedback
        table.update_item(
            Key={
                "session_id": session_id,
                "timestamp": timestamp
            },
            UpdateExpression="SET feedback_text = :text",
            ExpressionAttributeValues={
                ":text": feedback_text
            }
        )


def feedback_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """Handle feedback submission."""
    try:
        body_data: Dict[str, Any] = json.loads(event["body"])
        session_id: str = body_data["session_id"]
        timestamp: int = body_data["timestamp"]
        rating: str = body_data["rating"]  # "thumbs_up" or "thumbs_down"
        feedback_text: str = body_data.get("feedback_text", "")

        save_feedback(session_id, timestamp, rating, feedback_text)

        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            "body": json.dumps({"message": "Feedback saved"})
        }

    except Exception as e:
        logger.error(f"Error in feedback_handler: {e}")
        return {
            "statusCode": 500,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key",
                "Access-Control-Allow-Methods": "OPTIONS,POST"
            },
            "body": json.dumps("Error saving feedback")
        }
