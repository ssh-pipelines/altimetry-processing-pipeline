import boto3
import os
import json

# Initialize SNS client
sns_client = boto3.client("sns")
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]


def lambda_handler(event, context):
    error_details = event.get("errorMessage", {})

    # Prepare SNS message
    message = json.dumps(
        {"Message": "Pipeline Failure Detected", "ErrorDetails": error_details},
        indent=2,
    )

    # Publish to SNS topic
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN, Subject="Pipeline Failure Notification", Message=message
    )

    return {"status": "Notification sent", "details": error_details}
