import json
import boto3

def lambda_handler(event, context):
    sns = boto3.client('sns')
    s3 = boto3.client('s3')
    rekognition = boto3.client('rekognition')

    topic_name = 'ImageAnalysisNotification'
    email_address = 'praveen.kumar.r.burla@gmail.com'

    # Step 1 & 2: Check if the topic exists and get its ARN
    response = sns.list_topics()
    topic_arn = None
    for topic in response['Topics']:
        if topic['TopicArn'].endswith(':' + topic_name):
            topic_arn = topic['TopicArn']
            break

    # Step 3: Check if the email is already subscribed and confirmed
    response = sns.list_subscriptions_by_topic(TopicArn=topic_arn)
    is_already_subscribed = False
    for subscription in response['Subscriptions']:
        if subscription['Endpoint'] == email_address and subscription['SubscriptionArn'].startswith('arn:'):
            is_already_subscribed = True
            break

    # Subscribe the email if not already subscribed/confirmed
    if not is_already_subscribed:
        sns.subscribe(
            TopicArn=topic_arn,
            Protocol='email',
            Endpoint=email_address
        )
        return {
            'statusCode': 200,
            'body': json.dumps('Email verification sent.')
        }

    # Process S3 event
    for record in event['Records']:
        input_bucket = record['s3']['bucket']['name']
        input_key = record['s3']['object']['key']

        # Get the file from S3
        response = s3.get_object(Bucket=input_bucket, Key=input_key)
        image_content = response['Body'].read()

        # Image Rekognition
        rekognition_response = rekognition.detect_labels(
            Image={'Bytes': image_content},
            MaxLabels=10
        )
        labels_message_parts = [
            f"{label['Name']} ({label['Confidence']:.2f}% confidence)"
            for label in rekognition_response['Labels']
        ]
        labels_message = ', '.join(labels_message_parts)

        # Send An Email via SNS for already subscribed users
        sns_message = f'Labels detected in the image {input_key}: {labels_message}.'
        sns.publish(
            TopicArn=topic_arn,
            Message=sns_message,
            Subject='Image Labels Detection'
        )

    return {
        'statusCode': 200,
        'body': json.dumps('Process completed successfully!')
    }
