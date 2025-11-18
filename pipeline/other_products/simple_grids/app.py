import json
import logging
from simple_gridder import simple_gridder

def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level="INFO", format="[%(levelname)s] %(asctime)s - %(message)s", handlers=[logging.StreamHandler()]
    )
    
    if 'Records' in event:
        sqs_messages = event['Records']
        for message in sqs_messages:
            # Get the message body
            message_body = json.loads(message['body'])
            date = message_body.get('date')
            source = message_body.get('source')
            resolution = message_body.get('resolution')
            
            try:
                if date is None:
                    raise ValueError('"date" is a required parameter but none is given')
                simple_gridder.start_job(date, source, resolution)
            except Exception as e:
                logging.exception(e)
    else:
        date = event.get('date')
        source = event.get('source')
        resolution = event.get('resolution')
        
        try:
            if date is None:
                raise ValueError('"date" is a required parameter but none is given')
            simple_gridder.start_job(date, source, resolution)
        except Exception as e:
            logging.exception(e)