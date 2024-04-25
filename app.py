import json
import logging
import numpy as np
from crossover.parallel_crossovers import crossover_setup

def handler(event, context):
    if 'Records' in event:
        for record in event['Records']:
            message_body = json.loads(record['body'])
            day = np.datetime64(message_body.get('date'))
            source_1 = message_body.get('source_1')
            source_2 = message_body.get('source_2')
            if not all([source_1, source_2, day]):
                logging.exception(f'Missing job parameters: source_1="{source_1}", source_2="{source_2}", day="{day}"')
                continue
            crossover_setup(day, source_1, source_2)
    else:
        day = np.datetime64(event.get('date'))
        source_1 = event.get('source_1')
        source_2 = event.get('source_2')
        
        if not all([source_1, source_2, day]):
            raise AttributeError(f'Missing job parameters: source_1="{source_1}", source_2="{source_2}", day="{day}"')
        crossover_setup(day, source_1, source_2)