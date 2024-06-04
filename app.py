import json
import logging
import numpy as np
from crossover.parallel_crossovers import compute_crossovers

def handler(event, context):
    if 'Records' in event:
        for record in event['Records']:
            message_body = json.loads(record['body'])
            day = np.datetime64(message_body.get('date'))
            source_1 = message_body.get('source_1')
            source_2 = message_body.get('source_2', source_1)
            daily_file_version = message_body.get('df_version')
            
            if not all([source_1, source_2, day]):
                logging.exception(f'Missing job parameters: {daily_file_version = },{source_1 = },{source_2 = },{day = }')
                continue
            compute_crossovers(day, source_1, source_2, daily_file_version)
    else:
        day = np.datetime64(event.get('date'))
        source_1 = event.get('source_1')
        source_2 = event.get('source_2', source_1)
        daily_file_version = event.get('df_version')
        
        if not all([source_1, source_2, day]):
            raise AttributeError(f'Missing job parameters: {daily_file_version = },{source_1 = },{source_2 = },{day = }')
        compute_crossovers(day, source_1, source_2, daily_file_version)