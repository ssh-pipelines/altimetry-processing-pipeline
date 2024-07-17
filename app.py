import json
import logging
from typing import Tuple
import numpy as np
from crossover.parallel_crossovers import CrossoverProcessor

def parse_params(params: dict) -> Tuple[np.datetime64, str, str]:
    try:
        date = np.datetime64(params.get('date'))
    except:
        raise ValueError(f'Unable to parse date.')
    source = params.get('source')
    df_version = params.get('df_version')
    
    if None in [date, source, df_version]:
        raise ValueError(f'Missing job parameters: {df_version = },{source = },{date = }')
    return date, source, df_version

def handler(event, context):
    logging.root.handlers = []
    logging.basicConfig(
        level='INFO',
        format='[%(levelname)s] %(asctime)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    if 'Records' in event:
        for record in event['Records']:
            message_body = json.loads(record['body'])
            try:
                date, source, daily_file_version = parse_params(message_body)
            except ValueError as e:
                logging.error(e)
                continue
            processor = CrossoverProcessor(date, source, daily_file_version)
            processor.run()
    else:
        date, source, daily_file_version = parse_params(event)
        processor = CrossoverProcessor(date, source, daily_file_version)
        processor.run()