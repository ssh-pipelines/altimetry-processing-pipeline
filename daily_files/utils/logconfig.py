import logging
import os
from datetime import datetime


def configure_logging(file_timestamp: bool = True, log_level: str = 'INFO', testing: bool = False) -> None:
    logs_directory = '/tmp/logs'
    os.makedirs(logs_directory, exist_ok=True)
    log_filename = f'{datetime.now().isoformat() if file_timestamp else "log"}.log'
    logfile_path = os.path.join(logs_directory, log_filename)

    logging.root.handlers = []
    
    if testing:
        handlers = [logging.StreamHandler()]
        print(f'Logging to stream with level {logging.getLevelName(get_log_level(log_level))}')
    else:
        handlers=[
            logging.FileHandler(logfile_path),
            logging.StreamHandler()
        ]
        print(f'Logging to {logfile_path} with level {logging.getLevelName(get_log_level(log_level))}')

    logging.basicConfig(
        level=get_log_level(log_level),
        format='[%(levelname)s] %(asctime)s - %(message)s',
        handlers=handlers
    )


def get_log_level(log_level) -> int:
    """
    Defaults to logging.INFO
    :return:
    """

    value_map = {
        'INFO': logging.INFO,
        'DEBUG': logging.DEBUG,
        'WARNING': logging.WARNING,
        'WARN': logging.WARNING,
    }

    return value_map.get(log_level, logging.INFO)