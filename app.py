from daily_files import daily_file_generation

def handler(event, context):
    daily_file_generation.main(event)