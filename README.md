# crossovers
# I'll do this later. Probably.

## Stuff to do
- TBD: Check on `window_length` attr. sat1 and sat2 can have variable windows if not self crossovers

- make s3_utils for:
    - getting list of file objects
    - stream all file objects
    - uploading xover file
- add unittesting using local granules (might have to refactor some things)

- Look at requirements.txt versions

- Look at how single message failures are handled in batch SQS

- Fix bug where init_and_fill_running_window finds no data and just returns - won't work downstream.