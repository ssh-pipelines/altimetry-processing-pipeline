# crossovers
# I'll do this later. Probably.

## Stuff to do
- build_file_list should also account for cloud files
- Parameters:
    - key1 (satellite or source, one of ['GSFC', 'S6', 'MERGED'])
    - date (this_day)
- Lines 492 in parallel_crossovers to handle where this is running
- Extract out main from parallel_crossovers into other script or repo
- Break out file I/O into new function to account for both local and cloud streaming
- Check line 443 for running in the cloud
- Update saving netcdf
    - need to account for local and cloud for where to save file and the uploading to bucket

- Look at requirements.txt versions