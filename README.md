# ALTIMETRY PROCESSING PIPELINE

Monorepo for the independent stages of the altimetry processing pipeline used to generate NASA SSH products. 

Pipeline consists of:

### Daily along track generation

Daily files -> Crossover -> OER -> Crossover -> Bad pass flagging -> Finalization

### Additional product generation

Simple grids -> Indicators -> Imagery for website

```
                  GSFC Data     S6 Data
                      |            |
                      +------------+
                            |
                            v
                [ Generate Daily Files ]
                            |   
                            v   
                [ Generate Simple Grids ]
                            |  
                            v  
             +--------------+--------------+ 
             |                             | 
             v                             v 
[ Generate ENSO Maps & Imagery]  [ Generate Indicators ]
```

## Description

The directories contained within `pipeline/` contain everything for a given stage in the pipeline (note: stage as opposed to step, as the crossover stage is used for two steps). Each stage's directory can be thought of as an independent repository for that stage.

Each stage has been designed to be run on AWS Lambda, and as such there are limitations to how much can be executed locally.

## Setup

### Installing `utilities`:
Root level `utilities` module is required to be installed in order to successfully execute containers and unit tests.

From the root directory:
```
python -m pip install .
```

### Building images:

Images must be built from the root directory context

ex:
```
docker buildx build --platform linux/amd64 --load -t daily_files:latest -f path/to/nasa-ssh-pipeline/pipeline/daily_files/Dockerfile {path/to/nasa-ssh-pipeline/}
```