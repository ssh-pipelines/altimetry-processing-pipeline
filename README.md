# Orbit Error Reduction

## WORK IN PROGRESS

1. OER polygon creation
    - Polygon for each crossover file
        - Needs all crossovers from today and all crossovers from previous ten days
    - Fitting a polynomial to each day’s worth of crossovers
    - Time is converted to hours for polynomial…hours since start of daily file 
    - Saves numbers into a parallel polygon file for daily file with same date

2. OER evaluation at each timestep in original daily file using polygon(s)
    - Creates OER variable in meters, something added to SSH and SSH_SMOOTHED to correct for orbit error

3. Save corrected SSH