# Bad Pass Flagging

Note: Does not currently support running for date coverage longer than 3 years due to runtime. In order to reprocess all of GSFC for example you must submit multiple 3 year invocations. It is currently directly invoked but will be redeployed to better support handling of longer date ranges rendering the above unneccesary.

Inputs:

Optional GSFC:
- gsfc_start: "YYYY-MM-DD"
- gsfc_end: "YYYY-MM-DD"

Default S6:
- s6_start = "YYYY-MM-DD" (defualts to 60 days prior to date of runtime)
- s6_end = "YYYY-MM-DD" (defualts to date of runtime)