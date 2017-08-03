# Main python file for model code
# 
# Default Environment Variables
#
# GOSPEL_DB_URL - The database url string for connecting to gospel
# DOCKER_ML_OUTPUT_SCHEMA - Standard schema to use for model results/metrics
#
# How To Add Additional environment variables
# 
# In the Airflow variables UI ($AIRFLOW_URL/admin/variable/) edit the
# DOCKERIZED_ML_TASKS variable, find the entry in the JSON list corresponding
# to your model, add a key "env" to the JSON object with a key matching your environment
# variable name. Ex.
#
# [
# ...
# {
#   "name": "demand-projections-high",
#    "git_repo": "git@github.com:/bellhops/demand-projections-1",
#      "env": {
#        "output_table": "demand_projections_low",
#        "projection_strategy": "low"
#      }
# },
# ...
# ]
#
# You can then access this model using this python snipet:
# CONSTANT = json.loads(os.environ.get('CONSTANT', <default>))
#

import os
import json
from sqlalchemy import create_engine

if __name__ == "__main__":
    gospel_conn = create_engine(os.environ['GOSPEL_DB_URL'])
    
    # Model code that needs to be executed goes here, what this "main" portion of the model is doing
    # should be as simple/intuitive as possible use functions/libraries to encapsulte other code
    print("Hello world")
