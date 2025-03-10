from dagster import asset, AssetExecutionContext
import pandas as pd
import os
import subprocess


@asset
def pipeline_dbt_seed()->None:
    """
    Runs dbt seed 
    """
    cmd = ["dbt", "seed"]
    try:
        output= subprocess.check_output(cmd,stderr=subprocess.STDOUT).decode()
    except subprocess.CalledProcessError as e:
            output = e.output.decode()
            raise Exception(output)

@asset(deps=[pipeline_dbt_seed])
def pipeline_dbt_run()->None:
    """
    Runs dbt run 
    """
    cmd = ["dbt", "run"]
    try:
        output= subprocess.check_output(cmd,stderr=subprocess.STDOUT).decode()
    except subprocess.CalledProcessError as e:
            output = e.output.decode()
            raise Exception(output)

@asset(deps=[pipeline_dbt_run])
def pipeline_dbt_test()->None:
    """
    Runs dbt test 
    """
    cmd = ["dbt", "test"]
    try:
        output= subprocess.check_output(cmd,stderr=subprocess.STDOUT).decode()
    except subprocess.CalledProcessError as e:
            output = e.output.decode()
            raise Exception(output)    
