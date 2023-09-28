# Lesson

## Brief

### Preparation

We will be using the `elt` and `dwh` environments for this lesson. We will also be using google cloud (for which the account was created in the previous units) in this lesson.

### Lesson Overview

In this lesson, we will continue with orchestration from where we left off in the previous unit. We will use `Dagster` to orchestrate and schedule the `Meltano` and `Dbt` pipelines.

We will also dive into the concept of data testing, which is an important part of data quality management. We will use `Great Expectations`, an open-source library for data testing.

---

## Part 1 - Hands-on with Orchestration II

In the previous unit, combining Meltano and Dbt, we have an end-to-end ELT (data ingestion and transformation) pipeline. However, we ran the pipelines manually. Now, we will use Dagster to orchestrate the pipelines and schedule them to run periodically.

### Background

We can orchestrate Meltano and Dbt pipelines using Dagster. By executing the commands from within Dagster, we get to take full advantage of its capabilities such as scheduling, dependency management, end-to-end testing, partitioning and more.

![dagster](assets/dagster_meltano.png)

### Create a Dagster Project

First, activate the conda environment.

```bash
conda activate elt
```

We will create a Dagster project and use it to orchestrate the Meltano pipelines.

```bash
dagster project scaffold --name meltano-orchestration
```

Add the `dagster-meltano` library as a required install item in the Dagster project `setup.py`. Then run the following:

```bash
pip install -e ".[dev]"
dagster dev
```

### Using the Dagster-Meltano library

Replace the content of `meltano-orchestration/meltano_orchestration/__init__.py` with the following:

```python
from dagster import Definitions, ScheduleDefinition, job
from dagster_meltano import meltano_resource, meltano_run_op


@job(resource_defs={"meltano": meltano_resource})
def run_elt_job():
   tap_done = meltano_run_op("tap-github target-bigquery")()

# Addition: a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
elt_schedule = ScheduleDefinition(
    job=run_elt_job,
    cron_schedule="0 0 * * *",  # every day at midnight
)

defs = Definitions(
    schedules=[elt_schedule],
)
```

### Launching a Test Run of the Schedule

Refresh the Dagster project, look for the 'Launchpad' tab after clicking on the job name in the left nav.

When initiating a run in Dagster, we have to pass along configuration variables at run time such as the location of the Meltano project:

```yml
resources:
  meltano:
    config:
      project_dir: #full-path-to-the-meltano-ingestion-project-directory
ops:
  tap_github_target_bigquery:
    config:
      env:
        TAP_GITHUB_AUTH_TOKEN: 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
```

Then click 'Launch run'. We have just executed the `meltano run tap-github target-bigquery` command from within Dagster.

### Using Dbt with Dagster

We can also orchestrate Dbt with Dagster.

First, activate the conda environment.

```bash
conda activate dwh
```

Create a file named `profiles.yml` in the `resale_flat` dbt project directory in Unit 2.6 with the following content:

```yml
resale_flat:
  outputs:
    dev:
      dataset: resale_flat
      job_execution_timeout_seconds: 300
      job_retries: 1
      keyfile: #full-path-to-the-service-account-key-file
      location: US
      method: service-account
      priority: interactive
      project: meltano-learn
      threads: 1
      type: bigquery
  target: dev
```

Then create a new Dagster project that points to the directory.

```bash
dagster-dbt project scaffold --project-name resale_flat_dagster --dbt-project-dir #full-path-to-the-resale-flat-dbt-project-directory
```

To run the dagster webserver:

```bash
cd resale_flat_dagster
DAGSTER_DBT_PARSE_PROJECT_ON_LOAD=1 dagster dev
```

We can now trigger the Dbt pipeline from within Dagster by selecting the assets and clicking 'Materialize selected'.

We can even schedule the pipeline to run daily by uncommenting the code in `resale_flat_dagster/resale_flat_dagster/schedules.py`. Now click 'Reload definitions' and you will see the new schedule.

## Part 2 - Hands-on with Data Testing (Great Expectations)

### Background

Data testing is an important part of data quality management. It is the process of verifying that data satisfies the expected properties. It is also known as data validation, data quality assurance, data quality control, data quality assessment, etc.

Great Expectations is an open-source library for data testing. It is a Python library that helps you write, organize, evaluate, and share your data validation. It also provides a user interface for visualizing the results of the data tests. It can be used with any data source such as files, databases, data lakes, etc.

![great_expectations](assets/great_expectation.png)

### Create a Great Expectations Project (Data Context)

First, reactivate the conda environment.

```bash
conda activate elt
```

A data context is a directory that contains all the configuration and metadata for a Great Expectations project. It is created by running the following command:

```bash
great_expectations init
```

### Create a Datasource

In Great Expectations, Datasources simplify connections to data by managing and providing a consistent, cross-platform API.

Let's create and configure our first Datasource: a connection to the csv file `data/resale_flat.csv`

```bash
great_expectations datasource new
```

Select `1` for the type of Datasource and `1` for the Processing engine. Then enter the path to the `data` directory. Finally, a jupyter notebook will be launched to help you configure the Datasource.

- Change the `datasource_name` to `'resale_flat'`

Then execute all cells in the notebook. Based on that information, the CLI added some entries into your `great_expectations.yml` file, under the datasources header.

### Create an Expectation Suite

Expectations are the workhorse abstraction in Great Expectations. Each Expectation is a declarative, machine-verifiable assertion about the expected format, content, or behavior of your data. Suites are simply collections of Expectations.

```bash
great_expectations suite new
```

Select `3` as we will be using a Data Assistant to populate the Expectation Suite. Then you will be presented with 2 options of data asset:

- resale_flat_201701_202306.csv
- resale_flat_202307.csv

`resale_flat_201701_202306` contains the resale flat data from January 2017 to June 2023. Since we want to build an Expectation Suite based on what we know about the data from January 2017 to June 2023, we want to use it for profiling.

`resale_flat_202307` contains the July 2023 (assumee to be the _latest_) data, which we consider the “new” data set that we want to validate before using in production.

Choose the first file `resale_flat_201701_202306` as the data asset.

Name the Expectation Suite `resale_flat.validation`. Then, a jupyter notebook will be launched to help you configure the Expectation Suite.

- The second cell allows you to specify which columns you want to ignore when creating Expectations. We want our Data Assistant to examine the `floor_area_sqm` and `resale_price` columns and determine just what a reasonable range is based on our _past_ data. Let’s comment the 2 columns to include them.

Then execute all cells in the notebook. Once the Data Assistant is done executing it will open up Data Docs in your browser automatically. Data Docs translate Expectations, Validation Results, and other metadata into clean, human-readable documentation.

Take a look at the Expectations that were created for the 2 field. These are the rules that we will be comparing the _latest_ data (July 2023) against.

### Set up a Checkpoint

A Checkpoint runs an Expectation Suite against a Batch (a selection of records from a data asset). Running a Checkpoint produces Validation Results.

```bash
great_expectations checkpoint new resale_flat_checkpoint
```

This will again launch a jupyter notebook to help you configure the Checkpoint.

- The second code cell is pre-populated with an arbitrarily chosen Batch Request and Expectation Suite to get you started. Edit the data_asset_name to reference the data we want to validate: `resale_flat_202307.csv`.

Then execute all cells in the notebook in order to store the Checkpoint to your Data Context.

### Inspect the Validation Results

In order to build Data Docs and get your results in a nice, human-readable format, you can simply uncomment and run the last cell in the notebook. This will open Data Docs, where you can click on the latest Validation run to see the Validation Results page for this Checkpoint run.

Some of the failed expectations might not be sensible. You can always edit the Expectation Suite by:

```bash
great_expectations suite edit resale_flat.validation
```

### Connecting to a Database

We can also connect to a database as a Datasource. Let's create and configure a Datasource for the `resale_flat_prices_2020` table hosted on the Postgres database.

```bash
great_expectations datasource new
```

Select `2` for the type of Datasource and `2` for Postgres. Again, a jupyter notebook will be launched to help you configure the Datasource.

- Change the `datasource_name` to `'resale_flat_postgres'`

Set the following connection parameters:

- host = `"db.kjytsuhjlrmjodturbcb.supabase.co"`
- port = `"5432"`
- username = `"su_user"`
- password = `"MP8EtwVgM7w"`
- database = `"postgres"`
- schema_name = `"public"`
- table_name = `"resale_flat_prices_2020"`

Then execute all cells in the notebook. Based on that information, the CLI added some entries into your `great_expectations.yml` file, under the datasources header.

Then, create a new expectation suite for the `resale_flat_postgres` datasource:

```bash
great_expectations suite new
```

Select `3` as we will be using a Data Assistant to populate the Expectation Suite. Select `resale_flat_postgres` as the datasource and `default_inferred_data_connector_name` for data_connector. Then, select `public.resale_flat_prices_2020` as the data asset. Finally, name the suite `resale_flat_postgres.validation`. Then, a jupyter notebook will be launched to help you configure the Expectation Suite.

- Again, the second cell allows you to specify which columns you want to ignore when creating Expectations. We want our Data Assistant to examine the `floor_area_sqm` and `resale_price` columns and determine just what a reasonable range is based on our _past_ data. Let’s comment the 2 columns to include them.

Then execute all cells in the notebook. Once the Data Assistant is done executing it will open up Data Docs in your browser automatically. Data Docs translate Expectations, Validation Results, and other metadata into clean, human-readable documentation.

Now, let's define a checkpoint against the `resale_flat_prices_2021` table. Here, we are validating the expectations generated from `2020` against the `2021` data.

```bash
great_expectations checkpoint new resale_flat_postgres_checkpoint
```

This will again launch a jupyter notebook to help you configure the Checkpoint.

- The second code cell is pre-populated with an arbitrarily chosen Batch Request and Expectation Suite to get you started. Edit the data_asset_name to reference the data we want to validate: `public.resale_flat_prices_2021`. Also, change the `expectation_suite_name` to `resale_flat_postgres.validation`.

Execute all cells in the notebook in order to store the Checkpoint to your Data Context.

Again, uncomment and run the last cell in the notebook. This will open Data Docs, where you can click on the latest Validation run to see the Validation Results page for this Checkpoint run.

You can again edit the Expectation Suite by:

```bash
great_expectations suite edit resale_flat_postgres.validation
```

## Part 3 - Testing Dbt

Back in unit 2.5, we configured some simple tests in dbt to check for _null values_, _uniqueness_ and _foreign key constraints_. We have copied the dbt project `liquor_sales` from unit 2.5 to this unit. You can find the tests in the `schema.yml` files in the `/models` directory.

However, the built-in tests are limited in scope and functionality. We can expand on the tests using `dbt_utils`- a utility macros package for dbt and `dbt-expectations`- an extension package for dbt inspired by Great Expectations to write more comprehensive tests.

### Installing and Configuring `dbt_utils`

First, activate the conda environment.

```bash
conda activate dwh
```

Create a new `packages.yml` file:

```yml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

Run `dbt deps` to install the package. Refer to the [documentation](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/) for supported tests.

Let's add some additional tests to the `fact_sales` model at the end of `models/schema.yml`:

```yml
- name: date
  tests:
    - dbt_utils.accepted_range:
        min_value: "PARSE_DATE('%F', '2012-01-01')"
        max_value: "CURRENT_DATE()"
```

Here, we are using `dbt_utils.accepted_range` to check if the `date` field is within the range of `2012-01-01` and `CURRENT_DATE()`.

We can also add the `dbt_utils.expression_is_true` test to check if the `sale_dollars` field is the product of `bottles_sold` and `state_bottle_retail`:

```yml
tests:
  - dbt_utils.expression_is_true:
      expression: "ROUND(sale_dollars, 1) = TRUNC(bottles_sold * state_bottle_retail, 1)"
```

Here, we use `ROUND` to round the values to 1 decimal place and compare them.

Run the tests using `dbt test`. Observe which tests pass and which fail.

> 1. Run a SQL query to check which rows failed.
> 2. Run a SQL query to get the min and max values of `pack` and `bottle_volume_ml` in `liquor_sales_star.dim_item`.
> 3. Then, set the `min_value` and `max_value` in the `dbt_utils.accepted_range` test in `models/star/schema.yml` to the min and max values respectively.

### Installing and Configuring `dbt-expectations`

Add the following to `packages.yml`:

```yml
- package: calogica/dbt_expectations
  version: 0.10.0
```

Run `dbt deps` to install the package. Refer to the [documentation](https://hub.getdbt.com/calogica/dbt_expectations/latest/) for supported tests.

Let's add some tests to check the column types in `fact_sales`:

```yml
- name: invoice_and_item_number
  tests:
    - dbt_expectations.expect_column_values_to_be_of_type:
      column_type: string
- name: date
  tests:
    - dbt_expectations.expect_column_values_to_be_of_type:
      column_type: date
```

> 1. Add type tests for all the columns in `fact_sales`, `dim_item` and `dim_store`.
> 2. Can you think of any other tests that we can add to the models?
