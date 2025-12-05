# ETL with Bauplan and scheduling with Prefect


## Overview

A small reference implementation of data transformations using Bauplan models, with Prefect for orchestration and scheduling and Streamlit for visualization.

## Setup

Make sure to have run `uv sync` in the root of the repo to install dependencies and set up your Bauplan key for the public sandbox.

### Dataset

We will be working with the [taxi metadata csv file](https://www.kaggle.com/datasets/usmanshams/nyc-yellow-taxi-dataset-2024/), which we will join with the existing `taxi_fhvhv` table in the Bauplan public sandbox. Make sure to load the csv in a S3 bucket with public read and list access, so that the Bauplan sandbox can access it.

## The development loop

First, we create a [data branch](https://docs.bauplanlabs.com/tutorial/data_branches) for our work:

```bash
bauplan branch create <user_name>.taxi_zones_prefect
bauplan branch checkout <user_name>.taxi_zones_prefect
```

We then create a table import a file there with taxi zone metadata:

```bash
bauplan table create --name taxi_metadata --search-uri s3://<bucket>/taxi_zone_lookup.csv 
bauplan table import --name taxi_metadata --search-uri s3://<bucket>/taxi_zone_lookup.csv 
``` 

Finally, we can run the pipeline:

```bash
cd bpln_pipeline
bauplan run
```

And verify we have data in the child table just created by the pipeline:

```bash
bauplan query "SELECT COUNT(*) FROM my_child"
```

Finaly, we can open the streamlit app to visualize the results in a nice dashboard:

```bash
uv run streamlit run app.py
```

Et voilá - the entire data transformation loop with Bauplan is complete, and it's all Python!

## Productionizing with Prefect

Start a local Prefect server and take note of the URL:

```bash
uv run prefect server start
```

Then, in a separate terminal, set up the connection and run the flow:

```bash
uv run prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
uv run meta_flow.py --branch_name <branch_name>
```

## License

The code in the project is licensed under the MIT License (Prefect and Bauplan are owned by their respective owners and have their own licenses). 
