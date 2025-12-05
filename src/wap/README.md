# WAP with Bauplan and Prefect
A reference implementation of write-audit-publish (WAP) with Bauplan and Prefect 3.0

## Overview

A common need on S3-backed analytics systems (e.g. a data lakehouse) is safely ingesting new data into tables available to downstream consumers. 

![WAP](../../img/wap.jpg)

Due to their distributed nature and large quantity of data to be bulk-inserted, a lakehouse ingestion is more delicate than the equivalent operation on a traditional database.

Data engineering best practices suggest the Write-Audit-Publish (WAP) pattern, which consists of three main logical steps:

* Write: ingest data into a ''staging'' / ''temporary'' section of the lakehouse - the data is not visible yet to downstream consumers;
* Audit: run quality checks on the data, to verify integrity and quality (avoid the ''garbage in, garbage out'' problem);
* Publish: if the quality checks succeed, proceed to publish the data to the main section of the lakehouse - the data is now visible to downstream consumers; otherwise, raise an error / clean-up etc.

This repository showcases how [Prefect](https://www.prefect.io/) and [Bauplan](https://www.bauplanlabs.com/) can be used to implement WAP in ~150 lines of no-nonsense pure Python code: no knowledge of the JVM, SQL or Iceberg is required.  

In particular, we will leverage [Prefect transactions](https://docs-3.prefect.io/3.0rc/develop/transactions#write-your-first-transaction) as the ''outer layer'' for safe handling of the relevant _tasks_, and [Bauplan transactions (through branches)](https://docs.bauplanlabs.com/en/latest/tutorial/02_catalog.html) as the ''inner layer'' for safe handling of the relevant _data assets_:

* For a longer discussion on the context behind the project and the trade-offs involved, please refer to our [blog post](https://www.prefect.io/blog/prefect-on-the-lakehouse-write-audit-publish-pattern-with-bauplan).
* To get a quick feeling on the developer experience, check out this [demo video](https://www.loom.com/share/0387703f204e4b3680b1cb14302a04da?sid=536f3a9f-c590-4548-a3c2-b5861b8c17c0).

![Lakhouse flow](../../img/flow.jpg)

## Setup

Make sure to have run `uv sync` in the root of the repo to install dependencies and set up your Bauplan key for the public sandbox.

### Dataset

The current data checks assume you are ingesting the titanic dataset, as for example available [here](https://raw.githubusercontent.com/datasciencedojo/datasets/refs/heads/master/titanic.csv). Make sure to load the csv in a S3 bucket with public read and list access, so that the Bauplan sandbox can access it. If you want to use a different dataset, please adjust the column name in the `wap_flow.py` file accordingly.

## Run the flow

Start a local Prefect server and take note of the URL:

```bash
uv run prefect server start
```

Then, in a separate terminal, set up the connection and run the flow:

```bash
uv run prefect config set PREFECT_API_URL=http://127.0.0.1:4200/api
uv run wap_flow.py --table_name <table_name> --branch_name <branch_name> --s3_path s3://a-public-bucket/your-data.csv
```

This is a [video demonstration](https://www.loom.com/share/0387703f204e4b3680b1cb14302a04da?sid=536f3a9f-c590-4548-a3c2-b5861b8c17c0) of the flow in action, both in case of successful audit and in case of failure.

Through the Prefect server, you can visualize the flow in the UI, e.g. you can check the latest run:

![prefect UI](../../img/UI.png)

## License

The code in the project is licensed under the MIT License (Prefect and Bauplan are owned by their respective owners and have their own licenses). 
