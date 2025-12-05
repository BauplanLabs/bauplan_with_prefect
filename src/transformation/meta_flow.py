"""

A minimal Prefect flow that orchestrates a Bauplan ETL pipeline.

This script demonstrates the integration between Prefect and Bauplan:
* Create a data branch for isolation
* Import source data into an Iceberg table
* Run the Bauplan transformation pipeline
* Verify the output table has data
* Merge and cleanup the branch on success

Usage:
    python meta_flow.py --branch_name <branch_name>

"""

import os
from datetime import datetime
import bauplan
from dotenv import load_dotenv
from prefect import flow, task


@task
def create_branch(client: bauplan.Client, branch_name: str) -> bool:
    """Create a new branch for our pipeline work."""
    # cleanup if branch exists (for demo purposes)
    if client.has_branch(branch_name):
        print(f"Branch {branch_name} exists, deleting it first...")
        client.delete_branch(branch_name)

    print(f"Creating branch {branch_name} from main...")
    client.create_branch(branch_name, from_ref="main")
    assert client.has_branch(branch_name), "Branch creation failed"

    return True


@task
def import_source_data(
    client: bauplan.Client,
    branch_name: str,
    table_name: str,
    s3_uri: str
) -> bool:
    """Create and import the source table into the branch."""
    print(f"Creating table {table_name} from {s3_uri}...")
    client.create_table(
        table=table_name,
        search_uri=s3_uri,
        branch=branch_name,
        replace=True,
    )

    print(f"Importing data into {table_name}...")
    client.import_data(
        table=table_name,
        search_uri=s3_uri,
        branch=branch_name,
    )

    return True


@task
def run_pipeline(client: bauplan.Client, branch_name: str, project_dir: str) -> bool:
    """Run the Bauplan pipeline in the given directory."""
    print(f"Running pipeline in {project_dir} on branch {branch_name}...")
    run_state = client.run(
        project_dir=project_dir,
        ref=branch_name
    )
    print(f"Bauplan run completed: {run_state.job_id, run_state.job_status}")
    assert run_state.job_status.lower() == 'success' and run_state.job_id is not None

    return True


@task
def verify_output(client: bauplan.Client, branch_name: str, table_name: str) -> bool:
    """Check that the output table has data."""
    print(f"Verifying table {table_name} has data...")
    result = client.query(
        query=f"SELECT COUNT(*) as cnt FROM {table_name}",
        ref=branch_name,
        namespace="bauplan",
    )
    count = result.to_pydict()["cnt"][0]
    print(f"Table {table_name} has {count} rows")
    assert count > 0, f"Table {table_name} is empty!"

    return True


@task
def merge_and_cleanup(client: bauplan.Client, branch_name: str) -> bool:
    """Merge the branch into main and delete it."""
    print(f"Merging {branch_name} into main...")
    client.merge_branch(source_ref=branch_name, into_branch="main")

    print(f"Deleting branch {branch_name}...")
    client.delete_branch(branch_name)

    return True


@flow(log_prints=True)
def transformation_flow(branch_name: str):
    """
    Run the full ETL pipeline:
    1. Create a branch
    2. Import source data
    3. Run transformations
    4. Verify output
    5. Merge and cleanup
    """
    print(f"Starting transformation flow at {datetime.now()}")

    # load env vars
    load_dotenv()
    s3_uri = os.environ["TAXI_ZONE_LOOKUP_FILE"]
    # init client
    client = bauplan.Client()
    user_name = client.info().user.username
    assert branch_name.startswith(
        user_name
    ), f"Branch name must start with your username: {user_name}"

    # get the pipeline directory (relative to this script)
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.join(script_dir, "bpln_pipeline")

    # run the flow
    create_branch(client, branch_name)
    import_source_data(client, branch_name, "taxi_metadata", s3_uri)
    run_pipeline(client, branch_name, project_dir)
    verify_output(client, branch_name, "my_child")
    merge_and_cleanup(client, branch_name)

    print(f"All done at {datetime.now()}, see you, space cowboy.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--branch_name",
        type=str,
        required=True,
        help="Branch name (should start with your username)",
    )
    args = parser.parse_args()

    transformation_flow(branch_name=args.branch_name)
