from prefect import task, flow
from prefect import get_run_logger
from subflow_example import pass_data_between_subflows

if __name__ == "__main__":
    pass_data_between_subflows.from_source(
        source="https://github.com/Sahiler/demo-examples.git",
        entrypoint="subflow-example.py:pass_data_between_subflows",
    ).deploy(
        name="test-push-pool-flow",
        work_pool_name="easy-push-pool",
    )