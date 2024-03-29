import requests
import marvin_extension as ai_functions
from prefect import flow, task, get_run_logger, pause_flow_run
from enum import Enum
from typing import List
from pydantic import Field
from prefect.artifacts import create_table_artifact
from prefect.blocks.system import JSON
from prefect.input import RunInput

DEFAULT_FEATURES_TO_DROP = [
    "email",
    "login",
    "dob",
    "registered",
    "phone",
    "cell",
    "id",
    "nat",
]


class CleanedInput(RunInput):
    features_to_drop: list[str]


class UserInput(RunInput):
    number_of_users: int


class CreateArtifact(RunInput):
    create_artifact: bool = Field(description="Would you like to create an artifact?")


@flow()
def user_input_remove_features(url: str):
    raw_data = fetch(url)

    features = "\n".join(raw_data.get("results")[0].keys())

    description_md = (
        "## Features available:"
        f"\n```json{features}\n```\n"
        "Which columns would you like to drop?"
    )

    user_input = pause_flow_run(
        wait_for_input=CleanedInput.with_initial_data(
            description=description_md, features_to_drop=DEFAULT_FEATURES_TO_DROP
        )
    )
    return user_input.features_to_drop


# create block of json for the cleaned data
@task(name="Fetching URL", retries=1, retry_delay_seconds=5, retry_jitter_factor=0.1)
def fetch(url: str):
    logger = get_run_logger()
    response = requests.get(url)
    raw_data = response.json()
    logger.info(f"Raw response: {raw_data}")
    return raw_data


# HIL: human input for which features to drop initially
@task(name="Cleaning Data")
def clean(raw_data: dict, features_to_drop: list[str]):
    results = raw_data.get("results")[0]
    logger = get_run_logger()
    keysList = list(results.keys())
    logger.info(f"Columns available: {keysList}")
    z = list(set(keysList) - set(features_to_drop))
    logger.info(f"Features to drop: {features_to_drop}")
    # l = ['name', 'gender', 'location', 'picture']
    return list(map(results.get, z))


@flow(name="Create Artifact")
def create_artifact():
    description_md = f"""
    Information pulled: {JSON.load("all-users-json")}
    Would you like to create an artifact?
    """
    logger = get_run_logger()
    create_artifact = pause_flow_run(
        wait_for_input=CreateArtifact.with_initial_data(
            description=description_md, create_artifact=False
        )
    )
    print(type(JSON.load("all-users-json")))
    if create_artifact.create_artifact == True:
        logger.info("Report approved! Creating artifact...")
        create_table_artifact(key="name-table", table=JSON.load("all-users-json").value)
    else:
        raise Exception("User did not approve")


@flow(name="Create Names")
def create_names():
    description_md = """
    How many users would you like to create?
    """
    user_input = pause_flow_run(
        wait_for_input=UserInput.with_initial_data(
            description=description_md, number_of_users=2
        )
    )
    num_of_rows = user_input.number_of_users
    df = []
    url = "https://randomuser.me/api/"
    logger = get_run_logger()
    copy = num_of_rows
    features_to_drop = user_input_remove_features(url)
    while num_of_rows != 0:
        raw_data = fetch(url)
        df.append(clean(raw_data, features_to_drop))
        num_of_rows -= 1
    logger.info(f"create {copy} names: {df}")
    JSON(value=df).save("all-users-json", overwrite=True)


if __name__ == "__main__":

    ## HIL: human input for number of rows of names to create
    list_of_names = create_names()
    create_artifact()
    ai_functions.extract_information()
