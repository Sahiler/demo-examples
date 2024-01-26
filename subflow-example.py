from prefect import task, flow
from prefect import get_run_logger

@task
def get_name(firstname= 'Marvin', lastname ='Duck'):
    return f"{firstname} {lastname}"

@task
def say_hi(user_name: str):
    logger = get_run_logger()
    logger.info("Hello %s!", user_name)

@flow
def hello_world():
    user = get_name()
    say_hi(user)
    return user

@flow
def log_data_from_other_subflow(user_name: str):
    logger = get_run_logger()
    logger.info("Data received from other flow is: %s", user_name)

@flow
def pass_data_between_subflows():
    user = hello_world()
    log_data_from_other_subflow(user)

if __name__ == "__main__":
    pass_data_between_subflows()
