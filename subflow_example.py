from datetime import timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect import get_run_logger


@task
def get_name(firstname: str, lastname: str):
    logger = get_run_logger()
    logger.info("Received the user's name 📄")
    return f"{firstname} {lastname}"

@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def say_hi(user_name: str):
    logger = get_run_logger()
    logger.info("Caching results for 1 day 💾")
    return user_name

@flow
def hello_world(firstname: str, lastname: str):
    logger = get_run_logger()
    user = get_name(firstname, lastname)
    message = say_hi(user)
    logger.info("Hello %s! It is nice to meet you 😁", message)
    return message

@flow
def log_data_from_other_subflow(user_name: str):
    logger = get_run_logger()
    logger.info("Data received from other flow is: %s 🧑🏻‍💻", user_name)

@flow
def pass_data_between_subflows(firstname= 'Marvin', lastname ='Duck'):
    user = hello_world(firstname, lastname)
    log_data_from_other_subflow(user)

if __name__ == "__main__":
    pass_data_between_subflows()
