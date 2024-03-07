from prefect import flow, task, get_run_logger, pause_flow_run
from prefect.artifacts import create_table_artifact
from prefect.input import RunInput
from enum import Enum
from typing import Deque, List, Optional, Tuple
import requests




from pydantic import BaseModel


class Name(BaseModel):
    title: str
    first: str
    last: str


class Street(BaseModel):
    number: int
    name: str


class Coordinates(BaseModel):
    latitude: str
    longitude: str


class Timezone(BaseModel):
    offset: str
    description: str


class Location(BaseModel):
    street: Street
    city: str
    state: str
    country: str
    postcode: int
    coordinates: Coordinates
    timezone: Timezone


class Login(BaseModel):
    uuid: str
    username: str
    password: str
    salt: str
    md5: str
    sha1: str
    sha256: str


class Dob(BaseModel):
    date: str
    age: int


class Registered(BaseModel):
    date: str
    age: int


class Id(BaseModel):
    name: str
    value: str


class Picture(BaseModel):
    large: str
    medium: str
    thumbnail: str


class Result(BaseModel):
    gender: str
    name: Name
    location: Location
    email: str
    login: Login
    dob: Dob
    registered: Registered
    phone: str
    cell: str
    id: Id
    picture: Picture
    nat: str


class Info(BaseModel):
    seed: str
    results: int
    page: int
    version: str


class Model(BaseModel):
    results: List[Result]
    info: Info

class CleanedInput(RunInput):
    features_to_drop: List[str]


class UserInput(RunInput):
    number_of_rows: int

@task(name="Fetching URL", retries = 1, retry_delay_seconds = 5, retry_jitter_factor = 0.1)
def fetch(url: str):
    logger = get_run_logger()
    response = requests.get(url)
    raw_data = response.json()
    logger.info(f"Raw response: {raw_data}")
    return raw_data

# HIL: human input for which features to drop initially
@task(name="Cleaning Data")
def clean(raw_data: dict):
    results = raw_data.get('results')[0]
    logger = get_run_logger()
    logger.info(f"Cleaned results: {results.keys()}")
    l = ['name', 'gender', 'location', 'picture'] #'name', 'gender', 'location', 'city', 'country', 'state', 'coordinates'
    return map(results.get, l) #['gender']['city']['country']['state']['coordinates']['picture']

@flow()
def user_input_num_of_rows():
    description_md = f"""
    **Welcome to the User Greeting Flow!**

    Please enter your details below:
    How many rows of names would you like to create?
    """

    user_input = pause_flow_run(wait_for_input = UserInput.with_initial_data(description=description_md, number_of_rows=2))
    return user_input.number_of_rows


@flow()
def user_input_num_of_features(raw_data):
    description_md = f"""
    **Welcome to the User Greeting Flow!**
    Columns available: {raw_data.get('results')[0].keys()}
    Please enter your details below:
    Which columns would you like to drop?
    """

    user_input = pause_flow_run(wait_for_input = CleanedInput.with_initial_data(description=description_md, 
                                                                                features_to_drop=["email", "login", "dob", "registered", 
                                                                                                  "phone", "cell", "id", "nat"]))
    print(user_input.features_to_drop)
    print(type(user_input.features_to_drop))
    return user_input.features_to_drop

@flow(name="Create Names")
def create_names(num: int = 2):
    df = []
    url = "https://randomuser.me/api/"
    logger = get_run_logger()
    copy = num
    while num != 0:
        raw_data = fetch(url)
        df.append(clean(raw_data))
        num -= 1
    logger.info(f"create {copy} names: {df}")
    user_input_num_of_features(raw_data)

    return df

@flow(name="Redeploy Flow")
def redeploy_flow():
    logger = get_run_logger()
    user_input = pause_flow_run(wait_for_input = NameInput)
    if user_input.first_name != None:
        logger.info(f"User input: {user_input.first_name} {user_input.last_name}!")
    # raw type of user_input
    input = {'title': user_input.title.value, 
             'first': user_input.first_name, 
             'last': user_input.last_name}
    list_of_names.append(input)
    if(user_input.approve.value == 'Approve'):
        logger.info(f"Report approved! Creating artifact...")
        create_table_artifact(
            key="name-table",
            table=list_of_names,
            description = user_input.description
        )
    else:
        raise Exception("User did not approve")
    
    

if __name__ == "__main__":
    
    #num_of_rows = user_input_num_of_rows()
    ## HIL: human input for number of rows of names to create
    list_of_names = create_names(2)
    #list = Model.__fields__
    #print(list)
    #redeploy_flow()  