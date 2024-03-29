import prefect
from prefect import task, flow
from prefect.triggers import all_successful


# {
#   "type": "compound",
#   "require": "all",
#   "within": 3600,
#   "triggers": [
#     {
#       "type": "event",
#       "posture": "Reactive",
#       "expect": ["prefect.flow-run.Running"],
#       "match_related": {
#       }
#     },
#     {
#       "type": "event",
#       "posture": "Reactive",
#       "expect": ["prefect.block.json.load.called"],
#       "match_related": {
#         "prefect.resource.name": "all-users-json"
#       }
#     },
#     {
#       "type": "event",
#       "posture": "Reactive",
#       "expect": ["prefect.flow-run.Failed"],
#       "match_related": {
#       }
#     }
#   ]
# }






## wip on flow usecase

@task
def process_data(data):
    # Process the data here
    print(f"Processing data: {data}")

@task(trigger=all_successful)
def analyze_data(data):
    # Analyze the processed data here
    print(f"Analyzing data: {data}")

@flow
def event_driven_workflow(data):
    data = prefect.Parameter("data")
    processed_data = process_data(data)
    analyze_data(processed_data)

event_driven_workflow()