from enum import Enum
from typing import Optional
from prefect import flow, get_run_logger, pause_flow_run
from prefect.input import RunInput
import asyncio

class Overwrite_RAM(Enum):
    SMALL = "64gb"
    MEDIUM = "128gb"
    LARGE = "256gb"
    XLARGE = "512gb"

class Overwrite_Memory(Enum):
    SMALL = "64gb"
    MEDIUM = "128gb"
    LARGE = "256gb"
    XLARGE = "512gb"

class workpool(Enum):
    managed = "managed-execution"
    push_pool = "easy-push-pool"
    on_prem = "on-prem-k8s"

class InfraInput(RunInput):
    flow_name: str  # required, cannot be None
    description: Optional[str]  # required, can be None - same as str | None
    workpool_user_input: Optional[workpool]  # required, can be None - same as str | None
    f5: Overwrite_RAM = Overwrite_RAM.SMALL   # not required, but cannot be None
    memory: Overwrite_Memory = Overwrite_Memory.SMALL  # not required, but cannot be None


# use to_deployment to help redeploy the flow and .serve to serve the flow
@flow
async def redeploy_flow():
    logger = get_run_logger()

    user_input = await pause_flow_run(
        wait_for_input=InfraInput
    )

    if user_input.flow_name != None:
        logger.info(f"Redeploying with new work pool!")
        logger.info(f"New flow run with: {user_input.flow_name}!")

    # need to redeploy the flow with new inputs with to_deployment

asyncio.run(redeploy_flow())
