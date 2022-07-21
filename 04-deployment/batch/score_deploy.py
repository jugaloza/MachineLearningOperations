from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import CronSchedule
from prefect.flow_runners import SubprocessflowRunner


DeploymentSpec(
    flow_location="score.py",
    name="ride_duration_prediction",
    parameters={
        "taxi_type": "green",
        "run_id" : "8cf38dfad1a24ceb9b58de03cc679dd9"
    },
    schedule=CronSchedule(cron="0 3 2 * *"),
    flow_runner=SubprocessflowRunner(),
    tags=["ml"],
    flow_storage="6hcf38df-ad1a-24ce-bcb5-8de03cc677de3"

)