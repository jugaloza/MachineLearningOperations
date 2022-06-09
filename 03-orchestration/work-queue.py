from prefect import flow
from prefect.deployments import DeploymentSpec
from prefect.orion.schemas.schedules import IntervalSchedule
from datetime import timedelta
from prefect.flow_runners import SubprocessFlowRunner

@flow
def flow_example():
    print('Flow example')

DeploymentSpec(flow=flow_example,
               name="staging-experiment",
               schedule=IntervalSchedule(interval=timedelta(minutes=5)),
               flow_runner=SubprocessFlowRunner(),
               tags=['dev'])

DeploymentSpec(flow=flow_example,
               name='production-experiment',
               schedule=IntervalSchedule(interval=timedelta(minutes=5)),
               flow_runner=SubprocessFlowRunner(),
               tags=['prod'])

