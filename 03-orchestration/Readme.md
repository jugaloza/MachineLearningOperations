
<h2> 03 - Workflow Orchestration </h2>
Workflow orchestration is a set of tools that is used to automate training pipeline by scheduling and also monitoring the work in order to achieve the bussiness metrics

After defining pipeline(example below) there are chances of failure in any of the stage, so the goal of workflow orchestration is to minimize the errors and fail gracefully.
 
Data Preprocessing ---> Model Training ---> Model logging ---> Model Deployment
                                                                |
                                                                -> either using flask or REST API
                                                                
<h3> Negative Engineering </h3>
Most of the 90% of engineering time is spent on 
<ul>
  <li> Retries when APIs go down </li>
  <li> Malformed data </li>
  <li> Notifications </li>
  <li> Observability into failure </li>
  <li> Conditional Failure logic </li>
  <li> Timeouts </li>
</ul>

So, to overcome this time spent on negative engineering, prefect tool is used to increase the productivity and reduces time spent on negative engineering

<h3> Prefect </h3>

Open Source workflow orchestration for reducing time spent on Negative Engineering


