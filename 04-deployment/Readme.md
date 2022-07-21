<h1> 04. Model Deployment </h1>

What is Model Deployment?

After running the experiments and selecting the best model, now it is the time to productionize the  model i.e the model which will be use by the customers,
so that is what comes under the model deployment.

<h2> Options for model deployment </h2>

![Alt text](model_deployment_options.png?raw=true)

There are multiple options for model deployment phase.

First we need to figure out when we want predictions, continous predictions or predictions after some time let say after minute, hours, two hours or after days.



<ol>
  <li> Batch Mode </li>
  <p> Run the model regularly </p>
  
  Best example: Marketing(churn predictions)
  Every month some companies need to know about the customers for getting churn analysis
  
  <li> Online Mode </li>  