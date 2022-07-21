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

![Alt text](Batch_Mode.png?raw=true)

Best example: Marketing(churn predictions)
Every month some companies need to know about the customers for getting churn analysis
  
<li> Online Mode </li>  
<ol>
  <li> Web Service </li>
Example: Predicting ride duration is the best example of deploying model as a web service 


![Alt text](Online_Mode.png?raw=true)
  
<li> Streaming </li>
  
![Alt text](Streaming_Mode.png?raw=true) 
  

  
  
Customer after booking cab customer's app based on location and other
property, backend(producer) will generate events and different models(consumer) for trip predictions
and duration predictions will be running and inferencing.


