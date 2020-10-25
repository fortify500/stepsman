# StepsMan (Steps Manager)
* StepsMan is a step by step event driven business process and workflow manager.
* As opposed to other workflow or microservices orchestration platform, its minimal requirements are about 5mb of memory and is very well suited to be positioned as a sidecar in kubernetes.
* It has a very small footprint of 25mb and does not require external dependencies.
* It loads extremely fast, less than 1s.
* The client and server code are in the same executable.
* Works out of the box with simple defaults, but can be scaled by choosing to use postgresql as the state store.
* To be more resilient soon it will support redis queues at the cost of a bit of performance, but you can decide on your preference.
* Supports single mode, in-memory by default and so can achieve high performance out of the box. 
* StepsMan is very useful for approval systems and is very friendly for developers.

Built to save the day
> If you think so, support us with a `star` and a `follow` 😘 

More documentation to come...