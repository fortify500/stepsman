# StepsMan (Steps Manager)
* StepsMan is a step by step reliable event driven business process, decision management and workflow manager **completely over postgreSQL** (additional technologies can be added for optimizations and larger scale).
* StepsMan leverages operations such as postgreSQL Listen Notify to speed things(1) up but main rely on transactions and events. The rational is to not compromise on reliability.
* No SDKs required, stepsman utilizes a simple REST flow for both synchronous and asynchronous steps. It calls you. (2)
* You can also request to complete the task by yourself and submit the results to the engine.
* Also, planned is support for remote workers which can be implemented by you or simply run stepsman as a foreperson->worker mode.
* StepsMan is different in that it includes a lot of locations to run code. Everything that requires deciding what to do, defers to a code. Currently, we plan to support Rego (https://www.openpolicyagent.org/docs/latest/policy-language/) language which is based on datalog and is more elegant and resilient for decisions.
* As opposed to other workflow or microservices orchestration platforms, its memory requirements are small and is very well suited to be positioned as a sidecar in kubernetes.
* It has a small size footprint and does not require external dependencies.
* It loads extremely fast.
* The client and server code are in the same executable.
* Works out of the box (with in-memory or local sqlite) with sensible defaults, but can be scaled by choosing to use postgreSQL as the state store.
* Supports single process mode, in-memory by default and so can achieve high performance out of the box. 
* StepsMan is very useful for approval systems and is very friendly for developers.
(1) note that listen/notify would probably not work in postgresql cluster. At this point you can switch to a messaging technology such as redis pub/sub (contact @TzahiFadida).
(2) it is possible to cloud host - contact @TzahiFadida  
Built to save the day
> If you think so, support us with a `star` and a `follow` 😘 

p.s. StepsMan is a short for Steps Manager.

The roadmap is located at https://github.com/fortify500/stepsman/blob/main/roadmap/proposal.yaml but it is currently in flux and not everything is up to date or correct, however it can give an idea about the direction.
More documentation to come...