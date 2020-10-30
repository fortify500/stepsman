# StepsMan (Steps Manager)
* StepsMan is a step by step event driven business process and workflow manager.
* No SDKs required, a simple REST flow is supported for both synchronous and asynchronous steps which are controlled by the responder.
* StepsMan is different in that it includes a lot of locations to run code. Everything that requires deciding what to do, defers to a code. Currently we plan to support Rego (https://www.openpolicyagent.org/docs/latest/policy-language/) language which is based on datalog and is more elegant and resilient for decisions.
* As opposed to other workflow or microservices orchestration platforms, its minimal requirements are about 5mb of memory and is very well suited to be positioned as a sidecar in kubernetes.
* It has a very small footprint (currently) of 25mb and does not require external dependencies.
* It loads extremely fast, (currently) less than 1s.
* The client and server code are in the same executable.
* Works out of the box with sensible defaults, but can be scaled by choosing to use postgresql as the state store.
* To be more resilient, soon it will support a rest call to external scheduler or queue (implemented by the responder) at the cost of a bit of performance, but you can decide on your preference.
* Supports single mode, in-memory by default and so can achieve high performance out of the box. 
* StepsMan is very useful for approval systems and is very friendly for developers.

Built to save the day
> If you think so, support us with a `star` and a `follow` 😘 

p.s. StepsMan is a short for Steps Manager.

The roadmap is located at https://github.com/fortify500/stepsman/blob/main/roadmap/proposal.yaml but it is currently in flux and not everything is up to date or correct, however it can give an idea about the direction.
More documentation to come...