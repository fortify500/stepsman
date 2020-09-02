# STEPSMAN
Manage a scripted process step by step
Built to save the day
> If you think so, support us with a `star` and a `follow` 😘 

STEPSMAN is a command line utility which manages processes such as migrations, installations
, configurations, tests and anything which can be performed in steps.
Each step in the process is added a status and a position.

For example, suppose we have to:
1. Run an sql script.
2. Call rest api via curl.
3. Check that the sql entry was added.

STEPSMAN can manage this process as a script "run".

stepsman create -f 
created run id 1 (1 in this example)

run stepsman (will open in interactive mode)

* do run 1
* describe run 1 -> status done
* do run 1
* describe run 1 -> status failed because the rest server was down.
* -> we started the rest server
* do run 1
* describe run 1 ->status done
* do run 1
* describe run 1 -> status done

finished.