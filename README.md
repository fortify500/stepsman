# STEPSMAN
STEPSMAN is a step by step event driven business process and workflow manager.

Built to save the day
> If you think so, support us with a `star` and a `follow` 😘 

[![demo](https://github.com/fortify500/assets/raw/master/stepsman.gif)](https://github.com/c-bata/kube-prompt)

STEPSMAN is a command line utility which manages processes such as migrations, installations, configurations, tests and anything which can be performed in steps. Each step in the process is added a status and a position.

STEPSMAN can manage this process as a script "run".

stepsman create -f <file name> 

run stepsman (will open in interactive mode)
```
help

[stepsman]: help
Stepsman is a command line utility to manage processes such as:
* Installations
* Upgrades
* Migrations
* Tests
* Anything that looks like a list of steps to complete

"stepsman" with no commands and parameters will enter interactive mode

Usage:
  stepsman [command]

Available Commands:
  !           ! will execute a shell command.
  create      Create run
  describe    Describe a run steps
  do          Do can execute a command of a run step.
  help        Help about any command
  list        Runs summary or a list of a run steps.
  skip        Skip a step of a run.
  stop        Stop a run.

Flags:
      --config string   config file (default is $HOME/.stepsman.yaml)
  -h, --help            help for stepsman

Use "stepsman [command] --help" for more information about a command.
```