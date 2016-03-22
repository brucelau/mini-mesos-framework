## Java Mini Framework

This framework is intended to run applications with groups of tasks to perform in the executors it spawns.

### Getting started

#### Step 0
Make sure that JDK8, Maven, and Docker are installed.

Clone and build the Maven project:
```
$ cd [your working directory]
$ git clone git@github.com:lmok/mini-mesos-framework.git
$ cd java
$ mvn clean package
```
(Optional) build the dockerised executor here:
```
$ docker build -f images/executor/Dockerfile -t lmok/mini-executor .
```
Feel free to rename the executor container to something less mundane, but you have to push it to Dockerhub and change the image name & tag in the MiniScheduler constructor.

#### (playa-mesos) Step 1
Follow instructions at [**playa-mesos**](https://github.com/mesosphere/playa-mesos) up to step 4. 

Ensure you are in the directory with the Vagrantfile (i.e. playa-mesos/). Copy the clone of this repo into playa-mesos/:
```
$ cp -r [Path to framework]/mini-mesos-framework mini-mesos-framework
```

#### (playa-mesos) Step 2
Ssh into the mesos master (from playa-mesos/):
```
vagrant ssh
```

Run the framework:
```
$ cd /vagrant/
$ sh mini-mesos-framework/java/src/main/scripts/vagrant_framework 127.0.1.1:5050
```
*Go grab a coffee on the first run as the framework has to pull the image off of Dockerhub. When you get a status update to TASK_RUNNING then you're in business.*

If you exit the framework with a SIGINT/ctrl-c, you should be able to run `$ sudo docker ps -a` to see the exited docker executor. `$ docker logs [container id]` to see if the task ran.


### Architecture
To come... This vaguely mirrors a [**Marathon**](https://github.com/mesosphere/marathon)/[**Ochopod**](https://github.com/autodesk-cloud/ochopod) setup right now.

### Troubleshooting:

+ Anything vagrant/vmbox related: Don't look at me, blame [**play-mesos**](https://github.com/mesosphere/playa-mesos) instead!
+ Framework authentication errors: Make sure you didn't ask for the framework to be authenticated with an AUTHENTICATE=true flag. To set up authentication follow mesos instructions [**here**](http://mesos.apache.org/documentation/latest/authentication/). This framework will look for AUTHENTICATE, PRINCIPAL, and SECRET env vars.
+ lillio.mok@gmail.com for other inquiries. 
+ Everything ships with the generic apache license, by the way.


### TODO:
1. Build gateway/portal for deployment
2. Install Java ILP to optimise resource allocation
3. Pass customisable # of tasks and application specs
4. Try to plug ochopod into the executor (may need to Marathon-ify this fmwk)