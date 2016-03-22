## Java Mini Framework

This framework is intended to run applications with groups of tasks to perform in the executors it spawns. Note: right now this only works on a local master/slave setup because we need to use the Mesos Fetcher to find the executor on remote slaves.

### Getting started

#### Step 0

Clone and build the Maven project:
```
$ cd <your_working_directory>
$ git clone git@github.com:lmok/mini-mesos-framework.git
$ cd java
$ mvn clean package
```

You now have three options... use [**Mesos's local setup**](https://github.com/lmok/mini-mesos-framework/tree/master/java#using-a-manual-local-mesos-setup), [**dry-dock**](https://github.com/lmok/mini-mesos-framework/tree/master/java#using-dry-dock) (NOT FUNCTIONAL YET), or [**playa-mesos**](https://github.com/lmok/mini-mesos-framework/tree/master/java#using-playa-mesos).

### Using a manual local mesos setup

#### (Local Mesos) Step 1

Fill out the template scripts in src/main/scripts with appropriate values, or export:
```
$FMWK_JAR_DIR=<path_to_Maven_packaged_jar> # i.e. <abs_path_to>/target/MiniFramework-0.0.1-SNAPSHOT-jar-with-dependencies.jar
$MESOS_LIB_PATH=<path_to_native_mesos_library> # <mesos_build_path>/src/.lib since you are following Mesos' local setup guide
$FMWK_EXECUTOR_PATH=<path_to_dir_with_executor_script> # i.e. <abs_path_to>/src/main/scripts/
$LOG_DIR=<path_to_dir_for_logging>
```

#### (Local Mesos) Step 2

Install local [**Mesos**](http://mesos.apache.org/gettingstarted/) using their guide. I suggest going the Git route. Following the steps until just after ```make install```. Note that this could take a couple of minutes -- might want to get a coffee.

To run the master/slave vanilla, follow the remaining steps at the bottom:

```
$ ./bin/mesos-master.sh --ip=127.0.0.1 --work_dir=/var/lib/mesos
$ ./bin/mesos-slave.sh --master=127.0.0.1:5050
```

To run the master/slave with authentication, setup credentials according to the bottom of the page [**here**](http://mesos.apache.org/documentation/latest/authentication/) instead. 

#### (Local Mesos) Step 3
On your local machine, run:
```
$ ./framework <mesos_master_ip>:5050 # should be localhost if following Steps 1-2
```
Or, if authentication is configured from step 2:
```
$ AUTHENTICATE=true PRINCIPAL=<principal2_from_Step_2> SECRET=<secret2_from_Step_2> ./framework <mesos_master_ip>:5050
```

### Using dry-dock

#### (WIP dry-dock) Step 1

??? Don't do this yet, not working.

#### (WIP dry-dock) Step 2

Alternatively, you can use [**dry-dock**](https://github.com/UncleBarney/dry-dock) to spin up a Dockerised Mesos/Marathon cluser locally; be sure to have installed [**Docker**](https://www.docker.com/) before this. 

Find the IP for your Docker (see docs for dry-dock). Export this (e.g. I use docker-machine):
```
$ export MESOS_LOCALSETUP_HOST_IP=`docker-machine ip`
```

### Using playa-mesos

#### (playa-mesos) Step 1
Follow instructions at [**playa-mesos**](https://github.com/mesosphere/playa-mesos) up to step 4. 

Ensure you are in the directory with the Vagrantfile (i.e. playa-mesos/). Copy the clone of this repo into playa-mesos/:
```
$ cp -r [Path to framework]/mini-mesos-framework mini-mesos-framework
```

#### (playa-mesos) Step 2
Ssh into the mesos master (from playa-mesos/:
```
vagrant ssh
```

#### (playa-mesos) Step 3
Run the framework:
```
$ cd /vagrant
$ sh mini-mesos-framework/java/src/main/scripts/vagrant_framework 127.0.1.1:5050
```


### Troubleshooting:

+ Mesos Master/Slave not running: I defer to the Mesos documentation for this one. Make sure your firewall allows the machines to interact with each other.
+ Framework hangs on authentication: Same as above; make sure that your firewall allows connections to/from your Mesos master/slave.
+ Cannot find native Mesos library: Did you specify the right mesos build directory?


### TODO:
1. Configure with dry-dock to run locally
2. Enable Docker containeriser
3a. Build gateway docker container for task deployment
3b. Rework logic for executors + tasks to parallelise properly
4. Parse scripts with configurations / make installable package for Mesos Fetcher
5. Install Java ILP to optimise resource allocation
6. Pass customisable # of tasks and application specs
