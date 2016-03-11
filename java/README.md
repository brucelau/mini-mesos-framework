## Java Mini Framework

This framework is intended to run applications with groups of tasks to perform in the executors it spawns. Note: right now this only works on a local master/slave setup because we need to use the Mesos Fetcher to find the executor on remote slaves.

### Getting started

#### Step 1
Install [**Mesos**](http://mesos.apache.org/gettingstarted/) using their guide. I suggest going the Git route. Following the steps until just after ```make install```; note that this could take a couple of minutes.

#### Step 2

To run the master/slave vanilla, follow the remaining steps at the bottom:

```
$ ./bin/mesos-master.sh --ip=127.0.0.1 --work_dir=/var/lib/mesos
$ ./bin/mesos-master.sh --ip=127.0.0.1 --work_dir=/var/lib/mesos
```

To run the master/slave with authentication, setup credentials according to the bottom of the page [**here**](http://mesos.apache.org/documentation/latest/authentication/) instead. 

*(WIP -- not working yet)*: Alternatively, use [**dry-dock**](https://github.com/UncleBarney/dry-dock) to spin up a Dockerised Mesos/Marathon cluser locally; be sure to have installed Docker before this. 

#### Step 3
Build the Maven project:
```
$ cd (your-working-directory)
$ git clone git@github.com:lmok/mini-mesos-framework.git
$ cd java
$ mvn clean package
```

Fill out the template scripts in src/main/scripts with appropriate values, or export:
```
FMWK_JAR_DIR=<path_to_Maven_packaged_jar> # i.e. <abs path to>/target/MiniFramework-0.0.1-SNAPSHOT-jar-with-dependencies.jar
MESOS_BUILD_DIR=<path_to_Mesos_build_directory>
FMWK_EXECUTOR_PATH=<path_to_dir_with_executor_script> # i.e. <abs path to>/src/main/scripts/
LOG_DIR=<path_to_dir_for_logging>
```

On your local machine, run:
```
$ ./framework <mesos_master_ip>:5050
```
Or, if authentication is configured from step 2:
```
$ AUTHENTICATE=true PRINCIPAL=<principal2 from Step 2> SECRET=<secret2 from Step 2> ./framework <mesos_master_ip>:5050
```


### TODO:
1. Configure with dry-dock to run on localmachine
2. Parse scripts with configurations / make installable package for Mesos Fetcher
3. Enable Docker containeriser
4. Pass customisable # of tasks and application specs
