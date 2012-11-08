brooklyn-cdh
============

Brooklyn deployment and management of Cloudera Hadoop and Manager clusters.


### Setup 1:  Dependencies

You must have the following software installed and compiled (`mvn clean install`):

* `https://github.com/brooklyncentral/brooklyn`: currently snapshot is required, and 
  below we assume that `brooklyn` is in your path 
* `https://github.com/ahgittin/whirr-cm`: below we assume this is in a sibling directory to `brooklyn-cdh` 


### Compile

To compile brooklyn-cdh, simply `mvn clean install` in the project root.


### Setup 2:  Credentials

To run, you'll need to specify credentials for your preferred cloud.  This can be done 
in `~/.brooklyn/brooklyn.properties`:

    brooklyn.jclouds.aws-ec2.identity=AKXXXXXXXXXXXXXXXXXX
    brooklyn.jclouds.aws-ec2.credential=secret01xxxxxxxxxxxxxxxxxxxxxxxxxxx

Alternatively these can be set as shell environment parameters or JVM system properties.

Many other clouds are supported also, as well as pre-existing machines ("bring your own nodes"),
custom endpoints for private clouds, and specifying custom keys and passphrases.
For more information see:

    https://github.com/brooklyncentral/brooklyn/blob/master/docs/use/guide/defining-applications/common-usage.md#off-the-shelf-locations


### Run

To run it, either:

* Install the `brooklyn` CLI tool, either from source (as above) or from 
  http://brooklyncentral.github.com/ and then in root of this project:

        export BROOKLYN_CLASSPATH=target/brooklyn-cdh-1.0.0-SNAPSHOT.jar:../whirr-cm/target/whirr-cm-1.1-SNAPSHOT.jar
        brooklyn launch -a io.cloudsoft.cloudera.SampleClouderaManagedCluster -l aws-ec2:us-east-1

* Grab all dependencies (using maven, or in your favourite IDE) and run the 
  static `main` in `io.cloudsoft.cloudera.SampleClouderaManagedCluster`.

* Build `mvn assembly:assembly` below then follow the instructions in the generated archive.

After about 15 minutes, it should print out the URL of the Cloudera Manager node.
In the meantime you can follow the progress in the Brooklyn console, 
usually at localhost:8081.  

To destroy the VM's provisioned, either invoke `stop` on the root of the
application in the Brooklyn console or use the management console of your
cloud.  VM's are not destroyed simply by killing Brooklyn.


### Executable Assembly

This project can also build a binary redistributable by using mvn assembly:assembly.
See the source files under `src/main/assembly` for more information.  These can 
easily be modified for a custom archive.


### More about Brooklyn

Brooklyn is a code library and framework for managing distributed applications
in the cloud.  It has been used to create this project for rolling out Cloudera,
building on Whirr and other routines.

This project can be extended for more complex topologies, additional applications
which run alongside CDH, and to develop sophisticated management policies to
scale or tune the cluster for specific applications.

For more information consider:

* Visiting the open-source Brooklyn home page at  http://brooklyncentral.github.com
* Forking the Brooklyn project at  http://github.com/brooklyncentral/brooklyn
* Forking the brooklyn-cdh project at  http://github.com/cloudsoft/brooklyn-cdh
* Emailing  brooklyn-users@googlegroups.com 

For commercial enquiries -- including bespoke development and paid support --
contact Cloudsoft, the supporters of Brooklyn, at:

* www.CloudsoftCorp.com
* info@cloudsoftcorp.com

This software is (c) 2012 Cloudsoft Corporation, released as open source under the Apache License v2.0.

