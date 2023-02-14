# Charmed Spark History Server Operator

## Overview
The Charmed Spark History Server Operator delivers automated operations management from day 0 to day 2 on the Apache Spark History Server. 
It is part of an open source, end-to-end, production ready data platform on top of cloud native technologies provided by Canonical.

History Server is the component of Apache Spark which enables the user to view and analyze logs of completed Spark applications.

This operator charm deploys and operates Apache Spark History Server on Kubernetes environments. 
It depends on the S3 integrator charm from Canonical for S3 related configuration.

## Usage

```bash
$ juju deploy s3-integrator --channel latest/edge
$ juju deploy spark-history-server-k8s --channel latest/edge
$ juju relate spark-history-server-k8s s3-integrator
```

Once the spark history server unit is active, go to the IP of the unit at port 18080 to load the history server UI.
