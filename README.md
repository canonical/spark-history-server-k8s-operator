# Charmed Spark History Server Operator

[![CharmHub Badge](https://charmhub.io/spark-history-server-k8s/badge.svg)](https://charmhub.io/spark-history-server-k8s)
[![Release](https://github.com/canonical/spark-history-server-k8s-operator/actions/workflows/release.yaml/badge.svg)](https://github.com/canonical/spark-history-server-k8s-operator/actions/workflows/release.yaml)
[![Tests](https://github.com/canonical/spark-history-server-k8s-operator/actions/workflows/ci.yaml/badge.svg?branch=main)](https://github.com/canonical/spark-history-server-k8s-operator/actions/workflows/ci.yaml?query=branch%3Amain)

## Overview
The Charmed Spark History Server Operator delivers automated operations management from day 0 to day 2 on the Apache Spark History Server. 
It is part of an open source, end-to-end, production ready data platform on top of cloud native technologies provided by Canonical.

History Server is the component of Apache Spark which enables the user to view and analyze logs of completed Spark applications.

The Spark History Server charm operator deploys and operates Apache Spark History Server on Kubernetes environments.
It depends on the S3 integrator charm from Canonical for S3 related configuration.

The Spark History Server charm can be found on [Charmhub](https://charmhub.io/spark-history-server-k8s). 
Apache Spark is a free, open source software project by the Apache Software Foundation. Users can find out more at the [Spark project page](https://spark.apache.org/).


## Usage

```bash
juju deploy spark-history-server-k8s --channel 4/edge
juju deploy s3-integrator --channel 2/stable

SECRET_URI=$(juju add-secret s3-creds access-key=<access-key> secret-key=<secret-key>)
juju grant-secret s3-creds s3-integrator

juju config s3-integrator bucket=<bucket> endpoint=<endpoint> region=<region> path=spark-events credentials=$SECRET_URI 

juju relate spark-history-server-k8s s3-integrator
```

> [!NOTE]  
> If the `region` is not configured in the `s3-integrator` charm before integrating it with `spark-history-server-k8s` charm, the `spark-history-server-k8s` charm uses `us-east-1` as the region to send requests to S3.

Once the spark history server unit is active, go to the IP of the unit at port 18080 to load the history server UI.

Although both tracks `1/` and track `2/` of `s3-integrator` are supported for integration with the Spark History Server charm, it is still recommended to use
`s3-integrator` from track `2/` because of it's advanced capabilities like the use of Juju secrets for credentials and the track `1/` being locked for critical
bugfixes and security fixes and reaching EOL in the near future.
