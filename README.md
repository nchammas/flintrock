# flintrock

flintrock is a command-line tool for launching Apache Spark clusters.

## Scope

1. **Spark-centric**: If flintrock launches any service or tool other than Spark, it's strictly to support or integrate with Spark. flintrock is not for, say, launching a bare Mesos cluster.

2. **Suitable for programmatic use**: Many people will use flintrock interactively from the command line, but flintrock is also meant to be used as part of an automated job.

3. **Intended for short-lived clusters and automated testing of Spark**: flintrock is for quickly spinning up a Spark cluster to test something out, run a job, or just experiment. It's also meant for creating Spark clusters with specific OS or network configurations for automated performance testing.

 flintrock is not for spinning up a production Spark cluster.

## Goals

1. Be a fun side-project.
2. Be a better alternative to spark-ec2 for the use-cases outlined above.

It's unlikely that #2 will ever happen, but hey, I can dream right?

## History

This project is starting out as a fork of spark-ec2 with some modifications.
