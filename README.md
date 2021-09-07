# Demo: Optimizing Kafka Streams Topologies 

Demo application for the blogpost: "Optimizing Kafka Streams Topologies running on Kubernetes".

## Prerequisites
  * [Kubernetes Cluster](https://kubernetes.io/docs/tutorials/kubernetes-basics/create-cluster/)
  * [Helm](https://helm.sh/docs/intro/install/)
  * Kafka ( [e.g., via confluent helm charts](https://github.com/confluentinc/cp-helm-charts) )
  * [Keda] (https://keda.sh/)
  * bakdata Helm Charts
    which can be added with: 
```
helm repo add bakdata-common https://raw.githubusercontent.com/bakdata/streams-bootstrap/master/charts
```
---

## Generate Data

We provide schemas for the [Avro Random Generator](https://github.com/confluentinc/avro-random-generator) in `./data-gen`. The [Datagen Source Connector](https://docs.confluent.io/kafka-connect-datagen/current/index.html) by Confluent and their [ksql-datagen tool](https://docs.confluent.io/4.1.1/ksql/docs/tutorials/generate-custom-test-data.html) can use those provided schemas to generate the data into input topics. 

## Build

We use [jib](https://github.com/GoogleContainerTools/jib) to build the application and push it to a docker registry.

## Deploy

We provide 3 different example deployments in `./deployments`:

- `values-all.yaml`: Deployment for the application running the whole topology
- `values-customer-lookup.yaml`: Deployment for the first sub-topology performing the customer lookup
- `values-long-running.yaml`: Deployment for the second, long-running sub-topology

Before deployment you must change the brokers, Schemaregistry URL, and image values in the corresponding value files.

Using Helm, we can then deploy the application to Kubernetes:

```
helm upgrade --debug --install --force --values values-all.yaml complete-topology bakdata-common/streams-app --namespace {namespace}
```
