#!/bin/zsh

helm repo add bitnami https://charts.bitnami.com/bitnami
kubectl apply -f https://raw.githubusercontent.com/wiv33/spark/master/kubernetes/ns.yaml
helm install spark bitnami/spark -n spark
# Read more about the installation in the Apache Spark packaged by Bitnami Chart Github repository