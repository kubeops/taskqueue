[![Go Report Card](https://goreportcard.com/badge/kubeops.dev/taskqueue)](https://goreportcard.com/report/kubeops.dev/taskqueue)
[![Build Status](https://github.com/kubeops/taskqueue/workflows/CI/badge.svg)](https://github.com/kubeops/taskqueue/actions?workflow=CI)
[![Docker Pulls](https://img.shields.io/docker/pulls/appscode/kube-taskqueue.svg)](https://hub.docker.com/r/appscode/kube-taskqueue/)
[![Slack](https://shields.io/badge/Join_Slack-salck?color=4A154B&logo=slack)](https://slack.appscode.com)
[![Twitter](https://img.shields.io/twitter/follow/kubeops.svg?style=social&logo=twitter&label=Follow)](https://twitter.com/intent/follow?screen_name=Kubeops)

# taskqueue

TaskQueue is a Kubernetes controller that manages a queue of tasks. It is designed to be used task processing system by mainting a queue with max concurrent task at a time.

## Deploy into a Kubernetes Cluster

You can deploy TaskQueue using Helm chart found [here](https://github.com/kubeops/installer/tree/master/charts/taskqueue).

```console
helm repo add appscode https://charts.appscode.com/stable/
helm repo update

helm install taskqueue appscode/taskqueue
```
