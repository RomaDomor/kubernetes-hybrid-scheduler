# Smart Scheduler Helm Chart

A Helm chart for deploying the Kubernetes Hybrid Smart Scheduler Controller.

This chart bootstraps a deployment of the `smart-scheduler-controller`, which includes:
- A `Deployment` to run the controller logic.
- A `Service` to expose the admission webhook and metrics endpoints.
- A `MutatingWebhookConfiguration` to intercept pod creation requests.
- All necessary RBAC (`ServiceAccount`, `ClusterRole`, `ClusterRoleBinding`).
- A `ConfigMap` for runtime configuration of the scheduler.
- A Custom Resource Definition (`WorkloadProfile`) for persisting workload performance data.

## Prerequisites

- Kubernetes cluster v1.21+
- Helm v3.2.0+
- `kubectl` configured to connect to your cluster.

## Certificate Management

This chart includes a self-contained mechanism for generating the necessary TLS certificates for the admission webhook.

-   **Helm Hook Job:** A Kubernetes `Job` is executed as a `pre-install` and `pre-upgrade` hook.
-   **Self-Signed Certificates:** This Job uses `openssl` to generate a self-signed CA and a corresponding server certificate for the webhook `Service`.
-   **Secret Creation:** The generated certificates are stored in a `kubernetes.io/tls` secret.
-   **Webhook Patching:** The Job then patches the `MutatingWebhookConfiguration` to inject the CA bundle, making the webhook operational.

**Note:** This method does not handle automatic certificate renewal. The generated certificate is valid for 365 days. To renew the certificate, you must perform a `helm upgrade` of the chart within that period. For production environments requiring automatic renewal, consider using a tool like `cert-manager`.

## Installing the Chart

### Option 1: Install from Helm Repository (Recommended)

This is the recommended method for most users. It allows you to easily install and manage the chart and its versions.

1.  **Add the Smart Scheduler Helm repository:**

    ```sh
    helm repo add smart-scheduler https://romadomor.github.io/kubernetes-hybrid-scheduler/
    helm repo update
    ```

2.  **Install the chart:**

    Install the chart into your desired namespace (we recommend `kube-system`).

    ```sh
    # The CRD is installed automatically from the crds/ directory
    helm install smart-scheduler smart-scheduler/smart-scheduler \
      --namespace kube-system \
      --create-namespace
    ```

    You can search for available versions to install:
    ```sh
    helm search repo smart-scheduler
    ```

    And install a specific version using the `--version` flag:
    ```sh
    helm install smart-scheduler smart-scheduler/smart-scheduler --version 0.1.0
    ```

### Installing with Custom Configuration

To customize the installation, you can create a `my-values.yaml` file and override the default settings.

```sh
# Example my-values.yaml
config:
  cloudEndpoint: "192.168.1.100"
  rttThresholdMs: "80"
  localityBonus: "30"

resources:
  requests:
    cpu: 150m
    memory: 300Mi
  limits:
    cpu: 1
    memory: 1Gi
```

Then, install the chart using the `-f` flag with your preferred installation method:

```sh
# From Helm repository
helm install smart-scheduler smart-scheduler/smart-scheduler \
  -f my-values.yaml \
  --namespace kube-system

# Or from a local path
helm install smart-scheduler ./deployments/smart-scheduler \
  -f my-values.yaml \
  --namespace kube-system
```

## Uninstalling the Chart

To uninstall and remove all resources associated with the `smart-scheduler` release:

```sh
helm uninstall smart-scheduler --namespace kube-system
```
**Note:** This command will not remove the `WorkloadProfile` CRD.

## Configuration

The following table lists the configurable parameters of the Smart Scheduler chart and their default values.

| Parameter                      | Description                                                                          | Default Value                                  |
| ------------------------------ | ------------------------------------------------------------------------------------ |------------------------------------------------|
| `replicaCount`                 | Number of controller replicas to deploy.                                             | `1`                                            |
| `image.repository`             | The container image repository for the controller.                                   | `ghcr.io/romadomor/kubernetes-smart-scheduler` |
| `image.pullPolicy`             | The image pull policy.                                                               | `IfNotPresent`                                 |
| `image.tag`                    | The image tag. Overrides the chart's `appVersion` if set.                            | `"latest"`                                     |
| `serviceAccountName`           | The name of the `ServiceAccount` for the controller pod.                             | `smart-scheduler-controller`                   |
| `webhookSecretName`            | Name of the secret containing the webhook's TLS certificates.                        | `smart-scheduler-certs`                        |
| `service.type`                 | Kubernetes service type for the webhook.                                             | `ClusterIP`                                    |
| `service.ports.https`          | The external port for the webhook service (HTTPS).                                   | `443`                                          |
| `service.ports.metrics`        | The external port for the metrics service.                                           | `8080`                                         |
| `service.targetPorts.https`    | The internal container port for the webhook server.                                  | `8443`                                         |
| `service.targetPorts.metrics`  | The internal container port for the metrics server.                                  | `8080`                                         |
| `resources`                    | CPU/Memory resource requests and limits for the controller pod.                      | (see `values.yaml`)                            |
| `probes.liveness.*`            | Liveness probe settings for the controller.                                          | (see `values.yaml`)                            |
| `probes.readiness.*`           | Readiness probe settings for the controller.                                         | (see `values.yaml`)                            |
| `config.rttThresholdMs`        | WAN RTT threshold in milliseconds.                                                   | `"100"`                                        |
| `config.lossThresholdPct`      | WAN packet loss threshold in percent.                                                | `"2.0"`                                        |
| `config.cloudEndpoint`         | Cloud endpoint IP address for the WAN probe.                                         | `"10.20.0.3"`                                  |
| `config.localityBonus`         | Score bonus applied to scheduling decisions for the edge.                            | `"25"`                                         |
| `config.cloudMarginOverridePct`| Cloud deadline margin override percentage.                                           | `"0.15"`                                       |
| `config.wanStaleConfFactor`    | Confidence factor multiplier when WAN telemetry is stale.                            | `"0.8"`                                        |
| `config.edgeHeadroomOverridePct` | Percentage of extra resources required on an edge node to be considered schedulable. | `"0.1"`                                        |
| `config.histBoundsMode`        | Histogram bounds mode (`explicit` or `log`).                                         | `"explicit"`                                   |
| `config.histBounds`            | Comma-separated histogram bounds in milliseconds for `explicit` mode.                | (see `values.yaml`)                            |
| `config.histLogStartMs`        | Log mode: starting bound in milliseconds.                                            | `"50"`                                         |
| `config.histLogFactor`         | Log mode: factor per step.                                                           | `"2.0"`                                        |
| `config.histLogCount`          | Log mode: number of bounds.                                                          | `"20"`                                         |
| `config.histIncludeInf`        | Append a `+Inf` bucket to the histogram (`1` for true, `0` for false).               | `"1"`                                          |
| `config.ingestCapMs`           | Ingest cap for observed durations in milliseconds.                                   | `"1800000"`                                    |
| `config.minSampleCount`        | Minimum samples required for a reliable P95 calculation.                             | `"10"`                                         |
| `config.decayInterval`         | Histogram decay interval (e.g., `30m`, `1h`).                                        | `"1h"`                                         |