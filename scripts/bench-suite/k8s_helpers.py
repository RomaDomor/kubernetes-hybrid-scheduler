import sys
import time
from pathlib import Path

from kubernetes import client, config, utils, watch
from kubernetes.client import ApiException, V1DeleteOptions
from kubernetes.stream import stream
from utils import log, read_yaml_multi

CLIENT_WORKLOADS = {("Pod", "toolbox"), ("Job", "stream-data-generator")}

def kube_init(context: str, kubeconfig_path: str):
    """Initializes Kubernetes client from kubeconfig or in-cluster config."""
    try:
        if context:
            config.load_kube_config(context=context)
        else:
            config.load_kube_config(config_file=kubeconfig_path)
    except Exception:
        log("Could not load kubeconfig. Trying in-cluster config.")
        config.load_incluster_config()

def ensure_namespace(v1: client.CoreV1Api, ns: str):
    """Ensures a Kubernetes namespace exists."""
    try:
        v1.read_namespace(ns)
    except ApiException as e:
        if e.status == 404:
            log(f"Namespace '{ns}' not found, creating it.")
            v1.create_namespace(client.V1Namespace(metadata=client.V1ObjectMeta(name=ns)))
        else:
            raise

def k_apply(ns_offloaded: str, ns_local: str, file_path: Path):
    """Applies a YAML manifest, splitting workloads into correct namespaces."""
    k8s_client = client.ApiClient()
    docs = read_yaml_multi(file_path)

    offloaded_docs, local_docs = [], []
    for doc in docs:
        kind, name = doc.get("kind"), (doc.get("metadata") or {}).get("name")
        if (kind, name) in CLIENT_WORKLOADS:
            if name == "stream-data-generator" and kind == "Job":
                # Special handling for stream-data-generator env var
                for container in doc.get("spec", {}).get("template", {}).get("spec", {}).get("containers", []):
                    for env_var in container.get("env", []):
                        if env_var.get("name") == "PROCESSOR_URL":
                            env_var["value"] = f"http://stream-processor.{ns_offloaded}:8080"
            local_docs.append(doc)
        else:
            offloaded_docs.append(doc)

    if offloaded_docs:
        utils.create_from_yaml(k8s_client, yaml_objects=offloaded_docs, namespace=ns_offloaded)
    if local_docs:
        utils.create_from_yaml(k8s_client, yaml_objects=local_docs, namespace=ns_local)

def k_delete(ns_offloaded: str, ns_local: str, file_path: Path):
    """Deletes Kubernetes objects defined in a YAML file."""
    for doc in read_yaml_multi(file_path):
        kind, name = doc.get("kind"), (doc.get("metadata") or {}).get("name")
        ns = ns_local if (kind, name) in CLIENT_WORKLOADS else ns_offloaded
        try:
            # The python client's delete_from_yaml is less reliable, so we do it manually.
            api_map = {
                "Pod": client.CoreV1Api().delete_namespaced_pod,
                "Service": client.CoreV1Api().delete_namespaced_service,
                "Deployment": client.AppsV1Api().delete_namespaced_deployment,
                "Job": client.BatchV1Api().delete_namespaced_job,
                "ConfigMap": client.CoreV1Api().delete_namespaced_config_map,
                "Secret": client.CoreV1Api().delete_namespaced_secret,
            }
            if kind in api_map:
                api_map[kind](name=name, namespace=ns)
        except ApiException as e:
            if e.status != 404:
                log(f"Error deleting {kind}/{name} in {ns}: {e}")

def wait_pod_ready(v1: client.CoreV1Api, ns: str, name: str, timeout_sec: int = 180):
    """Waits for a pod to enter the 'Ready' state."""
    w = watch.Watch()
    for event in w.stream(v1.list_namespaced_pod, namespace=ns, field_selector=f"metadata.name={name}", timeout_seconds=timeout_sec):
        if event["type"] == "DELETED":
            raise RuntimeError(f"Pod {name} was deleted before becoming ready")
        pod = event["object"]
        if pod.status.conditions and any(c.type == "Ready" and c.status == "True" for c in pod.status.conditions):
            w.stop()
            return
    raise TimeoutError(f"Pod {name} did not become ready within {timeout_sec}s")

def wait_deployment_ready(apps_v1: client.AppsV1Api, ns: str, name: str, timeout_sec: int = 300):
    """Waits for a deployment's replicas to be ready and updated."""
    w = watch.Watch()
    for event in w.stream(apps_v1.list_namespaced_deployment, namespace=ns, field_selector=f"metadata.name={name}", timeout_seconds=timeout_sec):
        if event["type"] == "DELETED":
            raise RuntimeError(f"Deployment {name} was deleted")
        status = event["object"].status
        if (status.ready_replicas and status.replicas and
                status.ready_replicas == status.replicas and
                status.updated_replicas == status.replicas):
            w.stop()
            return
    raise TimeoutError(f"Deployment {name} did not become ready within {timeout_sec}s")

def get_job_logs(v1: client.CoreV1Api, ns: str, job_name: str) -> str:
    """Gets logs from all pods created by a specific job."""
    try:
        pods = v1.list_namespaced_pod(namespace=ns, label_selector=f"job-name={job_name}")
        logs = []
        for pod in pods.items:
            try:
                pod_logs = v1.read_namespaced_pod_log(name=pod.metadata.name, namespace=ns)
                logs.append(f"=== Pod {pod.metadata.name} ===\n{pod_logs}")
            except ApiException as e:
                logs.append(f"=== Pod {pod.metadata.name} === ERROR: {e}")
        result = "\n".join(logs)
        sys.stdout.write(result)
        return result
    except ApiException as e:
        log(f"Error getting logs for job {job_name}: {e}")
        return ""


def wait_job_complete(batch_v1: client.BatchV1Api, ns: str, name: str, timeout_sec: int) -> float:
    """Waits for a job to complete and returns its execution duration."""
    log(f"Waiting for Job {ns}/{name} to complete (timeout {timeout_sec}s)...")
    w = watch.Watch()
    start_time = time.time()
    for event in w.stream(batch_v1.list_namespaced_job, namespace=ns, field_selector=f"metadata.name={name}", timeout_seconds=timeout_sec):
        if event["type"] == "DELETED":
            raise RuntimeError(f"Job {name} was deleted before completing")
        job = event["object"]
        if job.status.conditions:
            for condition in job.status.conditions:
                if condition.type == "Complete" and condition.status == "True":
                    w.stop()
                    return time.time() - start_time
                elif condition.type == "Failed" and condition.status == "True":
                    w.stop()
                    log(f"Job {ns}/{name} failed. Capturing logs.")
                    get_job_logs(client.CoreV1Api(), ns, name)
                    raise RuntimeError(f"Job {name} failed")

    log(f"Job {ns}/{name} timed out. Capturing logs.")
    get_job_logs(client.CoreV1Api(), ns, name)
    raise TimeoutError(f"Job {name} did not complete within {timeout_sec}s")

def toolbox_exec(v1: client.CoreV1Api, ns: str, cmd: str, check=False) -> str:
    """Executes a command in the 'toolbox' pod."""
    try:
        return stream(v1.connect_get_namespaced_pod_exec, "toolbox", ns,
                      command=["sh", "-lc", cmd],
                      stderr=True, stdin=False, stdout=True, tty=False,
                      _preload_content=True)
    except Exception as e:
        if check:
            raise
        log(f"Toolbox command failed: '{cmd}', error: {e}")
        return ""

# -------------------- Force cleanup (kubectl-like) --------------------
def wait_gone(check_fn, name: str, timeout_sec: int = 120, interval: float = 0.5):
    """Poll until the resource retrieval raises 404 or times out."""
    end = time.time() + timeout_sec
    while time.time() < end:
        try:
            check_fn()
            time.sleep(interval)
        except ApiException as e:
            if e.status == 404:
                return True
            time.sleep(interval)
        except Exception:
            # Treat other errors as not-gone-yet, continue
            time.sleep(interval)
    log(f"Timeout waiting for deletion: {name}")
    return False

def force_delete_all_in_namespace(ns: str, label_selector: str = "", timeout_sec: int = 180):
    """
    Force delete almost everything in a namespace, similar to:
      kubectl delete all,cm,secret,ingress,sa,role,rolebinding,pvc,job,cronjob,deploy,rs,po \
        -l <selector> -n <ns> --force --grace-period=0
    If label_selector is empty, deletes all matching kinds in the namespace.
    """
    v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    batch_v1 = client.BatchV1Api()
    networking_v1 = client.NetworkingV1Api()
    rbac_v1 = client.RbacAuthorizationV1Api()

    opts_fg = V1DeleteOptions(propagation_policy="Foreground")
    opts_bg = V1DeleteOptions(propagation_policy="Background")

    ls = label_selector or ""

    def del_and_wait_deploy():
        try:
            items = apps_v1.list_namespaced_deployment(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    apps_v1.delete_namespaced_deployment(name, ns, body=opts_fg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete deployment/{name}: {e}")
                wait_gone(lambda: apps_v1.read_namespaced_deployment(name, ns), f"deployment/{name}")
        except Exception as e:
            log(f"list deployments: {e}")

    def del_and_wait_rs():
        try:
            items = apps_v1.list_namespaced_replica_set(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    apps_v1.delete_namespaced_replica_set(name, ns, body=opts_bg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete rs/{name}: {e}")
                wait_gone(lambda: apps_v1.read_namespaced_replica_set(name, ns), f"rs/{name}")
        except Exception as e:
            log(f"list replicasets: {e}")

    def del_and_wait_jobs():
        try:
            items = batch_v1.list_namespaced_job(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    batch_v1.delete_namespaced_job(name, ns, body=opts_fg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete job/{name}: {e}")
                wait_gone(lambda: batch_v1.read_namespaced_job(name, ns), f"job/{name}")
        except Exception as e:
            log(f"list jobs: {e}")

    def del_and_wait_cronjobs():
        try:
            items = batch_v1.list_namespaced_cron_job(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    batch_v1.delete_namespaced_cron_job(name, ns, body=opts_fg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete cronjob/{name}: {e}")
                wait_gone(lambda: batch_v1.read_namespaced_cron_job(name, ns), f"cronjob/{name}")
        except Exception as e:
            log(f"list cronjobs: {e}")

    def del_and_wait_pods():
        try:
            items = v1.list_namespaced_pod(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    v1.delete_namespaced_pod(name, ns, body=opts_fg, grace_period_seconds=0)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete pod/{name}: {e}")
                wait_gone(lambda: v1.read_namespaced_pod(name, ns), f"pod/{name}")
        except Exception as e:
            log(f"list pods: {e}")

    def del_and_wait_svcs():
        try:
            items = v1.list_namespaced_service(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                if name == "kubernetes":  # cluster service; usually not in user ns
                    continue
                try:
                    v1.delete_namespaced_service(name, ns, body=opts_bg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete svc/{name}: {e}")
                wait_gone(lambda: v1.read_namespaced_service(name, ns), f"svc/{name}")
        except Exception as e:
            log(f"list services: {e}")

    def del_and_wait_cms():
        try:
            items = v1.list_namespaced_config_map(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    v1.delete_namespaced_config_map(name, ns, body=opts_bg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete cm/{name}: {e}")
                wait_gone(lambda: v1.read_namespaced_config_map(name, ns), f"cm/{name}")
        except Exception as e:
            log(f"list configmaps: {e}")

    def del_and_wait_secrets():
        try:
            items = v1.list_namespaced_secret(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    v1.delete_namespaced_secret(name, ns, body=opts_bg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete secret/{name}: {e}")
                wait_gone(lambda: v1.read_namespaced_secret(name, ns), f"secret/{name}")
        except Exception as e:
            log(f"list secrets: {e}")

    def del_and_wait_ingresses():
        try:
            items = networking_v1.list_namespaced_ingress(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    networking_v1.delete_namespaced_ingress(name, ns, body=opts_bg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete ingress/{name}: {e}")
                wait_gone(lambda: networking_v1.read_namespaced_ingress(name, ns), f"ingress/{name}")
        except Exception as e:
            log(f"list ingresses: {e}")

    def del_and_wait_sas():
        try:
            items = v1.list_namespaced_service_account(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    v1.delete_namespaced_service_account(name, ns, body=opts_bg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete sa/{name}: {e}")
                wait_gone(lambda: v1.read_namespaced_service_account(name, ns), f"sa/{name}")
        except Exception as e:
            log(f"list serviceaccounts: {e}")

    def del_and_wait_roles():
        try:
            items = rbac_v1.list_namespaced_role(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    rbac_v1.delete_namespaced_role(name, ns, body=opts_bg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete role/{name}: {e}")
                wait_gone(lambda: rbac_v1.read_namespaced_role(name, ns), f"role/{name}")
        except Exception as e:
            log(f"list roles: {e}")

    def del_and_wait_rolebindings():
        try:
            items = rbac_v1.list_namespaced_role_binding(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    rbac_v1.delete_namespaced_role_binding(name, ns, body=opts_bg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete rolebinding/{name}: {e}")
                wait_gone(lambda: rbac_v1.read_namespaced_role_binding(name, ns), f"rolebinding/{name}")
        except Exception as e:
            log(f"list rolebindings: {e}")

    def del_and_wait_pvcs():
        try:
            items = v1.list_namespaced_persistent_volume_claim(ns, label_selector=ls).items
            for it in items:
                name = it.metadata.name
                try:
                    v1.delete_namespaced_persistent_volume_claim(name, ns, body=opts_fg)
                except ApiException as e:
                    if e.status != 404:
                        log(f"delete pvc/{name}: {e}")
                wait_gone(lambda: v1.read_namespaced_persistent_volume_claim(name, ns), f"pvc/{name}")
        except Exception as e:
            log(f"list pvcs: {e}")

    # Order: controllers first, then workload pods, then services/config, then RBAC/storage
    del_and_wait_cronjobs()
    del_and_wait_jobs()
    del_and_wait_deploy()
    del_and_wait_rs()
    del_and_wait_pods()
    del_and_wait_svcs()
    del_and_wait_ingresses()
    del_and_wait_cms()
    del_and_wait_secrets()
    del_and_wait_sas()
    del_and_wait_roles()
    del_and_wait_rolebindings()
    del_and_wait_pvcs()
    log("Force delete sweep finished.")
