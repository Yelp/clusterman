import colorlog

from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector

log = colorlog.getLogger(__name__)


def drain(connector: KubernetesClusterConnector, hostname: str) -> bool:
    """Cordons and safely evicts all tasks from a given node.
    :param hostnames: a single hostname to drain (as would be passed to kubectl drain)
    :returns: None
    """
    log.info(f"Preparing to drain {hostname}...")
    try:
        connector.drain_node(hostname)
        log.info(f"Drained {hostname}!")
        return True
    except Exception:  # TODO: figure out actual k8s exception
        log.exception(f"Unable to drain {hostname}!")
        return False
