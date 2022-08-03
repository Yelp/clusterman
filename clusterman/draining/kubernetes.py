import colorlog

from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector

log = colorlog.getLogger(__name__)


def drain(connector: KubernetesClusterConnector, agent_id: str) -> bool:
    """Cordons and safely evicts all tasks from a given node.
    :param hostnames: a single hostname to drain (as would be passed to kubectl drain)
    :returns: None
    """
    log.info(f"Preparing to drain {agent_id}...")
    return connector.drain_node(agent_id)
