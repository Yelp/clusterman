from typing import Optional

import colorlog

from clusterman.kubernetes.kubernetes_cluster_connector import KubernetesClusterConnector

log = colorlog.getLogger(__name__)


def drain(connector: Optional[KubernetesClusterConnector], node_name: str, disable_eviction: bool) -> bool:
    """Cordons and evicts/deletes all tasks from a given node.
    :param node_name: a single node name to drain (as would be passed to kubectl drain)
    :param connector: a kubernetes connector to connect kubernetes API
    :param disable_eviction: Force drain to use delete (ignoring PDBs)
    :returns: bool
    """
    if connector:
        log.info(f"Preparing to drain {node_name}...")
        return connector.drain_node(node_name, disable_eviction)
    else:
        log.info(f"Unable to drain {node_name} (no Kubernetes connector configured)")
        return False


def uncordon(connector: Optional[KubernetesClusterConnector], node_name: str) -> bool:
    """Cordons and safely evicts all tasks from a given node.
    :param node_name: a single node name to uncordon (as would be passed to kubectl uncordon)
    :param connector: a kubernetes connector to connect kubernetes API
    :returns: bool
    """
    if connector:
        log.info(f"Preparing to uncordon {node_name}...")
        return connector.uncordon_node(node_name)
    else:
        log.info(f"Unable to uncordon {node_name} (no Kubernetes connector configured)")
        return False
