def kube_drain(operator_client, host):
    operator_client.reload_state()
    operator_client.set_node_unschedulable(host.ip)
    operator_client.evict_pods_on_node(host.ip)
