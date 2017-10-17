import random


def allocated_cpu_resources(agent):
    for resource in agent['agent_info'].get('allocated_resources', []):
        if resource['name'] == 'cpus':
            return resource['scalar']['value']
    return 0


def find_largest_capacity_market(markets, threshold=float('inf')):
    try:
        return max(
            ((m, c) for m, c in markets.items() if c <= threshold),
            key=lambda mc: (mc[1], random.random()),
        )
    except ValueError:
        return None, 0
