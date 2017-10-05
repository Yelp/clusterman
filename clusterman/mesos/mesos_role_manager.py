import requests
import yaml

from clusterman.exceptions import MesosRoleManagerError


class MesosRoleManager:
    def __init__(self, name, services_file, master_service_label):
        self.name = name
        with open(services_file) as f:
            services = yaml.load(f)
        self.api_endpoint = 'http://{host}:{port}/api/v1'.format(
            host=services[master_service_label]['host'],
            port=services[master_service_label]['port'],
        )

    def _agents(self):
        response = requests.post(self.api_endpoint, data={'type': 'GET_AGENTS'})
        if not response.ok:
            raise MesosRoleManagerError(f'Could not get instances from Mesos master:\n{response.text}')

        for agent in response.json()['get_agents']['agents']:
            for attr in agent['agent_info']['attributes']:
                if attr['name'] == 'role' and self.name == attr['text']['value']:
                    yield agent
                    break  # once we've generated a valid agent, don't need to loop through the rest of its attrs
