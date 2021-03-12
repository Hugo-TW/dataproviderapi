import requests
import yaml
import os

account = ''
harbor_token = ''
k8s_token = ''
harbor_path = ''
project_name = ''
deploy_service_url = ''

config_yaml = 'cookbook/config/config.yaml'
service_yaml = 'cookbook/config/service.yaml'

def read_config():
    global project_name, account, harbor_token, k8s_token
    global harbor_path, deploy_service_url

    with open(config_yaml) as config:
        c = yaml.safe_load(config)
        project_name = c['config_list']['project_name']
        account = c['Harbor_list']['account']
        harbor_token = c['Harbor_list']['token']
        harbor_path = c['Harbor_list']['url']
        k8s_token = c['K8S_list']['token']
    
    deploy_service_url = 'https://10.55.8.214:6443/api/v1/namespaces/' + project_name + '/services'


def gen_service():
    with open(service_yaml, 'r') as service_stream:
        service_info = yaml.safe_load(service_stream)

    service_res = requests.post(deploy_service_url,
                        headers={'Authorization': 'bearer ' + k8s_token, 'Content-Type': 'application/yaml'},
                        data=yaml.dump(service_info), verify=False)
    return service_res


## main method start...
read_config()
logout = os.system('docker logout ' + harbor_path)

login_cmd = "docker login -u '" + account + "' -p " + harbor_token + " " + harbor_path
print(">>> docker login cmd:", login_cmd)
login = os.system(login_cmd)

service_r = gen_service()
if service_r.status_code == 201:
    print('Service created')
elif service_r.status_code == 409:
    print('Service already exist')
else:
    print('Create service error, please check.')
