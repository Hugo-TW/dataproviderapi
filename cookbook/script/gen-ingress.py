import requests
import yaml
import os

account = ''
harbor_token = ''
k8s_token = ''
harbor_path = ''
project_name = ''
deploy_ingress_url = ''

config_yaml = 'cookbook/config/config.yaml'
ingress_yaml = 'cookbook/config/Ingress.yaml'

def read_config():
    global project_name, account, harbor_token, k8s_token
    global harbor_path, deploy_ingress_url

    with open(config_yaml) as config:
        c = yaml.safe_load(config)
        project_name = c['config_list']['project_name']
        account = c['Harbor_list']['account']
        harbor_token = c['Harbor_list']['token']
        harbor_path = c['Harbor_list']['url']
        k8s_token = c['K8S_list']['token']

    deploy_ingress_url = 'https://10.55.8.214:6443/apis/extensions/v1beta1/namespaces/' + project_name + '/ingresses'


def gen_ingress():
    with open(ingress_yaml, 'r') as ingress_stream:
        ingress_info = yaml.safe_load(ingress_stream)

    ingress_res = requests.post(deploy_ingress_url,
                        headers={'Authorization': 'bearer ' + k8s_token, 'Content-Type': 'application/yaml'},
                        data=yaml.dump(ingress_info), verify=False)
    return ingress_res


## main method start...
read_config()
logout = os.system('docker logout ' + harbor_path)

login_cmd = "docker login -u '" + account + "' -p " + harbor_token + " " + harbor_path
print(">>> docker login cmd:", login_cmd)
login = os.system(login_cmd)

ingress_r = gen_ingress()
if ingress_r.status_code == 201:
    print('Ingress created')
elif ingress_r.status_code == 409:
    print('Ingress already exist')
else:
    print('Create Ingress error, please check.')
