import requests
import json
import yaml
import os
import time
import subprocess
import sys
import copy
import pprint
import requests.packages.urllib3
requests.packages.urllib3.disable_warnings()

pp = pprint.PrettyPrinter(indent=4)

def read_config():
    global image_name, image_tag, deployment_name, project_name, factory_arr
    global harbor_basic_auth, harbor_account, harbor_token, harbor_path, k8s_token
    global pods_status_url, deployments_url, cur_deploy_url, deploy_data

    image_name = str(sys.argv[1])
    image_tag = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode('utf-8').rstrip()[:5]

    config_yaml = 'cookbook/config/config.yaml'
    deploy_yaml = 'cookbook/config/deploy.yaml'

    # Get config variables
    with open(config_yaml) as config:
        c = yaml.safe_load(config)
        project_name = c['config_list']['project_name']
        deployment_name = c['config_list']['deployment_name']
        factory_arr = c['config_list']['factory']

        harbor_basic_auth = c['Harbor_list']['basic_auth']
        harbor_account = c['Harbor_list']['account']
        harbor_token = c['Harbor_list']['token']
        harbor_path = c['Harbor_list']['url']

        k8s_token = c['K8S_list']['token']
    pods_status_url = 'https://10.55.8.214:6443/api/v1/namespaces/' + project_name + '/pods/'
    deployments_url = 'https://10.55.8.214:6443/apis/apps/v1/namespaces/' + project_name + '/deployments'
    cur_deploy_url = deployments_url + '/' + deployment_name

    # Get deployment content
    with open(deploy_yaml, 'r') as deploy:
        deploy_data = yaml.safe_load(deploy)

    ## replace deploy image name
    container = deploy_data['spec']['template']['spec']['containers'][0]
    container['image'] = harbor_path + '/' + project_name + '/' + image_name + ':' + image_tag

def rep_deploy(dep_data, dep_name, FAC):
    data = copy.deepcopy(dep_data)
    data['metadata']['name'] = dep_name
    data['metadata']['namespace'] = project_name

    selector = data['spec']['selector']
    selector['matchLabels']['deploy'] = dep_name

    template = data['spec']['template']
    template['metadata']['labels']['deploy'] = dep_name
    template['spec']['containers'][0]['name'] = dep_name

    if len(FAC) != 0:
        template['spec']['containers'][0]['command'] = ["/bin/sh"]
        template['spec']['containers'][0]['args'] = [FAC + ".sh"]
    # pp.pprint(template)
    return data

def get_harbor_tags(current_tag):
    del_tags = []
    url = 'https://' + harbor_path + '/api/repositories/' + project_name + '/' + image_name + '/tags?detail=false'
    headers = {'Authorization': 'Basic ' + harbor_basic_auth}

    response = requests.request("GET", url, headers=headers, verify=False)
    res = response.json()
    print(res)

    for i in range(len(res)):
        if res[i]['name'] != current_tag:
            del_tags.append(res[i]['name'])
    return del_tags

def del_harbor_image():
    tag_list = get_harbor_tags(image_tag)
    for i in range(len(tag_list)):
        url = 'https://' + harbor_path + '/api/repositories/' + project_name + '/' + image_name + '/tags/' + tag_list[i]
        headers = {'Authorization': 'Basic '+harbor_basic_auth}
        response = requests.request("DELETE", url, headers=headers, verify=False)
        print("Delete images res:", response.reason)


read_config()
logout = os.system('docker logout ' + harbor_path)

# If yoy want to use console to debug, you can use this cmd as below:
# login_cmd = "docker login -u " + harbor_account + " -p " + harbor_token + " " + harbor_path
# [Important]If yoy want to checkin code to gitlab, you MUST be using this cmd to avoid login fail:
login_cmd = "docker login -u '" + harbor_account + "' -p " + harbor_token + " " + harbor_path
print(">>>", login_cmd)
login = os.system(login_cmd)
if login != 0:
    print('>>> docker login fail, exit!'); sys.exit(1)

tag_cmd = 'docker tag ' + image_name + ':' + image_tag + ' ' + harbor_path + '/' + project_name + '/' + image_name + ':' + image_tag
print(">>>", tag_cmd, "\n")
tag_res = os.system(tag_cmd)

# Push docker image to harbor
push_docker_img_cmd = 'docker push ' + harbor_path + '/' + project_name + '/' + image_name + ':' + image_tag
print(">>>", push_docker_img_cmd)
print("---", time.ctime())
res = os.system(push_docker_img_cmd)
if res != 0:
    print('>>> docker push to harbor fail, exit!'); sys.exit(1)
else:
    print("---", time.ctime())
    print('>>> docker push:', res)

if len(factory_arr) == 0:
    factory_arr.append('')

for i in range(len(factory_arr)):
    fac = factory_arr[i].lower()
    deploy_name = deployment_name if len(fac) == 0 else deployment_name + '-' + fac
    deploy_url = deployments_url + '/' + deploy_name

    print('\n>>> deploy url:', deploy_url)
    qry_deploy = requests.get(deploy_url, headers={'Authorization': 'bearer ' + k8s_token}, verify=False)
    print('>>> qry deploy:', qry_deploy)
    if qry_deploy.status_code == 200:
        del_deploy = requests.delete(deploy_url, headers={'Authorization': 'bearer ' + k8s_token}, verify=False)
        print('>>> del deploy:', del_deploy)

    fac_deploy_data = rep_deploy(deploy_data, deploy_name, fac.upper())
    print('>>> create deployment:', deploy_name)
    pp.pprint(fac_deploy_data)
    create_deploy = requests.post(deployments_url, headers={'Authorization': 'bearer ' + k8s_token, 'Content-Type': 'application/yaml'},
                    data=yaml.dump(fac_deploy_data), verify=False)
    print('>>> create deploy:', create_deploy)

# check if pod status is running
time.sleep(5)
while (True):
    index = -1

    # Get pods status
    req = requests.get(pods_status_url, headers={'Authorization': 'bearer ' + k8s_token}, verify=False)
    my_json = req.content.decode('utf8')
    res_data = json.loads(my_json)

    res_items = res_data["items"]
    if len(res_items) == 0:
        time.sleep(2)
        continue

    for i in range(len(res_items)):
        if deployment_name in res_items[i]["metadata"]["name"]:
            index = i; break

    status = res_items[index]["status"]["phase"].lower()
    if status == 'running':
        print('>>> pod status:', status); break

# Delete harbor images
del_harbor_image()
