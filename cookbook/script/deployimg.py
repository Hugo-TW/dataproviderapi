import requests
import json
import yaml
import os
import time
import subprocess
import sys

# Harbor setting start #
account = ''
harbor_token = ''
k8s_token = ''
harbor_path = ''
# Harbor setting end #
image_name = ''
deployment_name = ''
project_name = ''
deploy_yaml = ''
service_yaml = ''
ingress_yaml = ''
pods_status_url = ''
deploy_url = ''
del_url = ''
get_deploy_status_url = ''
deploy_service_url = ''
deploy_ingress_url = ''
harbor_basic_auth = ''


def get_image_info():
    global account, harbor_token, k8s_token, image_name
    global harbor_path, image_name, deployment_name, project_name, deploy_yaml, service_yaml
    global deploy_url, del_url, get_deploy_status_url, pods_status_url, deploy_service_url
    global ingress_yaml, deploy_ingress_url, harbor_basic_auth

    # image_name = 'datacollector'
    image_name = str(sys.argv[1])
    config_yaml = 'cookbook/config/config.yaml'
    deploy_yaml = 'cookbook/config/deploy.yaml'
    service_yaml = 'cookbook/config/service.yaml'
    ingress_yaml = 'cookbook/config/Ingress.yaml'

    with open(config_yaml) as config:
        c = yaml.safe_load(config)
        project_name = c['config_list']['project_name']
        deployment_name = c['config_list']['deployment_name']

        account = c['Harbor_list']['account']
        harbor_token = c['Harbor_list']['token']
        harbor_path = c['Harbor_list']['url']
        harbor_basic_auth = c['Harbor_list']['basic_auth']
        k8s_token = c['K8S_list']['token']
    pods_status_url = 'https://10.55.8.214:6443/api/v1/namespaces/' + project_name + '/pods/'
    deploy_url = 'https://10.55.8.214:6443/apis/apps/v1/namespaces/' + project_name + '/deployments'
    del_url = 'https://10.55.8.214:6443/apis/apps/v1/namespaces/' + project_name + '/deployments/' + deployment_name
    get_deploy_status_url = 'https://10.55.8.214:6443/apis/apps/v1/namespaces/' + project_name + '/deployments/' + deployment_name
    deploy_service_url = 'https://10.55.8.214:6443/api/v1/namespaces/' + project_name + '/services'
    deploy_ingress_url = 'https://10.55.8.214:6443/apis/extensions/v1beta1/namespaces/' + project_name + '/ingresses'


def gen_service():
    with open(service_yaml, 'r') as service_stream:
        service_info = yaml.safe_load(service_stream)
    service_res = requests.post(deploy_service_url,
                        headers={'Authorization': 'bearer ' + k8s_token, 'Content-Type': 'application/yaml'},
                        data=yaml.dump(service_info), verify=False)
    return service_res


def gen_ingress():
    with open(ingress_yaml, 'r') as ingress_stream:
        ingress_info = yaml.safe_load(ingress_stream)
    ingress_res = requests.post(deploy_ingress_url,
                        headers={'Authorization': 'bearer ' + k8s_token, 'Content-Type': 'application/yaml'},
                        data=yaml.dump(ingress_info), verify=False)
    return ingress_res


def get_harbor_tags(current_tag):
    del_tags = []
    url = 'https://'+harbor_path+'/api/repositories/' + project_name + '/' + image_name + '/tags?detail=false'
    headers = {'Authorization': 'Basic '+harbor_basic_auth}

    response = requests.request("GET", url, headers=headers, verify=False)
    res = response.json()
    for i in range(len(res)):
        if res[i]['name'] != current_tag:
            del_tags.append(res[i]['name'])
    return del_tags


def del_harbor_image():
    tag_list = get_harbor_tags(tag)
    for i in range(len(tag_list)):
        url = "https://chbcld.cminl.oa/api/repositories/iamp/"+image_name+"/tags/"+tag_list[i]
        headers = {
            'Authorization': 'Basic '+harbor_basic_auth
        }
        response = requests.request("DELETE", url, headers=headers, verify=False)
        print("Delete images res:", response.reason)


get_image_info()
# Using commit id as docker tag
logout = os.system('docker logout ' + harbor_path)

# If yoy want to use console to debug, you can use this cmd as below:
# login_cmd = "docker login -u " + account + " -p " + harbor_token + " " + harbor_path
# [Important]If yoy want to checkin code to gitlab, you MUST be using this cmd to avoid login fail:
login_cmd = "docker login -u '" + account + "' -p " + harbor_token + " " + harbor_path
print(">>> docker login cmd:", login_cmd)
login = os.system(login_cmd)

tag = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode('utf-8').rstrip()[:5]
tag_cmd = 'docker tag ' + image_name + ':' + tag + ' ' + harbor_path + '/' + project_name + '/' + image_name + ':' + tag
print(">>> docker tag cmd:", tag_cmd)
tag_res = os.system(tag_cmd)

# Push docker image to harbor
push_docker_img_cmd = 'docker push ' + harbor_path + '/' + project_name + '/' + image_name + ':' + tag
res = os.system(push_docker_img_cmd)
if res == 0 and login == 0:
    print('Push success')
    print('-------------Waiting 5 sec for check status --------------')
    time.sleep(5)

    deploy_status = requests.get(get_deploy_status_url, headers={'Authorization': 'bearer ' + k8s_token}, verify=False)
    if deploy_status.status_code == 404:
        print("No deployment")
    elif deploy_status.status_code == 401:
        print("Unauthorized")
    elif deploy_status.status_code == 200:
        print('-------------Deleting deployment --------------')
        del_req = requests.delete(del_url, headers={'Authorization': 'bearer ' + k8s_token}, verify=False)
        print('-------------Waiting 20 sec. --------------')
        time.sleep(20)

    # Gen deployment
    with open(deploy_yaml, 'r') as stream:
        common_deploy = yaml.safe_load(stream)
    common_deploy['spec']['template']['spec']['containers'][0][
        'image'] = harbor_path + '/' + project_name + '/' + image_name + ':' + tag
    r = requests.post(deploy_url, headers={'Authorization': 'bearer ' + k8s_token, 'Content-Type': 'application/yaml'},
                      data=yaml.dump(common_deploy), verify=False)

    # While loop
    while (True):
        index = -1
        time.sleep(2)
        # Get pods status
        req = requests.get(pods_status_url, headers={'Authorization': 'bearer ' + k8s_token}, verify=False)
        my_json = req.content.decode('utf8').replace("'", '"')
        # print(my_json)
        res_data = json.loads(my_json)
        for i in range(len(res_data["items"])):
            if deployment_name in res_data["items"][i]["metadata"]["name"]:
                index = i
                break
        status = res_data["items"][index]["status"]["phase"].lower()
        if status == 'running':
            print(status)
            break
        print(status)
    # Create service
    service_r = gen_service()
    if service_r.status_code == 201:
        print('Service created')
    elif service_r.status_code == 409:
        print('Service already exist')
    else:
        print('Create service error, please check.')

    ingress_r = gen_ingress()
    if ingress_r.status_code == 201:
        print('Ingress created')
    elif ingress_r.status_code == 409:
        print('Ingress already exist')
    else:
        print('Create Ingress error, please check.')

    # Delete harbor images
    del_harbor_image()

else:
    print('Push or login Fail. Please check your docker image or your login info.')
    sys.exit(1)

