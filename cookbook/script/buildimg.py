import subprocess
import os
import sys

#image_name = 'datacollector'
image_name = str(sys.argv[1])
print(">>> image name:", image_name)

tag = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode('utf-8').rstrip()[:5]
print(">>> image tag :", tag)

docker_bd_cmd = 'docker build -t '+image_name+':'+tag +' .'  # default file ./Dockerfile
print(">>> build cmd :", docker_bd_cmd)
tag_res = os.system(docker_bd_cmd)

docker_rm_cmd = 'docker image prune -f'
print(">>> prune cmd :", docker_rm_cmd)
rm_res = os.system(docker_rm_cmd)
