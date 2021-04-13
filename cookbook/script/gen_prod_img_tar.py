import subprocess
import os
import sys
import re
import time

image_name = str(sys.argv[1])
image_tag = subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode('utf-8').rstrip()[:5]
cur_img_name = image_name + ':' + image_tag
print(">>> current image and tag:", cur_img_name)

image_list = subprocess.check_output(['docker', 'images', image_name]).decode('utf-8').split("\n")
regex = re.compile(r'^V(\d+)') # reg for start with 'V' and followed with number
filter_list = []
prod_tag = ''
for i in range(len(image_list)):
    img_ele = image_list[i].split()[0:2]   # get first three element, ex: 'kafkaconsumer', '035b5', '9b5587a2cdc4'
    if len(img_ele) == 0:
        continue
    if regex.search(img_ele[1]):
        if image_tag in img_ele[1]:   # 035b5 in V2_035b5
            prod_tag = img_ele[1]
        img_ele.insert(0, int(img_ele[1].split('_')[0][1:])) # ex: [1, 'kafkaconsumer', 'V1_f9e3d']
        filter_list.append(img_ele)
        print(img_ele)

prod_img_name = ''
tar_file = ''
if len(prod_tag) != 0:
    tar_file = image_name + '_' + prod_tag + '.tar'
    # if prod_tag and tar file already exist, just show download url
    if os.path.isfile('/home/gitlab-runner/file/' + tar_file):
        print("download:", "http://10.55.99.101:3000/" + tar_file)
        exit(0)
    # if prod_tag exist but tar file not exist, use origin prod_tag to concate image name
    else:
        prod_img_name = image_name + ':' + prod_tag
        print(prod_img_name)

# if prod_img_name not set, use new version number to add new tag
if len(prod_img_name) == 0:
    cur_ver = sorted(filter_list, reverse=True)[0][0]
    prod_tag = 'V' + str(cur_ver + 1) + '_' + image_tag
    prod_img_name = image_name + ':' + prod_tag
    print(prod_img_name)

    tag_cmd = 'docker tag ' + cur_img_name + ' ' + prod_img_name
    print(tag_cmd)
    res = os.system(tag_cmd)

who_output = subprocess.check_output(['whoami'])
print(who_output)

tar_file = image_name + '_' + prod_tag + '.tar'
tar_cmd = 'docker save -o /home/gitlab-runner/file/' + tar_file + ' ' + prod_img_name
print(tar_cmd)
print("start tar image...", time.ctime())
res = os.system(tar_cmd)
print("tar image done!", time.ctime())

print("download:", " http://10.55.99.101:3000/" + tar_file + " ")
