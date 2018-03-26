import requests
import json


def ambari_list_users():
    """Returns all ambari users"""
    hadoop_manager_ip = __salt__['pnda.hadoop_manager_ip']()
    hadoop_manager_username = __salt__['pnda.hadoop_manager_username']()
    hadoop_manager_password = __salt__['pnda.hadoop_manager_password']()    
    full_uri = 'http://%s:8080/api/v1/users' % (hadoop_manager_ip)
    headers = {'X-Requested-By': hadoop_manager_username}
    auth = (hadoop_manager_username, hadoop_manager_password)
    req = requests.get(full_uri, auth=auth, headers=headers)
    return req.json()

def ambari_user_role_assign(user_name, user_role):
    """Returns True if role is assigned to the user else returns False"""
    hadoop_manager_ip = __salt__['pnda.hadoop_manager_ip']()
    hadoop_manager_username = __salt__['pnda.hadoop_manager_username']()
    hadoop_manager_password = __salt__['pnda.hadoop_manager_password']()
    cluster_name = __grains__['pnda_cluster']
    full_uri = 'http://%s:8080/api/v1/clusters/%s/privileges/' % (hadoop_manager_ip, cluster_name)
    headers = {'X-Requested-By': hadoop_manager_username}
    auth = (hadoop_manager_username, hadoop_manager_password)
    data = {
        "PrivilegeInfo": {
          "permission_name": user_role,
          "principal_name": user_name,
          "principal_type": "USER" 
        }
    }
    print data
    users = ambari_list_users()
    flag = 0
    for user in users['items']:
        print user['Users']['user_name'], user_name
        if user['Users']['user_name'] != user_name:
            continue
        flag = 1
        break
    if not flag:
        return False
    req = requests.post(full_uri, auth=auth, headers=headers, data=json.dumps(data))
    return True if req.status_code == 201 else False

def ambari_group_role_assign(group_name, group_role):
    """Returns True if role is assigned to the group else returns False"""
    hadoop_manager_ip = __salt__['pnda.hadoop_manager_ip']()
    hadoop_manager_username = __salt__['pnda.hadoop_manager_username']()
    hadoop_manager_password = __salt__['pnda.hadoop_manager_password']()
    cluster_name = __grains__['pnda_cluster']
    full_uri = 'http://%s:8080/api/v1/clusters/%s/privileges/' % (hadoop_manager_ip, cluster_name)
    headers = {'X-Requested-By': hadoop_manager_username}
    auth = (hadoop_manager_username, hadoop_manager_password)
    data = {
        "PrivilegeInfo": {
          "permission_name": group_role,
          "principal_name": group_name,
          "principal_type": "GROUP"
        }
    }
    print data
    users = ambari_list_users()
    flag = 0
    for user in users['items']:
        print user['Users']['user_name'], user_name
        if user['Users']['user_name'] != user_name:
            continue
        flag = 1
        break
    if not flag:
        return False
    req = requests.post(full_uri, auth=auth, headers=headers, data=json.dumps(data))
    return True if req.status_code == 201 else False

