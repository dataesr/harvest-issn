import gzip
import os
import pandas as pd
import swiftclient
import time
import subprocess
from io import BytesIO, TextIOWrapper
from retry import retry

from project.server.main.logger import get_logger

logger = get_logger(__name__)
SWIFT_SIZE = 10000
key = os.getenv('OS_PASSWORD')
project_name = os.getenv('OS_PROJECT_NAME')
project_id = os.getenv('OS_TENANT_ID')
tenant_name = os.getenv('OS_TENANT_NAME')
username = os.getenv('OS_USERNAME')
user = f'{tenant_name}:{username}'
init_cmd = f"swift --os-auth-url https://auth.cloud.ovh.net/v3 --auth-version 3 \
      --key {key}\
      --user {user} \
      --os-user-domain-name Default \
      --os-project-domain-name Default \
      --os-project-id {project_id} \
      --os-project-name {project_name} \
      --os-region-name GRA"

conn = None
def get_connection() -> swiftclient.Connection:
    global conn
    if conn is None:
        conn = swiftclient.Connection(
            authurl='https://auth.cloud.ovh.net/v3',
            user=user,
            key=key,
            os_options={
                'user_domain_name': 'Default',
                'project_domain_name': 'Default',
                'project_id': project_id,
                'project_name': project_name,
                'region_name': 'GRA'
            },
            auth_version='3'
        )
    return conn

@retry(delay=3, tries=50, backoff=2)
def upload_object(container: str, filename: str, destination: str) -> str:
    if destination is None:
        destination = filename.split('/')[-1]
    logger.debug(f'Uploading {filename} in {container} as {destination}')
    cmd = init_cmd + f' upload {container} {filename} --object-name {destination}' \
                     f' --segment-size 1048576000 --segment-threads 100'
    #os.system(cmd)
    r = subprocess.check_output(cmd, shell=True)
    return f'https://storage.gra.cloud.ovh.net/v1/AUTH_{project_id}/{container}/{destination}'

@retry(delay=3, tries=50, backoff=2)
def download_object(container: str, filename: str, out: str) -> None:
    logger.debug(f'Downloading {filename} from {container} to {out}')
    cmd = init_cmd + f' download {container} {filename} -o {out}'
    #os.system(cmd)
    r = subprocess.check_output(cmd, shell=True)

def delete_object(container: str, filename: str) -> None:
    logger.debug(f'Deleting {filename} from {container}')
    cmd = init_cmd + f' delete {container} {filename}'
    #os.system(cmd)
    r = subprocess.check_output(cmd, shell=True)

@retry(delay=2, tries=50)
def exists_in_storage(container: str, path: str) -> bool:
    try:
        connection = get_connection()
        connection.head_object(container, path)
        return True
    except:
        return False
