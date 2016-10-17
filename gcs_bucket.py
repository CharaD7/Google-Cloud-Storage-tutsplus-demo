import sys
from pprint import pprint

from googleapiclient import discovery
from googleapiclient import http
from oauth2client.client import GoogleCredentials


def create_service():
    credentials = GoogleCredentials.get_application_default()
    return discovery.build('storage', 'v1', credentials=credentials)


def list_buckets(project):
    service = create_service()
    res = service.buckets().list(project=project).execute()
    pprint(res)


def create_bucket(project, bucket_name):
    service = create_service()
    res = service.buckets().insert(
        project=project, body={
            "name": bucket_name
        }
    ).execute()
    pprint(res)


def delete_bucket(bucket_name):
    service = create_service()
    res = service.buckets().delete(bucket=bucket_name).execute()
    pprint(res)


def get_bucket(bucket_name):
    service = create_service()
    res = service.buckets().get(bucket=bucket_name).execute()
    pprint(res)


def print_help():
        print """Usage: python gcs_bucket.py <command>
Command can be:
    help: Prints this help
    list: Lists all the buckets in specified project
    create: Create the provided bucket name in specified project
    delete: Delete the provided bucket name
    get: Get details of the provided bucket name
"""


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] == "help" or \
        sys.argv[1] not in ['list', 'create', 'delete', 'get']:
        print_help()
        sys.exit()
    if sys.argv[1] == 'list':
        if len(sys.argv) == 3:
            list_buckets(sys.argv[2])
            sys.exit()
        else:
            print_help()
            sys.exit()
    if sys.argv[1] == 'create':
        if len(sys.argv) == 4:
            create_bucket(sys.argv[2], sys.argv[3])
            sys.exit()
        else:
            print_help()
            sys.exit()
    if sys.argv[1] == 'delete':
        if len(sys.argv) == 3:
            delete_bucket(sys.argv[2])
            sys.exit()
        else:
            print_help()
            sys.exit()
    if sys.argv[1] == 'get':
        if len(sys.argv) == 3:
            get_bucket(sys.argv[2])
            sys.exit()
        else:
            print_help()
            sys.exit()

