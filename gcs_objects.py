import sys
from pprint import pprint

from googleapiclient import discovery
from googleapiclient import http
from oauth2client.client import GoogleCredentials


def create_service():
    credentials = GoogleCredentials.get_application_default()
    return discovery.build('storage', 'v1', credentials=credentials)


def list_objects(bucket):
    service = create_service()
    # Create a request to objects.list to retrieve a list of objects.
    fields_to_return = \
        'nextPageToken,items(name,size,contentType,metadata(my-key))'
    req = service.objects().list(bucket=bucket, fields=fields_to_return)

    all_objects = []
    # If you have too many items to list in one request, list_next() will
    # automatically handle paging with the pageToken.
    while req:
        resp = req.execute()
        all_objects.extend(resp.get('items', []))
        req = service.objects().list_next(req, resp)
    pprint(all_objects)


def create_object(bucket, filename):
    service = create_service()
    # This is the request body as specified:
    # http://g.co/cloud/storage/docs/json_api/v1/objects/insert#request
    body = {
        'name': filename,
    }
    with open(filename, 'rb') as f:
        req = service.objects().insert(
            bucket=bucket, body=body,
            # You can also just set media_body=filename, but for the sake of
            # demonstration, pass in the more generic file handle, which could
            # very well be a StringIO or similar.
            media_body=http.MediaIoBaseUpload(f, 'application/octet-stream'))

        resp = req.execute()
    pprint(resp)


def delete_object(bucket, filename):
    service = create_service()
    res = service.objects().delete(bucket=bucket, object=filename).execute()
    pprint(res)


def print_help():
        print """Usage: python gcs_objects.py <command>
Command can be:
    help: Prints this help
    list: Lists all the objects in the specified bucket
    create: Upload the provided file in specified bucket
    delete: Delete the provided filename from bucket
"""


if __name__ == "__main__":
    if len(sys.argv) < 2 or sys.argv[1] == "help" or \
        sys.argv[1] not in ['list', 'create', 'delete', 'get']:
        print_help()
        sys.exit()
    if sys.argv[1] == 'list':
        if len(sys.argv) == 3:
            list_objects(sys.argv[2])
            sys.exit()
        else:
            print_help()
            sys.exit()
    if sys.argv[1] == 'create':
        if len(sys.argv) == 4:
            create_object(sys.argv[2], sys.argv[3])
            sys.exit()
        else:
            print_help()
            sys.exit()
    if sys.argv[1] == 'delete':
        if len(sys.argv) == 4:
            delete_object(sys.argv[2], sys.argv[3])
            sys.exit()
        else:
            print_help()
            sys.exit()

