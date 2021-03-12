from base64 import b64encode
import os
import tempfile

import boto3

page_template = "<html><body><ul>{}</ul></body></html>"
list_template = '<li><a href="{path}">{name}</a>'

S3_BUCKET = os.environ.get("S3_BUCKET")

FILE_TYPES = {".egg", ".whl", ".gz"}


def get_all_packages():
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects')

    packages = set()
    for result in paginator.paginate(Bucket=S3_BUCKET, Delimiter='/'):
        for prefix in result.get('CommonPrefixes'):
            packages.add(prefix.get('Prefix').strip("/"))

    list_items = []
    for package in sorted(packages):
        list_items.append(list_template.format(path=package + "/",
                                               name=package))

    return {
        "isBase64Encoded": False,
        "headers": {"Content-Type": "text/html"},
        "body": page_template.format("\n".join(list_items)),
    }


def get_package(package_name):
    client = boto3.client('s3')
    paginator = client.get_paginator('list_objects')

    files = set()
    prefix = f"{package_name}/{package_name}"
    for result in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for entry in result.get('Contents'):
            filename = entry.get("Key").split("/", maxsplit=1)[1]
            if os.path.splitext(filename)[1] in FILE_TYPES:
                files.add(filename)

    list_items = []
    for filename in sorted(files):
        list_items.append(list_template.format(path=filename,
                                               name=filename))

    return {
        "isBase64Encoded": False,
        "headers": {"Content-Type": "text/html"},
        "body": page_template.format("\n".join(list_items)),
    }


def get_file(path, filename):
    if os.path.splitext(path)[1] not in FILE_TYPES:
        raise Exception("Invalid file requested.")

    client = boto3.client('s3')

    # download file
    with tempfile.TemporaryFile() as f:
        client.download_fileobj(S3_BUCKET, path, f)
        f.seek(0)
        data = b64encode(f.read())

    return {
        "isBase64Encoded": True,
        "headers": {
            "Content-Disposition": f"attachment; filename={filename}",
            "Content-Type": "application/zip, application/octet-stream",
        },
        "body": data.decode(),
    }


def handler(event, context):
    path = event.get("path", "/")[1:]
    try:
        package, filename = path.split("/", maxsplit=1)
    except ValueError:
        package = path
        filename = ""

    if filename:
        return get_file(path, filename)
    elif package:
        return get_package(package)
    else:
        return get_all_packages()
