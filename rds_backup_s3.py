from tracemalloc import Snapshot
import boto3
import os
import boto3
import yaml
import argparse
import datetime
import time as tm
import logging
import json
from botocore.exceptions import ClientError

# Set config file path
CONF_PATH_ENV_KEY = "rds_backup_conf.yml"


# For logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler('rds_utility_tracker.log')
stream_handler = logging.StreamHandler()
stream_formatter = logging.Formatter(
    '%(asctime)-15s p%(process)s {%(pathname)s:%(lineno)d} %(levelname)-8s %(funcName)s  %(message)s')
file_formatter = logging.Formatter(json.dumps(
    {'time': '%(asctime)s', 'level': '%(levelname)s', 'function name ': '%(funcName)s', 'process': 'p%(process)s', 'line no': '%(lineno)d', 'message': '%(message)s'}))
file_handler.setFormatter(file_formatter)
stream_handler.setFormatter(stream_formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

# Function for the database fetching based on the tags

def rds_db_instance(tags):
    # tags = _getProperty(CONF_PATH_ENV_KEY)
    client = boto3.client('rds')
    logging.info(f'Connected with rds resources')
    resource = client.describe_db_instances()
    databases = []
    logging.info(f'Start fetching the database based on the tags .......')
    for i in resource['DBInstances']:
        try:
            if tags['tags'] in i["TagList"]:
                databases.append(i['DBInstanceIdentifier'])
            logging.info(f'Fetched databases based on the tags are {databases}')

        except:
            logging.error(f'No database found based on the tags')

    return databases


# function for fetching the snapshots on the basis of the database
def fetch_db_snapshots_with_db(tags):
    client = boto3.client('rds')
    logging.info(f'Connected with rds resources')
    databases = rds_db_instance(tags)
    resource = client.describe_db_snapshots()
    snapshotsarn = []
    logging.info(f'Start fetching the snapshots based on the database .......')
    for i in resource['DBSnapshots']:
        try:
            a = i['SnapshotCreateTime']
            b = a.date()
            c = datetime.datetime.now().date()
            d = c-b
            for db in databases:
                if (db in i["DBInstanceIdentifier"]) & (d.days < 7):
                    snapshotsarn.append(i['DBSnapshotArn'])
                logging.info(
                    f'Fetched snapshots of {db} are {snapshotsarn}')

        except:
            logging.error(f'No snapshotsarn found based on the database')

    return snapshotsarn


# function for fetching the snapshots on the basis of the tags
def fetch_db_snapshots(tags):
    client = boto3.client('rds')
    logging.info(f'Connected with rds resources')
    resource = client.describe_db_snapshots()
    snapshotsarn = []
    logging.info(f'Start fetching the snapshots based on the tags .......')
    for i in resource['DBSnapshots']:
        try:
            if tags['tags'] in i["TagList"]:
                snapshotsarn.append(i['DBSnapshotArn'])
            logging.info(f'Fetched snapshots based on the tags are {snapshotsarn}')

        except:
            logging.error(f'No snapshotsarn found based on the tags')

    return snapshotsarn

# Export snapshots to S3
def export_snapshot_s3(arguments):
    snapshotsarn = fetch_db_snapshots_with_db(arguments)
    client = boto3.client('rds')
    new = arguments['arguments']
    for snap in snapshotsarn:
        try:
            response = client.start_export_task(
                ExportTaskIdentifier='rds-backup-{}'.format(snap.split(":")[6]),
                SourceArn=snap,
                S3BucketName=new['s3BucketName'],
                IamRoleArn=new['iamRoleArn'],
                KmsKeyId=new['kmsKeyId'],
                ExportOnly=new['exportOnly']
            )
            # print(response)
            logging.info(f'Exporting of {snap} in S3 starting now....')

        except:
            logging.error(f'Exporting of {snap} in S3 is failed')


# Check the property file
def _getProperty(property_file_path):

    try:
        load_property = open(property_file_path)
        parse_yaml = yaml.load(load_property, Loader=yaml.FullLoader)
        logging.info(f'configuration file path found {property_file_path}')
        return parse_yaml

    except FileNotFoundError:
        logging.exception(
            f"unable to find {property_file_path}. Please mention correct property file path.")

    return None


# main function
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    args = parser.parse_args()
    file = _getProperty(CONF_PATH_ENV_KEY)
    export_snapshot_s3(file)
