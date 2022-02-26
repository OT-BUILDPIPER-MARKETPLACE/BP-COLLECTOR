#!/usr/bin/env python3

try:
    import configparser
except:
    from six.moves import configparser
import sys, os, argparse, logging, yaml, json, datetime,time as tm
import json_log_formatter
import boto3
from botocore.exceptions import ClientError
from otawslibs import generate_aws_session , aws_resource_tag_factory , aws_ec2_actions_factory , aws_rds_actions_factory
from otfilesystemlibs import yaml_manager

SCHEULE_ACTION_ENV_KEY = "SCHEDULE_ACTION"
CONF_PATH_ENV_KEY = "CONF_PATH"
LOG_PATH = "/var/log/ot/aws-resource-scheduler.log"

FORMATTER = json_log_formatter.VerboseJSONFormatter()
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

FILE_HANDLER = logging.FileHandler(LOG_PATH)
STREAM_HANDLER = logging.StreamHandler(sys.stdout)

FILE_HANDLER.setFormatter(FORMATTER)
STREAM_HANDLER.setFormatter(FORMATTER)

LOGGER.addHandler(FILE_HANDLER)
LOGGER.addHandler(STREAM_HANDLER)
  

def rds_db_instance(client,properties,rds_tags):
    logging.info(f'Connected with rds resources')
    resource = client.describe_db_instances()
    databases = []
    logging.info(f'Start fetching the rds based on the tags .......')
    for i in resource['DBInstances']:
        try:
            if rds_tags in i["TagList"]:
                databases.append(i['DBInstanceIdentifier'])
            logging.info(f'Fetched rds based on the tags are {databases}')

        except:
            logging.error(f'No rds found based on the tags')

    return databases

def fetch_db_snapshots_with_db(client,properties,rds_tags):
    logging.info(f'Connected with rds resources')
    databases = rds_db_instance(client,properties,rds_tags)
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
                if (db in i["DBInstanceIdentifier"]) & (d.days < 1):
                    snapshotsarn.append(i['DBSnapshotArn'])
            logging.info(f'Fetched snapshots of {db} are {snapshotsarn}')

        except:
            logging.error(f'No snapshotsarn found based on the database')

    return snapshotsarn


def export_snapshot_s3(client,properties,rds_tags,snapshotsarn):
    new = properties['services']['rds']['arguments']
    for snap in snapshotsarn:
        try:
            response = client.start_export_task(
                ExportTaskIdentifier='rds-backups-{}'.format(snap.split(":")[7]),
                SourceArn=snap,
                S3BucketName=new['s3BucketName'],
                IamRoleArn=new['iamRoleArn'],
                KmsKeyId=new['kmsKeyId'],
            )
            logging.info(f'Exporting of {snap} in S3 starting now....')

        except:
            logging.error(f'Exporting of {snap} in S3 is failed')

def _awsResourceManagerFactory(properties, aws_profile, args):

    instance_ids = []
    
    try:
        
        LOGGER.info(f'Connecting to AWS.')

        if aws_profile:
            session = generate_aws_session._create_session(aws_profile)
        else:
            session = generate_aws_session._create_session()

        LOGGER.info(f'Connection to AWS established.')
        print(properties['services'])
        for property in properties['services']:
             
            if property == "rds":

                LOGGER.info(f'Reading RDS tags')

                for tag in properties['services']['rds']:
                    if tag == "tags":
                        rds_tags = properties['services']['rds']['tags']
                    else:
                        rds_tags = properties['tags']

                if rds_tags:

                    LOGGER.info(f'Found RDS tags details for filtering : {rds_tags}')

                    rds_client = session.client("rds", region_name=properties['region'])

                    LOGGER.info(f'Scanning AWS RDS resources snapshots in {properties["region"]} region based on tags {rds_tags} provided')

                    snapshotsarn = fetch_db_snapshots_with_db(rds_client,properties,rds_tags)

                    if snapshotsarn:

                        LOGGER.info(f'Found AWS RDS resources {snapshotsarn} in  {properties["region"]} region based on tags provided: {rds_tags}',extra={"snapshotsarn": snapshotsarn})

                        if os.environ[SCHEULE_ACTION_ENV_KEY] == "export":

                            export_snapshot_s3(rds_client,properties,rds_tags,snapshotsarn)                       

                        else:
                            logging.error(f"{SCHEULE_ACTION_ENV_KEY} env not set")
                    
                    else:
                        LOGGER.warning(f'No RDS instances snapshots found on the basis of tag filters provided in conf file in region {properties["region"]} ')
                else:
            
                    LOGGER.warning(f'Found rds_tags key in config file but no RDS tags details mentioned for filtering')

            else:
                LOGGER.info("Scanning AWS service details in config")
                
    except ClientError as e:
        if "An error occurred (AuthFailure)" in str(e):
            raise Exception('AWS Authentication Failure!!!! .. Please mention valid AWS profile in property file or use valid IAM role ').with_traceback(e.__traceback__)    
        else:
            raise e
    except KeyError as e:
        raise Exception(f'Failed fetching env {SCHEULE_ACTION_ENV_KEY} value. Please add this env variable').with_traceback(e.__traceback__)    


def _exportResources(args):

    LOGGER.info(f'Fetching properties from conf file: {args.property_file_path}.')

    yaml_loader = yaml_manager.getYamlLoader()
    properties = yaml_loader._loadYaml(args.property_file_path)

    LOGGER.info(f'Properties fetched from conf file.')

    if properties:
        if "aws_profile" in properties:
            aws_profile = properties['aws_profile']
        else:
            aws_profile = None
        
        _awsResourceManagerFactory(properties, aws_profile, args)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--property-file-path", help="Provide path of property file", default = os.environ[CONF_PATH_ENV_KEY], type=str)
    args = parser.parse_args()
    _exportResources(args)
    



