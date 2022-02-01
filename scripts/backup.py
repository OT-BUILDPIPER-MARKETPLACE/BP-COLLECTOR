#!/usr/bin/env python3

import os, boto3,yaml, argparse,logging,json,datetime
from botocore.exceptions import ClientError
from datetime import datetime, timedelta

CONF_PATH_ENV_KEY = "CONF_PATH"

LOGGER = logging.getLogger("imported_module")
LOGGER.setLevel(logging.INFO)

FILE_HANDLER = logging.FileHandler('backup_manager.log')
STREAM_HANDLER = logging.StreamHandler()

STREAM_FORMATTER = logging.Formatter(json.dumps(
    {'time': '%(asctime)s', 'level': '%(levelname)s', 'function name ':'%(funcName)s','process': 'p%(process)s','line no':'%(lineno)d','message': '%(message)s'}))
FILE_FORMATTER = logging.Formatter(json.dumps(
    {'time': '%(asctime)s', 'level': '%(levelname)s', 'function name ':'%(funcName)s','process': 'p%(process)s','line no':'%(lineno)d','message': '%(message)s'}))

FILE_HANDLER.setFormatter(FILE_FORMATTER)
STREAM_HANDLER.setFormatter(STREAM_FORMATTER)
LOGGER.addHandler(FILE_HANDLER)
LOGGER.addHandler(STREAM_HANDLER)

def _getProperty(property_file_path):
    try:
        load_property = open(property_file_path)
        parse_yaml = yaml.load(load_property, Loader=yaml.FullLoader)
      
        logging.info(f'configuration file path found {property_file_path}')
        return parse_yaml

    except FileNotFoundError:
        logging.exception(f"unable to find {property_file_path}. Please mention correct property file path.")  
    return None


def _is_elasticache_conf_valid(elasticache_backup_conf):

    mandatory_fields = ['CacheClusterId','SnapshotSource','max_snapshots','backup_duration','s3bucket']
    is_valid = True

    for field in mandatory_fields:
        if field not in elasticache_backup_conf:
            is_valid = False
            LOGGER.error(f'{field} is missing in Elasticache backup configuration. Please add this field in conf file')
            break

        else:
            if not elasticache_backup_conf[field]:
                LOGGER.error(f'Found {field} in Elasticache backup configuration but value is empty. Please add value for {field} in conf file')
                is_valid = False
                break
            else:
                LOGGER.info(f'Found {field} in Elasticache backup configuration')

    return is_valid       

def _backupFactory(properties, aws_profile, args): 
    
    try:
        
        LOGGER.info(f'Connecting to AWS.')
        
        if aws_profile:
            session = boto3.Session(profile_name=aws_profile)
        else:
            session = boto3.Session()

        LOGGER.info(f'Connection to AWS established.')

        for property in properties:
            if property == "elasticache_backup_conf":

                LOGGER.info(f'Found Elasticache backup configuration')
                elasticache_backup_conf = properties['elasticache_backup_conf']
    
                if elasticache_backup_conf:
                    LOGGER.info(f'Validating conf now.')
                    if _is_elasticache_conf_valid(elasticache_backup_conf):
                        LOGGER.info(f'Configuration file is valid')
                        redis_client = session.client("elasticache", region_name=properties['region'])
                        snapshots = _list_snapshots(elasticache_backup_conf, redis_client)
                        if snapshots:
                            _copy_snaphots_to_s3(snapshots, elasticache_backup_conf, redis_client)
                        else:
                            continue
                    else:
                        LOGGER.error('Conf Validation failed. Please fix it')
                        

    
    except ClientError as e:
        if "An error occurred (AuthFailure)" in str(e):
            raise Exception('AWS Authentication Failure!!!! .. Please mention valid AWS profile in property file or use valid IAM role ').with_traceback(e.__traceback__)    
        else:
            raise e 


def _copy_snaphots_to_s3(snapshots, elasticache_backup_conf, redis_client):

    backupafter = datetime.now() - timedelta(days=elasticache_backup_conf['backup_duration'])
    LOGGER.info(f"filtering snapshot after {backupafter}")
    for snapshot in snapshots:
        for node_snapshot in snapshot['NodeSnapshots']:
            if  node_snapshot['SnapshotCreateTime'].date() >= backupafter.date():
                LOGGER.info(f"proceeding with Snapshot {snapshot['SnapshotName']}")
                try:
                    redis_client.copy_snapshot(SourceSnapshotName=snapshot['SnapshotName'],
                    TargetSnapshotName=snapshot['SnapshotName'],
                    TargetBucket=elasticache_backup_conf['s3bucket']
                    )
                    LOGGER.info(f"Snapshot {snapshot['SnapshotName']} pushed in S3 {elasticache_backup_conf['s3bucket']} bucket ")

                except ClientError as e:
                    if "InvalidParameterValue" in str(e) and "already contains an object with key" in str(e):
                        LOGGER.error(f'Failed to copy as snaphot is already present in S3 bucket. Ignoring... Detailed Error: {e} ')
                    else:
                        raise  Exception(f'Failed due to AWS Client error. Error Msg: {e}').with_traceback(e.__traceback__)
                except Exception as e:
                    raise  e


def _list_snapshots(elasticache_backup_conf, redis_client):

    CacheClusterId = elasticache_backup_conf['CacheClusterId']
    snapshots_list = redis_client.describe_snapshots(
        CacheClusterId=elasticache_backup_conf['CacheClusterId'],
        SnapshotSource=elasticache_backup_conf['SnapshotSource'],
        MaxRecords=elasticache_backup_conf['max_snapshots'],
        ShowNodeGroupConfig=True|False
        )       
    snapshots = snapshots_list['Snapshots']
    if not snapshots:
        LOGGER.error("Snapshots not found")
        return None
    else:
        LOGGER.info(f"{len(snapshots)} Snapshots found")
        return snapshots
        
                        
def _backupManager(args):

    LOGGER.info(f'Fetching properties from conf file: {args.property_file_path}.')

    properties = _getProperty(args.property_file_path)

    LOGGER.info(f'Properties fetched from conf file.')

    if properties:
        if "aws_profile" in properties:
            aws_profile = properties['aws_profile']
        else:
            aws_profile = None
        _backupFactory(properties, aws_profile, args)



if __name__ == "__main__":    
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--property-file-path", help="Provide path of property file",
                        default=os.environ[CONF_PATH_ENV_KEY], type=str)
    args = parser.parse_args()
    _backupManager(args)