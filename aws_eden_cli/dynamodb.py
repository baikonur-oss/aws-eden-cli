import datetime
import json
import logging
import time

import botocore
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()


def describe_remote_state_table(dynamodb, table_name):
    response = dynamodb.describe_table(TableName=table_name)
    table_status = response['Table']['TableStatus']
    return table_status


def create_remote_state_table(dynamodb, table_name):
    try:
        response = dynamodb.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'type',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'name',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'last_updated',
                    'AttributeType': 'S'
                }
            ],
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'type',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'name',
                    'KeyType': 'RANGE'
                },
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName': 'type_name_last_updated_gsi',
                    'KeySchema': [
                        {
                            'AttributeName': 'type_name',
                            'KeyType': 'HASH',
                        },
                        {
                            'AttributeName': 'last_updated',
                            'KeyType': 'RANGE',
                        },
                    ],
                    'Projection': {
                        'ProjectionType': 'ALL',
                    }
                },
            ],
            BillingMode='PAY_PER_REQUEST',
        )
        table_status = response['TableDescription']['TableStatus']
        return table_status

    except Exception as e:
        if hasattr(e, 'response') and 'Error' in e.response:
            logger.error(e.response['Error']['Message'])
            return None
        else:
            logger.error(f"Unknown exception raised: {e}")
            return None


def check_remote_state_table(dynamodb, table_name: str, create_if_not_exist=False):
    try:
        table_status = describe_remote_state_table(dynamodb, table_name)
    except botocore.exceptions.NoCredentialsError:
        logger.error("AWS credentials not found!")
        return False
    except Exception as e:
        if hasattr(e, 'response') and 'Error' in e.response:
            code = e.response['Error']['Code']
            if code == 'ResourceNotFoundException':
                if create_if_not_exist:
                    logger.error(f"Remote state table {table_name} does not exist, creating...")
                    table_status = create_remote_state_table(dynamodb, table_name)
                else:
                    logger.error(f"Remote state table {table_name} does not exist")
                    return False

                if table_status is None:
                    return False
            else:
                logger.error(e.response['Error']['Message'])
                return False

        else:
            logger.error(f"Unknown exception raised: {e}")
            return False

    if table_status == 'DELETING':
        logger.error("Table deletion is in progress, try again later")
        return False

    elif table_status == 'UPDATING':
        logger.error("Table update is in progress, try again later")
        return False

    elif table_status == 'CREATING':
        logger.info("Waiting for table creation...")
        while table_status != 'ACTIVE':
            time.sleep(0.1)
            table_status = describe_remote_state_table(dynamodb, table_name)

    return True


def delete_profile(table, profile_name):
    try:
        table.delete_item(
            Key={
                'type': '_profile',
                'name': profile_name,
            },
        )
    except Exception as e:
        if hasattr(e, 'response') and 'Error' in e.response:
            logger.error(e.response['Error']['Message'])
            return False
        else:
            logger.error(f"Unknown exception raised: {e}")
            return False
    return True


def create_profile(table, profile_name, profile_dict):
    try:
        table.put_item(
            Item={
                'type': '_profile',
                'name': profile_name,
                'profile': json.dumps(profile_dict)
            }
        )
    except Exception as e:
        if hasattr(e, 'response') and 'Error' in e.response:
            logger.error(e.response['Error']['Message'])
            return False
        else:
            logger.error(f"Unknown exception raised: {e}")
            return False
    return True


def fetch_all_environments(table):
    environments = {}

    try:
        r = table.scan()
    except Exception as e:
        if hasattr(e, 'response') and 'Error' in e.response:
            code = e.response['Error']['Code']
            if code == 'ResourceNotFoundException':
                logger.error(f"eden table not found, please create table with "
                             f"\"eden config push\" or \"eden create\" first")
            else:
                logger.error(e.response['Error']['Message'])
            return None
        else:
            logger.error(f"Unknown exception raised: {e}")
            return None

    for item in r['Items']:
        key: str = item.pop('env_name')

        if '$' in key:
            profile_name, env_name = key.split('$')

            if profile_name not in environments:
                environments[profile_name] = []

            item['name'] = env_name
            environments[profile_name].append(item)

    return environments


def fetch_all_profiles(table):
    profiles = {}

    try:
        r = table.query(
            KeyConditionExpression=Key('type').eq('_profile')
        )
    except Exception as e:
        if hasattr(e, 'response') and 'Error' in e.response:
            logger.error(e.response['Error']['Message'])
            return None
        else:
            logger.error(f"Unknown exception raised: {e}")
            return None

    for item in r['Items']:
        logger.info(item)
        profiles += {
            'name': item['name'],
            'profile': item['profile'],
        }

    return profiles


def fetch_profile(table, profile_name):
    try:
        r = table.query(
            KeyConditionExpression=Key('type').eq('_profile') & Key('name').eq(profile_name)
        )
    except Exception as e:
        if hasattr(e, 'response') and 'Error' in e.response:
            logger.error(e.response['Error']['Message'])
            return None
        else:
            logger.error(f"Unknown exception raised: {e}")
            return None

    if 'Items' not in r or len(r['Items']) == 0:
        logger.warning(f"Profile {profile_name} not found in remote table!")
        return None
    elif len(r['Items']) != 1:
        logger.warning(f"Got multiple profiles for profile name {profile_name} from remote table!")
        return None
    elif 'profile' not in r['Items'][0]:
        logger.warning(f"Profile {profile_name} does not contain any parameters!")
        return None

    return r['Items'][0]['profile']


def put_environment(table, profile_name, name, cname):
    try:
        return table.put_item(
            Item={
                'type': profile_name,
                'last_updated_time': str(datetime.datetime.now().timestamp()),
                'endpoint': cname,
                'name': name,
            }
        )
    except Exception as e:
        if hasattr(e, 'response') and 'Error' in e.response:
            logger.error(e.response['Error']['Message'])
            return None
        else:
            logger.error(f"Unknown exception raised: {e}")
            return None


def delete_environment(table, profile_name, name):
    try:
        return table.delete_item(
            Key={
                'type': profile_name,
                'name': name,
            },
        )
    except Exception as e:
        if hasattr(e, 'response') and 'Error' in e.response:
            logger.error(e.response['Error']['Message'])
            return None
        else:
            logger.error(f"Unknown exception raised: {e}")
            return None
