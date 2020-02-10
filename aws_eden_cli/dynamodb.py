import datetime
import json
import logging
import time

import botocore
import boto3
from boto3.dynamodb.conditions import Key

logger = logging.getLogger()


class DynamoDBState:
    def __init__(self, table_name: str):
        self.dynamodb_client = boto3.client('dynamodb')
        self.dynamodb_resource = boto3.resource('dynamodb')

        self.table_name = table_name
        self.table = self.dynamodb_resource.Table(table_name)

    def get_table_name(self):
        return self.table_name

    def describe_remote_state_table(self):
        response = self.dynamodb_client.describe_table(TableName=self.table_name)
        table_status = response['Table']['TableStatus']
        return table_status

    def create_remote_state_table(self):
        try:
            response = self.dynamodb_client.create_table(
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
                        'AttributeName': 'type_name',
                        'AttributeType': 'S'
                    },
                    {
                        'AttributeName': 'last_updated',
                        'AttributeType': 'S'
                    }
                ],
                TableName=self.table_name,
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

    def check_remote_state_table(self, auto_create: bool = False):
        try:
            table_status = self.describe_remote_state_table()
        except botocore.exceptions.NoCredentialsError:
            logger.error("AWS credentials not found!")
            return False
        except Exception as e:
            if hasattr(e, 'response') and 'Error' in e.response:
                code = e.response['Error']['Code']
                if code == 'ResourceNotFoundException':
                    if auto_create:
                        logger.info(f"Remote state table {self.table_name} does not exist, creating...")
                        table_status = self.create_remote_state_table()
                    else:
                        logger.error(f"Remote state table {self.table_name} does not exist")
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
                table_status = self.describe_remote_state_table()

        return True

    def delete_profile(self, profile_name):
        try:
            self.table.delete_item(
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

    def create_profile(self, profile_name, profile_dict):
        try:
            self.table.put_item(
                Item={
                    'type': '_profile',
                    'name': profile_name,
                    'type_name': f"_profile_{profile_name}",
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

    def fetch_all_environments(self):
        environments = {}

        try:
            r = self.table.scan()
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
            env_type: str = item.pop('type')
            if env_type == '_profile':
                continue

            if env_type not in environments:
                environments[env_type] = []

            environments[env_type].append(item)

        return environments

    def fetch_all_profiles(self):
        profiles = {}

        try:
            r = self.table.query(
                KeyConditionExpression=Key('type').eq('_profile')
            )
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
            name = item['name']
            profile = item['profile']

            profiles[name] = profile

        return profiles

    def fetch_profile(self, profile_name):
        try:
            r = self.table.get_item(
                Key={
                    'type': '_profile',
                    'name': profile_name,
                }
            )
        except Exception as e:
            if hasattr(e, 'response') and 'Error' in e.response:
                logger.error(e.response['Error']['Message'])
                return None
            else:
                logger.error(f"Unknown exception raised: {e}")
                return None

        if 'Item' not in r:
            logger.warning(f"Profile {profile_name} not found in remote table!")
            return None
        elif 'profile' not in r['Item']:
            logger.warning(f"Profile {profile_name} does not contain any parameters!")
            return None

        profile = r['Item']['profile']

        try:
            profile_json = json.loads(profile)
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error: {e}")
            return None

        return profile_json

    def put_environment(self, profile_name, name, cname):
        try:
            return self.table.put_item(
                Item={
                    'type': profile_name,
                    'name': name,
                    'type_name': f"{type}_{profile_name}",
                    'last_updated_time': str(datetime.datetime.now().timestamp()),
                    'endpoint': cname,
                }
            )
        except Exception as e:
            if hasattr(e, 'response') and 'Error' in e.response:
                logger.error(e.response['Error']['Message'])
                return None
            else:
                logger.error(f"Unknown exception raised: {e}")
                return None

    def delete_environment(self, profile_name, name):
        try:
            return self.table.delete_item(
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
