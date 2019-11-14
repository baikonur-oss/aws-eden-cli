import argparse
import configparser
import json
import logging
import os
import sys
import time
from pathlib import Path

import boto3
import botocore
from botocore.exceptions import ClientError

from . import consts
import aws_eden_core.methods as function

parameters = consts.parameters
logger = logging.getLogger()


def read_config(path):
    config = configparser.ConfigParser()
    config.read(path)
    return config


def create_parser():
    parser = argparse.ArgumentParser(description='ECS Dynamic Environment Manager. '
                                                 'Clone Amazon ECS environments easily.')

    subparsers = parser.add_subparsers()

    parser_config = subparsers.add_parser('config', help='Configure eden')
    parser_config.set_defaults(handler=command_config)
    parser_config.add_argument('--check', action='store_true',
                               help='Check configuration file integrity')
    parser_config.add_argument('--push', action='store_true',
                               help='Push local profile to DynamoDB for use by eden API')
    parser_config.add_argument('--remote-table-name', type=str, required=False, default='eden',
                               help='profile name in eden configuration file')

    parser_create = subparsers.add_parser('create', help='Create environment or deploy to existent')
    parser_create.set_defaults(handler=command_create)

    parser_delete = subparsers.add_parser('delete', help='Delete environment')
    parser_delete.set_defaults(handler=command_delete)

    for i in [parser_config, parser_create, parser_delete]:
        for p in parameters:
            i.add_argument(p['flag'], type=str, required=False,
                           help=p['help_string'])

        i.add_argument('-p', '--profile', type=str, required=False, default='default',
                       help='profile name in eden configuration file')

        i.add_argument('-c', '--config-path', type=str, required=False, default='~/.eden/config',
                       help='eden configuration file path')

        i.add_argument('-v', '--verbose', action='store_true')

    for i in [parser_create, parser_delete]:
        i.add_argument('--name', type=str, required=True, help='Environment name (branch name etc.)')

    parser_create.add_argument('--image-uri', type=str, required=True, help='Image URI to deploy '
                                                                            '(ECR repository path, image name and tag)')

    return parser


def command_config(args: dict):
    path = os.path.expanduser('~/.eden')
    if not os.path.exists(path):
        os.makedirs(path)

    path = os.path.expanduser(args['config_path'])

    config_file = Path(path)
    if not config_file.is_file():
        logger.info(f"Config file {path} is empty")
        return

    config = read_config(path)

    updated = False

    profile_name = args['profile']

    if profile_name not in config:
        config[profile_name] = {}

    for parameter in parameters:
        key = parameter['name']

        if key not in args:
            continue
        value = args[key]

        if value is None:
            continue

        logger.info(f"Setting {key} to {value} in profile {profile_name}")
        config[profile_name][key] = value
        updated = True

    if updated:
        with open(path, 'w') as configfile:
            config.write(configfile)
    else:
        if not args['check'] and not args['push']:
            logger.error("No parameters to update were given, exiting")

    if args['check']:
        path = os.path.expanduser('~/.eden')
        if not os.path.exists(path):
            logger.error(f"Path ~/.eden does not exist")
            exit(-1)

        path = os.path.expanduser(args['config_path'])
        config = read_config(path)

        errors = 0
        for profile in config:
            errors += check_profile(config, profile)

        if errors == 0:
            logger.info("No errors found")
        else:
            logger.info(f"Found {errors} errors")

    elif args['push']:
        dynamodb_client = boto3.client('dynamodb')
        dynamodb_resource = boto3.resource('dynamodb')
        table_name = args['remote_table_name']

        status = check_remote_state_table(dynamodb_client, table_name)

        if not status:
            return

        table = dynamodb_resource.Table(table_name)
        table.put_item(
            Item={
                'env_name': f"_profile_{profile_name}",
                'profile':  json.dumps(create_envvar_dict(args, config))
            }
        )
        logger.info(f"Successfully pushed profile {profile_name} to DynamoDB")


def check_remote_state_table(dynamodb, table_name):
    try:
        table_status = describe_remote_state_table(dynamodb, table_name)
    except botocore.exceptions.NoCredentialsError:
        logger.error("AWS credentials not found!")
        return False
    except Exception as e:
        if hasattr(e, 'response') and 'Error' in e.response:
            code = e.response['Error']['Code']
            if code == 'ResourceNotFoundException':
                table_status = create_remote_state_table(dynamodb, table_name)
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


def describe_remote_state_table(dynamodb, table_name):
    response = dynamodb.describe_table(TableName=table_name)
    table_status = response['Table']['TableStatus']
    return table_status


def create_remote_state_table(dynamodb, table_name):
    try:
        response = dynamodb.create_table(
            AttributeDefinitions=[
                {
                    'AttributeName': 'env_name',
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
                    'AttributeName': 'env_name',
                    'KeyType':       'HASH'
                },
            ],
            GlobalSecondaryIndexes=[
                {
                    'IndexName':  'env_name_last_updated_gsi',
                    'KeySchema':  [
                        {
                            'AttributeName': 'env_name',
                            'KeyType':       'HASH',
                        },
                        {
                            'AttributeName': 'last_updated',
                            'KeyType':       'RANGE',
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


def check_profile(config, profile):
    errors = 0
    if profile == "DEFAULT":
        logger.debug("Skipping ConfigParser DEFAULT profile (comes up even if not in file)")
        return 0

    if profile not in config:
        logger.error(f"Profile {profile} is not in config file")
        errors += 1
        return errors

    for parameter in parameters:
        key = parameter['name']

        if key not in config[profile]:
            logger.error(f"Necessary key {key} is not provided for profile {profile}")
            errors += 1
            continue
        value = config[profile][key]

        if value is None:
            logger.error(f"Necessary key {key} is None for profile {profile}")
            errors += 1
            continue

        if not parameter['validator'](value):
            logger.error(f"Validation failed for key {key} in profile {profile}")
            errors += 1
            continue
    return errors


def command_create(args: dict, event: dict):
    path = os.path.expanduser('~/.eden')
    if not os.path.exists(path):
        os.makedirs(path)

    path = os.path.expanduser(args['config_path'])
    config = read_config(path)

    variables = create_envvar_dict(args, config)
    function.create_env(event['branch'], event['image_uri'], variables)


def command_delete(args: dict, event: dict):
    path = os.path.expanduser('~/.eden')
    if not os.path.exists(path):
        os.makedirs(path)

    path = os.path.expanduser(args['config_path'])
    config = read_config(path)

    variables = create_envvar_dict(args, config)
    function.delete_env(event['branch'], variables)


def create_envvar_dict(args, config):
    variables = {}
    profile_name = args['profile']

    for p in parameters:
        parameter_name = p['name']

        if parameter_name in args:
            if args[parameter_name] is not None:
                variables[p['envvar_name']] = args[parameter_name]
                continue
        if profile_name not in config or parameter_name not in config[profile_name]:
            logger.error(f"Necessary parameter {parameter_name} not found in profile {profile_name} "
                         f"and is not provided as an argument")
            exit(-1)
        else:
            variables[p['envvar_name']] = config[profile_name][parameter_name]

    return variables


def setup_logging(debug):
    handler = logging.StreamHandler(sys.stdout)
    if debug:
        handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '%(asctime)s.%(msecs)-3dZ %(levelname)-8s [%(pathname)s:%(funcName)s:%(lineno)d] %(message)s ',
            '%Y-%m-%dT%H:%M:%S'
        )
        handler.setFormatter(formatter)
    else:
        handler.setLevel(logging.INFO)

    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("s3transfer").setLevel(logging.WARNING)

    logger.addHandler(handler)


def main(args=sys.argv):
    parser = create_parser()
    args = parser.parse_args(args=args)
    args_dict = vars(args)

    setup_logging(args_dict['verbose'])

    if hasattr(args, 'handler'):
        if args.handler != command_config:
            # validators.check_cirn(args_dict['image_uri'])

            event = {
                'branch':    args_dict['name'],
                'image_uri': args_dict['image_uri'] if 'image_uri' in args_dict else None,
            }

            args.handler(args_dict, event)
        else:
            args.handler(args_dict)
    else:
        parser.print_help()
        exit(0)
