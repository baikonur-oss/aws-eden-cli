import argparse
import json
import logging
import os
import sys
from pathlib import Path

import aws_eden_core.methods as function
import boto3

from . import consts, utils, dynamodb

dynamodb_client = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')

logger = logging.getLogger()


def create_parser():
    parser = argparse.ArgumentParser(description='ECS Dynamic Environment Manager. '
                                                 'Clone Amazon ECS environments easily.')

    subparsers = parser.add_subparsers()

    # eden create
    parser_create = subparsers.add_parser('create', help='Create environment or deploy to existent')
    parser_create.set_defaults(handler=command_create)

    # eden delete
    parser_delete = subparsers.add_parser('delete', help='Delete environment')
    parser_delete.set_defaults(handler=command_delete)

    # eden config *
    parser_config = subparsers.add_parser('config', help='Configure eden')

    # eden config setup
    config_subparsers = parser_config.add_subparsers()
    parser_config_setup = config_subparsers.add_parser('setup',
                                                       help='Setup profiles for other commands')
    parser_config_setup.set_defaults(handler=command_config_setup)

    # eden config check
    parser_config_check = config_subparsers.add_parser('check',
                                                       help='Check configuration file integrity')
    parser_config_check.set_defaults(handler=command_config_check)

    # eden config push
    parser_config_push = config_subparsers.add_parser('push',
                                                      help='Push local profile to DynamoDB for use by eden API')
    parser_config_push.set_defaults(handler=command_config_push)

    # eden config remote_remove
    parser_config_remote_delete = config_subparsers.add_parser('remote-remove',
                                                               help='Remove remote profile from DynamoDB')
    parser_config_remote_delete.set_defaults(handler=command_config_remote_delete)

    # profile vars for no profile or profile override
    for i in [parser_config_setup, parser_create, parser_delete]:
        for p in consts.parameters:
            i.add_argument(p['flag'], type=str, required=False,
                           help=p['help_string'])

    # switches for all subcommands
    for i in [parser_config_setup, parser_config_check,  # local profile config commands
              parser_config_push, parser_config_remote_delete,  # remote commands
              parser_create, parser_delete]:
        i.add_argument('-p', '--profile', type=str, required=False, default='default',
                       help='profile name in eden configuration file')

        i.add_argument('-c', '--config-path', type=str, required=False, default='~/.eden/config',
                       help='eden configuration file path')

        i.add_argument('-v', '--verbose', action='store_true')

    # switches for remote subcommands
    for i in [parser_config_push, parser_config_remote_delete]:
        i.add_argument('--remote-table-name', type=str, required=False, default='eden',
                       help='Remote DynamoDB table name')

    for i in [parser_create, parser_delete]:
        i.add_argument('--name', type=str, required=True, help='Environment name (branch name etc.)')

    parser_create.add_argument('--image-uri', type=str, required=True, help='Image URI to deploy '
                                                                            '(ECR repository path, image name and tag)')

    return parser


def command_config_setup(args: dict):
    setup_logging(args['verbose'])
    config = utils.parse_config(args)
    if config is None:
        return
    config, updated = utils.config_write_overrides(args, config, args['profile'])

    if updated:
        config_file_path = os.path.expanduser(args['config_path'])
        config_file = Path(config_file_path)
        with open(config_file, 'w') as configfile:
            config.write(configfile)
    else:
        logger.error("No parameters to update were given, exiting")


def command_config_check(args):
    setup_logging(args['verbose'])
    config = utils.parse_config(args)
    if config is None:
        return
    config, _ = utils.config_write_overrides(args, config, args['profile'])

    errors = 0
    for profile in config:
        errors += utils.check_profile(config, profile)
    if errors == 0:
        logger.info("No errors found")
    else:
        logger.info(f"Found {errors} errors")


def command_config_push(args):
    setup_logging(args['verbose'])
    profile_name = args['profile']
    config = utils.parse_config(args)
    if config is None:
        return
    config, _ = utils.config_write_overrides(args, config, args['profile'])

    table_name = args['remote_table_name']
    table = dynamodb_resource.Table(table_name)

    status = dynamodb.check_remote_state_table(dynamodb_client, table_name)

    if not status:
        return

    profile_dict = utils.create_envvar_dict(args, config, profile_name)

    status = dynamodb.create_profile(table, profile_name, profile_dict)
    if not status:
        return

    logger.info(f"Successfully pushed profile {profile_name} to DynamoDB table {table_name}")


def command_config_remote_delete(args):
    setup_logging(args['verbose'])
    profile_name = args['profile']
    table_name = args['remote_table_name']
    table = dynamodb_resource.Table(table_name)

    status = dynamodb.delete_profile(table, profile_name)
    if not status:
        return

    logger.info(f"Successfully removed profile {profile_name} from DynamoDB table {table_name}")


def command_create(args: dict, name, image_uri):
    setup_logging(args['verbose'])
    config = utils.parse_config(args)
    if config is None:
        return
    config, _ = utils.config_write_overrides(args, config, args['profile'])

    variables = utils.create_envvar_dict(args, config, args['profile'])
    function.create_env(name, image_uri, variables)


def command_delete(args: dict, name):
    setup_logging(args['verbose'])
    config = utils.parse_config(args)
    if config is None:
        return
    config, _ = utils.config_write_overrides(args, config, args['profile'])

    variables = utils.create_envvar_dict(args, config, args['profile'])
    function.delete_env(name, variables)


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

    if hasattr(args, 'handler'):
        if args.handler == command_create:
            args.handler(args_dict, args.name, args.image_uri)
        elif args.handler == command_delete:
            args.handler(args_dict, args.name)
        else:
            args.handler(args_dict)
    else:
        parser.print_help()
        exit(0)
