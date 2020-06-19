import argparse
import datetime
import json
import logging
import os
import sys
from pathlib import Path

import aws_eden_core.methods

from . import consts, utils, dynamodb

logger = logging.getLogger()

handlers_remote = []
state = dynamodb.DynamoDBState(consts.DEFAULT_TABLE_NAME)


def create_parser():
    parser = argparse.ArgumentParser(description='ECS Dynamic Environment Manager. '
                                                 'Clone Amazon ECS environments easily.')

    subparsers = parser.add_subparsers()

    parsers = []
    parsers_remote = []

    # eden create
    parser_create = subparsers.add_parser('create', help='Create environment or deploy to existent')
    parser_create.set_defaults(handler=command_create)
    parsers.append(parser_create)
    parsers_remote.append(parser_create)
    handlers_remote.append(command_create)

    # eden delete
    parser_delete = subparsers.add_parser('delete', help='Delete environment')
    parser_delete.set_defaults(handler=command_delete)
    parsers.append(parser_delete)
    parsers_remote.append(parser_delete)
    handlers_remote.append(command_delete)

    # eden list
    parser_ls = subparsers.add_parser('ls', help='List existing environments')
    parser_ls.set_defaults(handler=command_ls)
    parsers.append(parser_ls)
    parsers_remote.append(parser_ls)
    handlers_remote.append(command_ls)

    # eden config *
    parser_config = subparsers.add_parser('config', help='Configure eden')

    # eden config setup
    config_subparsers = parser_config.add_subparsers()
    parser_config_setup = config_subparsers.add_parser('setup',
                                                       help='Setup profiles for other commands')
    parser_config_setup.set_defaults(handler=command_config_setup)
    parsers.append(parser_config_setup)

    # eden config check
    parser_config_check = config_subparsers.add_parser('check',
                                                       help='Check configuration file integrity')
    parser_config_check.set_defaults(handler=command_config_check)
    parsers.append(parser_config_check)

    # eden config push
    parser_config_push = config_subparsers.add_parser('push',
                                                      help='Push local profile to DynamoDB for use by eden API')
    parser_config_push.set_defaults(handler=command_config_push)
    parsers.append(parser_config_push)
    parsers_remote.append(parser_config_push)
    handlers_remote.append(command_config_push)

    # eden config pull
    parser_config_pull = config_subparsers.add_parser('pull',
                                                      help='Pull remote profile to local configuration')
    parser_config_pull.set_defaults(handler=command_config_pull)
    parsers.append(parser_config_pull)
    parsers_remote.append(parser_config_pull)
    handlers_remote.append(command_config_pull)

    # eden config ls
    parser_config_ls = config_subparsers.add_parser('ls',
                                                    help='List remote profiles')
    parser_config_ls.set_defaults(handler=command_config_ls)
    parsers.append(parser_config_ls)
    parsers_remote.append(parser_config_ls)
    handlers_remote.append(command_config_ls)

    # eden config remote_remove
    parser_config_remote_delete = config_subparsers.add_parser('remote-rm',
                                                               help='Delete remote profile from DynamoDB')
    parser_config_remote_delete.set_defaults(handler=command_config_remote_delete)
    parsers.append(parser_config_remote_delete)
    parsers_remote.append(parser_config_remote_delete)
    handlers_remote.append(command_config_remote_delete)

    # profile vars for no profile or profile override
    for i in [parser_config_setup, parser_create, parser_delete]:
        for p in consts.parameters:
            i.add_argument(p['flag'], type=str, required=False,
                           help=p['help_string'])

    # switches for all subcommands
    for i in parsers:
        i.add_argument('-p', '--profile', type=str, required=False, default=consts.DEFAULT_PROFILE_NAME,
                       help='profile name in eden configuration file')

        i.add_argument('-c', '--config-path', type=str, required=False, default='~/.eden/config',
                       help='eden configuration file path')

        i.add_argument('-v', '--verbose', action='store_true')

    # switches for remote subcommands
    for i in parsers_remote:
        i.add_argument('--remote-table-name', type=str, required=False, default=consts.DEFAULT_TABLE_NAME,
                       help='Remote DynamoDB table name')

    for i in [parser_create, parser_delete]:
        i.add_argument('--name', type=str, required=True, help='Environment name (branch name etc.)')

    parser_create.add_argument('--image-uri', type=str, required=True, help='Image URI to deploy '
                                                                            '(ECR repository path, image name and tag)')

    return parser


def command_ls(args_dict: dict):
    setup_logging(args_dict['verbose'])

    environments: dict = state.fetch_all_environments()

    if environments is None:
        return

    if len(environments) == 0:
        logger.info("No environments available")
        return

    for profile_name in environments:
        logger.info(f"Profile {profile_name}:")

        for environment in environments[profile_name]:
            last_updated = datetime.datetime.fromtimestamp(float(environment['last_updated_time']))
            logger.info(f"{environment['name']} {environment['endpoint']} (last updated: {last_updated.isoformat()})")

        logger.info("")

    return


def command_config_ls(args_dict: dict):
    setup_logging(args_dict['verbose'])

    status = state.check_remote_state_table()
    if not status:
        return

    profiles: dict = state.fetch_all_profiles()

    if profiles is None:
        return

    if len(profiles) == 0:
        logger.info("No profiles available")
        return

    for profile_name, profile in profiles.items():
        logger.info(f"Profile {profile_name}:")

        profile_dict = json.loads(profile)

        for k, v in profile_dict.items():
            logger.info(f"{k} = {v}")

        logger.info("")

    return


def command_config_setup(args_dict: dict):
    setup_logging(args_dict['verbose'])
    profile_name = args_dict['profile']

    config = utils.parse_config(args_dict)
    if config is None:
        return

    config, updated = utils.config_write_overrides(args_dict, config, profile_name)
    if config is None:
        return

    if updated:
        config_file_path = os.path.expanduser(args_dict['config_path'])
        config_file = Path(config_file_path)
        with open(config_file, 'w') as configfile:
            config.write(configfile)
    else:
        logger.error("No parameters to update were given, exiting")


def command_config_check(args_dict: dict):
    setup_logging(args_dict['verbose'])
    profile_name = args_dict['profile']

    config = utils.parse_config(args_dict)
    if config is None:
        return

    config, _ = utils.config_write_overrides(args_dict, config, profile_name)
    if config is None:
        return

    errors = 0
    for profile in config:
        errors += utils.check_profile(config, profile)
    if errors == 0:
        logger.info("No errors found")
    else:
        logger.info(f"Found {errors} errors")


def command_config_push(args_dict: dict):
    setup_logging(args_dict['verbose'])
    profile_name = args_dict['profile']

    config = utils.parse_config(args_dict)
    if config is None:
        return

    config, _ = utils.config_write_overrides(args_dict, config, profile_name)
    if config is None:
        return

    status = state.check_remote_state_table(auto_create=True)
    if not status:
        return

    profile_dict = utils.dump_profile(args_dict, config, profile_name)

    status = state.put_profile(profile_name, profile_dict)
    if not status:
        return

    logger.info(f"Successfully pushed profile {profile_name} to DynamoDB table {state.get_table_name()}")


def command_config_pull(args_dict: dict):
    setup_logging(args_dict['verbose'])
    profile_name = args_dict['profile']

    config = utils.parse_config(args_dict)
    if config is None:
        return

    status = state.check_remote_state_table()
    if not status:
        return

    profile = state.fetch_profile(profile_name)
    if not profile:
        return

    config, updated = utils.config_write_overrides(profile, config, profile_name,
                                                   fail_on_missing_non_default_profile=False)

    config_file_path = os.path.expanduser(args_dict['config_path'])
    config_file = Path(config_file_path)
    with open(config_file, 'w') as configfile:
        config.write(configfile)

    logger.info(f"Successfully pulled profile {profile_name} to local configuration")


def command_config_remote_delete(args_dict: dict):
    setup_logging(args_dict['verbose'])
    profile_name = args_dict['profile']

    status = state.check_remote_state_table()
    if not status:
        return

    status = state.delete_profile(profile_name)
    if not status:
        return

    logger.info(f"Successfully removed profile {profile_name} from DynamoDB table {state.get_table_name()}")


def command_create(args_dict: dict):
    name = args_dict['name']
    image_uri = args_dict['image_uri']

    setup_logging(args_dict['verbose'])
    profile_name = args_dict['profile']

    status = state.check_remote_state_table(auto_create=True)
    if not status:
        return

    config = utils.parse_config(args_dict)
    if config is None:
        return

    config, _ = utils.config_write_overrides(args_dict, config, profile_name)
    if config is None:
        return

    profile = utils.dump_profile(args_dict, config, profile_name)

    r = aws_eden_core.methods.create_env(name, image_uri, profile)
    state.put_environment(profile_name, r['name'], r['cname'])


def command_delete(args_dict: dict):
    name = args_dict['name']

    setup_logging(args_dict['verbose'])
    profile_name = args_dict['profile']

    status = state.check_remote_state_table()
    if not status:
        return

    config = utils.parse_config(args_dict)
    if config is None:
        return

    config, _ = utils.config_write_overrides(args_dict, config, profile_name)
    if config is None:
        return

    profile = utils.dump_profile(args_dict, config, profile_name)

    r = aws_eden_core.methods.delete_env(name, profile)
    state.delete_environment(profile_name, r['name'])


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


def main(args=None):
    global state

    if args is None:
        args = sys.argv

    parser = create_parser()
    args = parser.parse_args(args=args)
    args_dict = vars(args)

    if hasattr(args, 'handler'):
        # configure state if working with remote commands,
        # make state inaccessible otherwise
        if args.handler in handlers_remote:
            table_name = args.remote_table_name
            if table_name != consts.DEFAULT_TABLE_NAME:
                state = dynamodb.DynamoDBState(table_name)
        else:
            state = None

        args.handler(args_dict)

    else:
        parser.print_help()
        exit(0)
