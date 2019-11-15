import configparser
import logging
import os
from pathlib import Path

from . import consts

logger = logging.getLogger()


def read_config(path):
    config = configparser.ConfigParser()
    config.read(path)
    return config


def parse_config(args):
    default_config_path = os.path.expanduser('~/.eden')
    if not os.path.exists(default_config_path):
        os.makedirs(default_config_path)

    config_file_path = os.path.expanduser(args['config_path'])
    config_file = Path(config_file_path)
    if not config_file.is_file():
        logger.info(f"Config file {config_file} is empty")
        return None

    return read_config(config_file)


def config_write_overrides(config, args):
    updated = False

    profile_name = args['profile']

    if profile_name not in config:
        config[profile_name] = {}

    for parameter in consts.parameters:
        key = parameter['name']

        if key not in args:
            continue
        value = args[key]

        if value is None:
            continue

        logger.info(f"Setting {key} to {value} in profile {profile_name}")
        config[profile_name][key] = value
        updated = True

    return config, updated


def check_profile(config, profile):
    errors = 0
    if profile == "DEFAULT":
        logger.debug("Skipping ConfigParser DEFAULT profile (comes up even if not in file)")
        return 0

    if profile not in config:
        logger.error(f"Profile {profile} is not in config file")
        errors += 1
        return errors

    for parameter in consts.parameters:
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


def create_envvar_dict(args, config):
    variables = {}
    profile_name = args['profile']

    for parameter in consts.parameters:
        parameter_name = p['name']
        envvar_name = p['envvar_name']

        if parameter_name in args:
            if args[parameter_name] is not None:
                variables[envvar_name] = args[parameter_name]
                continue
        if profile_name not in config or parameter_name not in config[profile_name]:
            logger.error(f"Necessary parameter {parameter_name} not found in profile {profile_name} "
                         f"and is not provided as an argument")
            exit(-1)
        else:
            variables[envvar_name] = config[profile_name][parameter_name]

    return variables
