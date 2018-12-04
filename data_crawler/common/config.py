import logging.config
import os

import yaml

DEFAULT_CONFIG_PATH_TO_ROOT = 'config/config.yaml'
LOGGING_CONF = 'config/logging.yaml'


class DCConfig:
    conf = None
    root_dir = None

    @classmethod
    def load(cls, config_path=DEFAULT_CONFIG_PATH_TO_ROOT):
        if config_path is None:
            config_path = DEFAULT_CONFIG_PATH_TO_ROOT
        try:
            cls.conf = yaml.load(open(config_path, 'r'))
        except Exception as e:
            raise FileNotFoundError('Invalid config file ' + str(e))
        cls.set_root_dir(cls.get_working_dir_opt())
        cls.logger = logging.getLogger('dc')

    @classmethod
    def check_input_config(cls):
        pass

    @classmethod
    def get_working_dir_opt(cls):
        return cls.conf['data_crawler'].get('targeta', 'project')

    @classmethod
    def get_rdf_type(cls):
        return cls.conf['data_crawler'].get('rdf_output', 'turtle')

    @classmethod
    def get_logging_level(cls):
        return cls.conf['logging'].get('level', 'WARN')

    @classmethod
    def get_schedule(cls):
        return cls.conf['data_crawler'].get('scheduling', 'daily')

    @classmethod
    def set_root_dir(cls, working_dir_opt):
        if working_dir_opt == 'project':
            cls.root_dir = os.path.join(os.path.dirname(os.path.realpath(__file__)), "..", "..")
        elif working_dir_opt == 'current':
            cls.root_dir = os.getcwd()
        else:
            if os.path.isdir(working_dir_opt):
                cls.root_dir = working_dir_opt
            else:
                logging.getLogger('dp').error('Configuration "[data_crawler][target]" is not valid.')
                cls.root_dir = os.getcwd()

    @classmethod
    def get_def_conf_path(cls):
        return DEFAULT_CONFIG_PATH_TO_ROOT


def logging_init():
    if not os.path.isfile(LOGGING_CONF):
        print("Could not load logging configuration at destination: "
              + str(os.path.join(os.getcwd(), LOGGING_CONF)))
        raise FileNotFoundError('Missing ' + str(LOGGING_CONF))
    logging.config.dictConfig(yaml.safe_load(open(LOGGING_CONF, 'r').read()))


