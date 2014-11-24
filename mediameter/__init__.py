import os, ConfigParser

import mediacloud.api
import mediameter.cliff

def get_settings_file_path():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_file_path = os.path.join(current_dir,'../','settings.config')
    return config_file_path

# load the shared settings file
settings = ConfigParser.ConfigParser()
settings.read(get_settings_file_path())

# connect to everything
mc_server = mediacloud.api.WriteableMediaCloud(settings.get('mediacloud','key'))
cliff_server = mediameter.cliff.Cliff(settings.get('cliff','host'),settings.get('cliff','port'))
