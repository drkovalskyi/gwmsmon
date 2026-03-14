"""Configuration loader for gwmsmon2."""

import configparser
import os

DEFAULTS = {
    "htcondor": {
        "pool": "cmsgwms-collector-global.fnal.gov:9620",
    },
    "prodview": {
        "basedir": "/var/www/prodview",
    },
    "analysisview": {
        "basedir": "/var/www/analysisview",
    },
    "globalview": {
        "basedir": "/var/www/globalview",
    },
    "poolview": {
        "basedir": "/var/www/poolview",
    },
    "factoryview": {
        "basedir": "/var/www/factoryview",
    },
    "utilization": {
        "timespan": "31",
    },
}


def load(path="/etc/gwmsmon2.conf"):
    """Load configuration from INI file.

    Falls back to defaults for any missing section or key.
    If the file does not exist, returns pure defaults.
    """
    cp = configparser.ConfigParser()

    # Apply defaults
    for section, values in DEFAULTS.items():
        if not cp.has_section(section):
            cp.add_section(section)
        for key, val in values.items():
            cp.set(section, key, val)

    # Override with file if it exists
    if os.path.exists(path):
        cp.read(path)

    return cp
