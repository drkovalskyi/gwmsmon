"""Configuration loader for gwmsmon."""

import configparser
import os

DEFAULTS = {
    "htcondor": {},
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
        "fetch_timeout": "30",
    },
    "utilization": {
        "timespan": "31",
    },
}

# Required keys that must be provided by the config file
REQUIRED = {
    "htcondor": ["pool"],
}


def load(path="/etc/gwmsmon.conf"):
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

    # Validate required keys
    missing = []
    for section, keys in REQUIRED.items():
        for key in keys:
            if not cp.has_option(section, key) or not cp.get(section, key):
                missing.append(f"[{section}] {key}")
    if missing:
        import logging
        logging.getLogger(__name__).warning(
            "config missing required keys: %s", ", ".join(missing))

    return cp
