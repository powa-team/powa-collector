"""
Simple configuration file handling, as a JSON.
"""
import json
import os
import sys


SAMPLE_CONFIG_FILE = """
{
    "repository": {
        "dsn": "postgresql://powa_user@localhost:5432/powa",
    },
    "debug": false
}

"""

CONF_LOCATIONS = [
    '/etc/powa-collector.conf',
    os.path.expanduser('~/.config/powa-collector.conf'),
    os.path.expanduser('~/.powa-collector.conf'),
    './powa-collector.conf'
]


def get_full_config(conn):
    """
    Return the full configuration, consisting of the information from the local
    configuration file and the remote servers stored on the repository
    database.
    """
    return add_servers_config(conn, parse_options())


def add_servers_config(conn, config):
    """
    Add the activated remote servers stored on the repository database to a
    given configuration JSON.
    """
    if ("servers" not in config):
        config["servers"] = {}

    cur = conn.cursor()
    cur.execute("""
                SELECT id, hostname, port, username, password, dbname,
                    frequency
                FROM public.powa_servers s
                WHERE s.id > 0
                AND s.frequency > 0
                ORDER BY id
            """)

    for row in cur:
        parms = {}
        parms["host"] = row[1]
        parms["port"] = row[2]
        parms["user"] = row[3]
        if (row[4] is not None):
            parms["password"] = row[4]
        parms["dbname"] = row[5]

        key = row[1] + ':' + str(row[2])
        config["servers"][key] = {}
        config["servers"][key]["dsn"] = parms
        config["servers"][key]["frequency"] = row[6]
        config["servers"][key]["srvid"] = row[0]

    conn.commit()

    return config


def parse_options():
    """
    Look for the configuration file in all supported location, parse it and
    return the resulting JSON, also adding the implicit values if needed.
    """
    options = None

    for possible_config in CONF_LOCATIONS:
        options = parse_file(possible_config)
        if (options is not None):
            break

    if (options is None):
        print("Could not find the configuration file in any of the expected"
              + " locations:")
        for possible_config in CONF_LOCATIONS:
            print("\t- %s" % possible_config)

        sys.exit(1)

    if ('repository' not in options or 'dsn' not in options["repository"]):
        print("The configuration file is invalid, it should contains"
              + " a repository.dsn entry")
        print("Place and adapt the following content in one of those "
              "locations:""")
        print("\n\t".join([""] + CONF_LOCATIONS))
        print(SAMPLE_CONFIG_FILE)
        sys.exit(1)

    if ('debug' not in options):
        options["debug"] = False

    return options


def parse_file(filepath):
    """
    Read a configuration file and return the JSON
    """
    try:
        return json.load(open(filepath))
    except IOError:
        return None
    except Error as e:
        print("Error parsing config file %s:" % filepath)
        print("\t%s" % e)
        sys.exit(1)
