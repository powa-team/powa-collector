"""
Simple configuration file handling, as a JSON.
"""
import json


def get_full_config(conn):
    """
    Return the full configuration, consisting of the information from the local
    configuration file and the remote servers stored on the repository
    database.
    """
    return add_servers_config(conn, parse_options())


def add_servers_config(conn, config):
    """
    Add the remote servers stored on the repository database to a given
    configuration JSON.
    """
    if ("servers" not in config):
        config["servers"] = {}

    cur = conn.cursor()
    cur.execute("""
                SELECT id, hostname, port, username, password, dbname,
                    frequency
                FROM powa_servers s
                WHERE s.id > 0
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
    Parse the local configuration file and return the resulting JSON, also
    adding the implicit values if needed.
    """
    options = parse_file('./powa-collector.conf')
    if ('debug' not in options):
        options["debug"] = False

    return options


def parse_file(filepath):
    """
    Read a configuration file and return the JSON
    """
    return json.load(open(filepath))
