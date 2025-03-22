"""
General functions shared between the main thread and the remote server threads.
"""
def get_powa_version(conn):
    """Get powa's extension version"""
    cur = conn.cursor()
    cur.execute("""SELECT
        regexp_split_to_array(extversion, E'\\\\.'),
        extversion
        FROM pg_catalog.pg_extension
        WHERE extname = 'powa'""")
    res = cur.fetchone()
    cur.close()

    return res


def conf_are_equal(conf1, conf2):
    """Compare two configurations, returns True if equal"""
    for k in conf1.keys():
        if (k not in conf2):
            return False
        if (conf1[k] != conf2[k]):
            return False

    for k in conf2.keys():
        if (k not in conf1):
            return False
        if (conf1[k] != conf2[k]):
            return False

    return True
