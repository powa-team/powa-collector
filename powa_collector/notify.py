"""
Helper functions for the NOTIFY-based communication processing.
"""
def notify_parse_force_snapshot(notif):
    """Parse the payload of a received FORCE_SNAPSHOT notification"""
    if (len(notif) != 1):
        raise Exception('Command "%r" malformed' % notif)

    try:
        r_srvid = int(notif[0])
        notif.pop(0)
    except:
        raise Exception(('invalid remote server id %s' % notif[0]))

    return r_srvid


def notify_parse_refresh_db_cat(notif):
    """Parse the payload of a received REFRESH_DB_CAT notification"""
    if (len(notif) < 2):
        raise Exception('Command "%r" malformed' % notif)

    try:
        r_srvid = int(notif[0])
        notif.pop(0)
    except:
        raise Exception(('invalid remote server id %s' % notif[0]))

    try:
        r_nb_db = int(notif[0])
        notif.pop(0)
    except:
        raise Exception(('invalid number of databases %s' % notif[0]))

    r_dbnames = notif

    if (r_nb_db != len(r_dbnames)):
        raise Exception('Caller asked for %d db, but found %d: %r' %
                        (r_nb_db, len(r_dbnames), r_dbnames))

    return (r_srvid, r_dbnames)
