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
