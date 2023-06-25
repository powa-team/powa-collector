"""
Helper functions for the NOTIFY-based communication processing.
"""
import psycopg2

from powa_collector.utils import get_powa_version


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

def notify_allowed(pid, conn):
    """
    Check that the role used in the given backend, identified by its pid, is
    allowed to send notifications.
    """
    powa_roles = ['powa_signal_backend', 'powa_write_all_data', 'powa_admin']
    roles_to_test = []
    cur = conn.cursor()

    powa_ver = get_powa_version(conn)

    # Should not happen
    if not powa_ver:
        return False

    # pg 14+ introduced predefined roles
    if conn.server_version >= 140000:
        roles_to_test.append('pg_signal_backend')

    try:
        # powa 5+ introduced various powa_* predefined roles so try them, but
        # also possibly pg14+ predefined roles
        if (int(powa_ver[0][0]) >= 5):
            cur.execute("""WITH s(v) AS (
                    SELECT unnest(%s)
                    UNION ALL
                    SELECT rolname
                    FROM {powa}.powa_roles
                    WHERE powa_role = ANY (%s)
                )
                SELECT bool_or(pg_has_role(usesysid, v, 'USAGE'))
                    FROM pg_stat_activity a
                    CROSS JOIN s
                    WHERE pid = %s""", (roles_to_test, powa_roles, pid))
        # if powa 4- but we have a list of predefined roles, check those
        elif (len(roles_to_test) > 0):
            cur.execute("""WITH s(v) AS (SELECT unnest(%s))
                SELECT bool_or(pg_has_role(usesysid, v, 'USAGE'))
                    FROM pg_stat_activity a
                    CROSS JOIN s
                    WHERE pid = %s""", (roles_to_test, pid))
        # if not (both old powa and postgres version), fallback to testing
        # whether the given role has enough privileges to INSERT data in
        # powa_statements to decide whether they should be allowed to use the
        # nofication system.
        else:
            cur.execute("""SELECT
                    has_table_privilege(usesysid, '{powa}.powa_statements',
                                        'INSERT')
                FROM pg_stat_activity
                WHERE pid = %s""", (pid, ))
    except psycopg2.Error as e:
        conn.rollback()
        return False

    row = cur.fetchone()

    # Connection already closed, deny
    if (not row):
        return False

    return (row[0] is True)
