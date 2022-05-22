def get_global_snapfuncs_sql(ver):
    """Get the list of enabled global functions for snapshotting"""
    # XXX should we ignore entries without query_src?

    if (int(ver[0][0]) >= 5):
        return """SELECT name, external, query_source, query_cleanup,
                    function_name
                  FROM {powa}.powa_functions pf
                  -- FIXME
                  -- JOIN pg_extension ext ON ext.extname = pf.module
                  -- JOIN pg_namespace nsp ON nsp.oid = ext.extnamespace
                  WHERE operation = 'snapshot' AND enabled
                  AND srvid = %s
                  ORDER BY priority"""
    else:
        return """SELECT module AS name, false AS external, query_source,
                    query_cleanup, function_name
                  FROM {powa}.powa_functions pf
                  -- FIXME
                  -- JOIN pg_extension ext ON ext.extname = pf.module
                  -- JOIN pg_namespace nsp ON nsp.oid = ext.extnamespace
                  WHERE operation = 'snapshot' AND enabled
                  AND srvid = %s
                  ORDER BY priority"""


def get_src_query(schema, src_fct, srvid):
    """Get the SQL query we'll use to get results from a snapshot function"""
    return ("SELECT %(srvid)d, * FROM %(schema)s.%(fname)s(0)" %
            {'fname': src_fct, 'schema': schema, 'srvid': srvid})


def get_db_snapfuncs_sql(srvid, server_version_num):
    """Get the SQL query we'll use to get results from a snapshot function"""
    return ("""SELECT db_module, query_source, tmp_table, dbnames
            FROM {powa}.powa_db_functions(%(srvid)d, %(server_version_num)d)
            WHERE operation = 'snapshot'
            ORDER BY priority
            """ % {'srvid': srvid, 'server_version_num': server_version_num})


def get_global_tmp_name(schema, src_fct):
    """Get the temp table name we'll use to spool changes"""
    return "%s.%s_tmp" % (schema, src_fct)


def get_nsp(conn, external, module):
    if external:
        return conn._nsps[module]
    else:
        return conn._nsps['powa']
