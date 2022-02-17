def get_snapshot_functions():
    """Get the list of enabled functions for snapshotting"""
    # XXX should we ignore entries without query_src?
    return """SELECT module, query_source, query_cleanup, function_name
              FROM public.powa_functions
              WHERE operation = 'snapshot' AND enabled
              AND srvid = %s
              ORDER BY priority"""


def get_src_query(src_fct, srvid):
    """Get the SQL query we'll use to get results from a snapshot function"""
    return ("SELECT %(srvid)d, * FROM public.%(fname)s(0)" %
            {'fname': src_fct, 'srvid': srvid})


def get_tmp_name(src_fct):
    """Get the temp table name we'll use to spool changes"""
    return "%s_tmp" % src_fct
