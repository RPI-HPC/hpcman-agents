"""Database abstraction and helpers."""

from itertools import chain, count, izip

# Interface to psycopg2.
import psycopg2
DB_connect = psycopg2.connect
DBTimestamp = psycopg2.Timestamp
DBDatabaseError = psycopg2.DatabaseError
DBOperationalError = psycopg2.OperationalError

__all__ = [
    "DB_connect", "DB_cleanup", "DBTimestamp",
    "DBDatabaseError", "DBOperationalError",
    "DB_get_next_row"
    ]

def DB_cleanup(conn, cur=None):
    if cur is not None:
        try:
            cur.close()
        except:
            pass
    if conn is not None:
        try:
            conn.close()
        except:
            pass


class RowDict(dict):
    """Dictionary built from a row and its description."""

    def __init__(self, desc, cols):
        """Build a dictionary for a row."""

        dict.__init__(self,
                      chain(izip(count(), cols),
                            zip([d[0] for d in desc], cols)))

    def __getitem__(self, cn):
        if isinstance(cn, int):
            return dict.__getitem__(self, cn)
        return dict.__getitem__(self, cn.lower())


def DB_get_next_row(cur):
    """Get a row from a cursor.  Returns a dict, or None."""

    r = cur.fetchone()
    if r is None:
        return r
    return RowDict(cur.description, r)
