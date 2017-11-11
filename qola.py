"""
Qola
"""
import sys
import types


def to_list(val):
    """
    Method for casting an object into
    a list if it isn't already a list
    """
    return val if type(val) is list else [val]


class Table():
    """
    Class for defining database tables that are
    passed to the Q object
    """
    name = None
    pk = None

    def __init__(self, alias=''):
        self.alias = alias

    def identifier(self):
        """
        This will return the name of the table as
        well as the alias if one has been set
        """
        alias = self.alias.strip().rjust(1) if self.alias else ''
        return '%s%s' % (self.name, alias)


class Q():
    """
    Class for constructing queries programmatically
    """
    def __init__(self, tbl, alias=''):
        self.table = tbl()
        self.selector = Selector()
        self.joiner = Joiner()
        self.clauser = Clauser()
        self.orderer = Orderer()
        self.limiter = Limiter()
        self.setter = Setter()

    def assemble(self, mode):
        return database().assemble(self, mode)

    def select(self, cols, alias=None):
        self.selector.add(cols, alias=alias)
        return self

    def join(self, tbl, join_on, outer=False):
        self.joiner.add(tbl, join_on, outer=outer)
        return self

    def key(self, val):
        self.where('%s = ?' % self.table.pk, val)
        return self

    def where(self, clause, params, is_or=False):
        self.clauser.add(clause, params, is_or=is_or)
        return self

    def order(self, order):
        self.orderer.add(order)
        return self

    def limit(self, limits):
        self.limiter.add(limits)
        return self

    def set(self, key, value):
        self.setter.add(key, value)
        return self

    def insert(self, vals=dict()):
        for k, v in vals.items():
            self.set(k, v)

        sql, params = self.assemble('insert')
        return self.execute(sql, params)

    def update(self, vals=dict()):
        for k, v in vals.items():
            self.set(k, v)

        sql, params = self.assemble('update')
        return self.execute(sql, params)

    def delete(self):
        sql, params = self.assemble('delete')
        return self.execute(sql, params)

    def fetch(self):
        sql, params = self.assemble('select')
        return self.execute(sql, params, 'all')

    def fetch_one(self):
        sql, params = self.assemble('select')
        return self.execute(sql, params, 'one')

    @staticmethod
    def execute(sql, params=list(), fetch_mode=None):
        return database().query(sql, params, fetch_mode=fetch_mode)


class Selector():
    def __init__(self):
        self.cols = list()

    def __call__(self):
        return self.cols if self.cols else ['*']

    def add(self, cols, alias=None):
        cols = to_list(cols)
        if alias:
            cols = ['%s.%s' % (alias, k) for k in cols]
        self.cols.extend(cols)


class Joiner():
    def __init__(self):
        self.joins = list()

    def __call__(self):
        return self.joins

    def add(self, tbl, join_on, outer=False):
        join_type = 'OUTER' if outer else 'INNER'
        self.joins.extend([join_type, 'JOIN', tbl.identifier()])


class Clauser():
    def __init__(self):
        self.clauses = list()
        self.params = list()

    def __call__(self):
        return self.clauses

    def add(self, clause, params, is_or=False):
        if self.clauses:
            operator = 'OR' if is_or else 'AND'
            self.clauses.append(operator)
        self.clauses.append(clause)
        self.params.extend(to_list(params))


class Orderer():
    def __init__(self):
        self.orders = list()

    def __call__(self):
        return self.orders

    def add(self, order):
        self.orders.extend(to_list(order))


class Limiter():
    def __init__(self):
        self.limits = list()

    def __call__(self):
        return self.limits

    def add(self, limits):
        self.limits = [str(l) for l in to_list(limits)]


class Setter():
    def __init__(self):
        self.items = dict()

    def __call__(self):
        return self.items

    def add(self, key, value):
        param = value if isinstance(value, Expr) else Expr('?', [value])
        self.items[key] = param


class Expr():
    def __init__(self, value, params):
        self.value = value
        self.params = params


class Database():
    def __init__(self, dsn):
        self._dsn = dsn
        self._con = None
        self._db_driver = None

    def _connect(self):
        if not self._con:
            self._con = self._db_driver.connect(self._dsn)
            self._con.row_factory = self._dict_factory
        return self._con.cursor()

    def _dict_factory(self, cursor, row):
        dict_row = dict()
        for idx, col in enumerate(cursor.description):
            dict_row[col[0]] = row[idx]
        return dict_row

    def _list_null(self, values):
        return [None if v == '' else v for v in values]

    def _select(self, cols):
        return ['SELECT', ','.join(cols)]

    def _from(self, parts, tbl):
        parts.extend(['FROM', tbl.identifier()])

    def _join(self, parts, joins):
        if joins:
            parts.extend(joins)

    def _where(self, parts, clauses):
        if clauses:
            parts.extend(['WHERE', ' '.join(clauses)])

    def _order(self, parts, orders):
        if orders:
            parts.extend(['ORDER BY', ','.join(orders)])

    def _limit(self, parts, limits):
        if limits:
            parts.extend(['LIMIT', ','.join(limits)])

    def _insert(self, tbl, cols):
        return ['INSERT', 'INTO', tbl.name, '(%s)' % ','.join(cols)]

    def _values(self, parts, vals):
        parts.extend(['VALUES', '(%s)' % ','.join(vals)])

    def _update(self, tbl):
        return ['UPDATE', tbl.identifier()]

    def _set(self, parts, cols):
        parts.extend(['SET', ','.join(cols)])

    def _delete(self):
        return ['DELETE']

    def _build_select(self, qry):
        parts = self._select(qry.selector())
        self._from(parts, qry.table)
        self._join(parts, qry.joiner())
        self._where(parts, qry.clauser())
        self._order(parts, qry.orderer())
        self._limit(parts, qry.limiter())
        return (' '.join(parts), qry.clauser.params)

    def _build_insert(self, qry):
        cols = list()
        vals = list()
        params = list()
        for c, q in qry.setter().items():
            cols.append(c)
            vals.append('?')
            params.extend(q.params)

        params = self._list_null(params)
        parts = self._insert(qry.table, cols)
        self._values(parts, vals)
        return (' '.join(parts), params)

    def _build_update(self, qry):
        cols = list()
        vals = list()
        for c, q in qry.setter().items():
            cols.append('%s = %s' % (c, q.value))
            vals.extend(q.params)

        parts = self._update(qry.table)
        self._set(parts, cols)
        self._join(parts, qry.joiner())
        self._where(parts, qry.clauser())

        params = self._list_null(qry.clauser.params)
        return (' '.join(parts), vals + params)

    def _build_delete(self, qry):
        parts = self._delete()
        self._from(parts, qry.table)
        self._join(parts, qry.joiner())
        self._where(parts, qry.clauser())
        return (' '.join(parts), qry.clauser.params)

    def assemble(self, qry, mode):
        builder = '_build_%s' % mode
        return getattr(self, builder)(qry)

    def close(self):
        if self._con:
            self._con.close()

    def commit(self):
        self._con.commit()

    def rollback(self):
        self._con.rollback()

    def query(self, query, params=list(), fetch_mode=None):
        cursor = self._connect()
        result = list()

        try:
            cursor.execute(query, params)
            if fetch_mode == 'all':
                result = cursor.fetchall()
            if fetch_mode == 'one':
                result = cursor.fetchone()
            self.commit()
        except Exception, err:
            sys.stderr.write('ERROR: %s\n' % str(err))
            self.rollback()

        cursor.close()
        return result


class SQLite(Database):
    def __init__(self, dsn):
        Database.__init__(self, dsn)

        try:
            import sqlite3 as db_driver
            self._db_driver = db_driver
        except ImportError:
            self._db_driver = None


_database = None


def database():
    if isinstance(_database, types.FunctionType):
        return _database()
    return _database


def set_database(db):
    module = sys.modules[__name__]
    setattr(module, '_database', db)
