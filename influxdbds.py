import datetime
import numbers
import logging
import influxdb
from functools32 import lru_cache
from pyslet.odata2.core import EntityCollection, CommonExpression, PropertyExpression, BinaryExpression, \
    LiteralExpression, Operator

operator_symbols = {
    Operator.lt: ' < ',
    Operator.gt: ' > ',
    Operator.eq: ' = ',
    Operator.ne: ' != '
}

@lru_cache()
def get_tags_and_field_keys(client, measurement_name, db_name):
    client.switch_database(db_name)
    field_keys = tuple(f['fieldKey'] for f in client.query('show field keys')[measurement_name])
    tag_keys = tuple(t['tagKey'] for t in client.query('show tag keys')[measurement_name])
    return tuple(field_keys + tag_keys)


class InfluxDBEntityContainer(object):
    """Object used to represent an Entity Container (influxdb database)

    modelled after the SQLEntityContainer in pyslet (sqlds.py)

    container
        pyslet.odata2.csdl.EntityContainer

    dsn
        data source name in the format: influxdb://user:pass@host:port/
        supported schemes include https+influxdb:// and udp+influxdb://
    """
    def __init__(self, container, dsn, topmax, **kwargs):
        self.container = container
        self.dsn = dsn
        self.client = influxdb.InfluxDBClient.from_DSN(self.dsn)
        self._topmax = topmax
        for es in self.container.EntitySet:
            self.bind_entity_set(es)

    def bind_entity_set(self, entity_set):
        entity_set.bind(self.get_collection_class(), container=self)

    def get_collection_class(self):
        return InfluxDBMeasurement


# noinspection SqlDialectInspection
class InfluxDBMeasurement(EntityCollection):
    """represents a measurement query, containing points

    name should be "database.measurement"
    """
    def __init__(self, container, **kwargs):
        super(InfluxDBMeasurement, self).__init__(**kwargs)
        self.container = container
        self.db_name, self.measurement_name = self.entity_set.name.split('__')
        if self.db_name == u'internal':
            self.db_name = u'_internal'
        self.topmax = getattr(self.container, '_topmax', 50)

    @lru_cache()
    def _query_len(self):
        """influxdb only counts non-null values, so we return the count of the field with maximum non-null values"""
        q = u'SELECT COUNT(*) FROM "{}"'.format(self.measurement_name)
        self.container.client.switch_database(self.db_name)
        rs = self.container.client.query(q)
        max_count = max(val for val in rs.get_points().next().values() if isinstance(val, numbers.Number))
        self._influxdb_len = max_count
        return max_count

    def __len__(self):
        return self._query_len()

    def expand_entities(self, entityIterable):
        """Utility method for data providers.

        Given an object that iterates over all entities in the
        collection, returns a generator function that returns expanded
        entities with select rules applied according to
        :py:attr:`expand` and :py:attr:`select` rules.

        Data providers should use a better method of expanded entities
        if possible as this implementation simply iterates through the
        entities and calls :py:meth:`Entity.Expand` on each one."""
        for e in entityIterable:
            if self.expand or self.select:
                e.Expand(self.expand, self.select)
            yield e

    def itervalues(self):
        return self.expand_entities(
            self._generate_entities())

    def _generate_entities(self):
        # SELECT_clause [INTO_clause] FROM_clause [WHERE_clause]
        # [GROUP_BY_clause] [ORDER_BY_clause] LIMIT_clause OFFSET <N> [SLIMIT_clause]
        q = u'SELECT * FROM "{}" {} {} {}'.format(
            self.measurement_name,
            self._where_expression(),
            self._orderby_expression(),
            self._limit_expression(),
        )
        logging.debug('Querying InfluxDB: {}'.format(q))
        result = self.container.client.query(q, database=self.db_name)
        fields = get_tags_and_field_keys(self.container.client, self.measurement_name, self.db_name)
        for m in result[self.measurement_name]:
            t = datetime.datetime.strptime(m['time'], '%Y-%m-%dT%H:%M:%SZ')
            e = self.new_entity()
            e['time'].set_from_value(t)
            for f in fields:
                e[f].set_from_value(m[f])
            e.exists = True
            self.lastEntity = e
            yield e

    def _where_expression(self):
        """generates a valid InfluxDB "WHERE" query part from the parsed filter (set with self.set_filter)"""
        if self.filter is None:
            return ''
        elif isinstance(self.filter, BinaryExpression):
            expressions = (self.filter.operands[0],
                           self.filter.operands[1])
            symbol = operator_symbols[self.filter.operator]
            return 'WHERE {}'.format(symbol.join(self._sql_expression(o) for o in expressions))
        else:
            raise NotImplementedError

    def _orderby_expression(self):
        """generates a valid InfluxDB "ORDER BY" query part from the parsed order by clause (set with self.set_orderby)"""
        return ''

    def _limit_expression(self):
        if not self.paging:
            return ''
        if not self.skip:
            return 'LIMIT {}'.format(str(self.top))
        return 'LIMIT {} OFFSET {}'.format(str(self.top), str(self.skip))

    def _sql_expression(self, expression):
        if isinstance(expression, PropertyExpression):
            return expression.name
        elif isinstance(expression, LiteralExpression):
            return self._format_literal(expression.value.value)

    def _format_literal(self, val):
        if isinstance(val, unicode):
            return "'{}'".format(val)
        else:
            return str(val)

    def __getitem__(self, key):
        raise NotImplementedError

    def set_page(self, top, skip=0, skiptoken=None):
        self.top = int(top or 0) or self.topmax  # a None value for top causes the default iterpage method to set a skiptoken
        self.skip = skip
        self.skiptoken = int(skiptoken or 0)
        self.nextSkiptoken = None

    def iterpage(self, set_next=False):
        """returns iterable subset of entities, defined by parameters to self.set_page"""
        if self.top == 0:  # invalid, return nothing
            return
        if self.skiptoken >= len(self):
            self.nextSkiptoken = None
            self.skip = None
            self.skiptoken = None
            return
        if self.skip is None:
            if self.skiptoken is not None:
                self.skip = int(self.skiptoken)
            else:
                self.skip = 0
        self.paging = True
        if set_next:
            # yield all pages
            done = False
            while self.skiptoken <= len(self):
                self.nextSkiptoken = (self.skiptoken or 0) + self.top
                for e in self.itervalues():
                    yield e
                self.skiptoken = self.nextSkiptoken
            self.paging = False
            self.top = self.skip = 0
            self.skiptoken = self.nextSkiptoken = None
        else:
            # yield one page
            self.nextSkiptoken = (self.skiptoken or 0) + self.top
            for e in self.itervalues():
                yield e
            self.paging = False





