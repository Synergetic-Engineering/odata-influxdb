import datetime
import numbers

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
    def __init__(self, container, dsn, **kwargs):
        self.container = container
        self.dsn = dsn
        self.client = influxdb.InfluxDBClient.from_DSN(self.dsn)
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
        self.topmax = 100

    def __len__(self):
        """influxdb only counts non-null values, so we return the count of the field with maximum non-null values"""
        q = u'SELECT COUNT(*) FROM "{}"'.format(self.measurement_name)
        self.container.client.switch_database(self.db_name)
        rs = self.container.client.query(q)
        max_count = max(val for val in rs.get_points().next().values() if isinstance(val, numbers.Number))
        self._influxdb_len = max_count
        return max_count

    def itervalues(self):
        return self.order_entities(
            self.expand_entities(self._generate_entities()))

    def _generate_entities(self):
        q = u'SELECT * FROM "{}" {}'.format(self.measurement_name, self._where_expression())
        result = self.container.client.query(q, database=self.db_name)
        fields = get_tags_and_field_keys(self.container.client, self.measurement_name, self.db_name)
        for m in result[self.measurement_name]:
            t = datetime.datetime.strptime(m['time'], '%Y-%m-%dT%H:%M:%SZ')
            e = self.new_entity()
            e['time'].set_from_value(t)
            for f in fields:
                e[f].set_from_value(m[f])
            e.exists = True
            yield e

    def _where_expression(self):
        """generates a valid InfluxDB "WHERE" query from the parsed filter (set with self.set_filter)"""
        if self.filter is None:
            return ''
        elif isinstance(self.filter, BinaryExpression):
            expressions = (self.filter.operands[0],
                           self.filter.operands[1])
            symbol = operator_symbols[self.filter.operator]
            return 'WHERE {}'.format(symbol.join(self._sql_expression(o) for o in expressions))
        else:
            raise NotImplementedError

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
        self.top = top or self.topmax or 10 # a None value for top causes the default iterpage method to set a skiptoken
        self.skip = skip
        self.skiptoken = skiptoken
        self.nextSkiptoken = None

