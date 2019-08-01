import datetime
import numbers
import logging
import sys
import influxdb
from functools32 import lru_cache
from pyslet.iso8601 import TimePoint
import pyslet.rfc2396 as uri
from pyslet.odata2.core import EntityCollection, CommonExpression, PropertyExpression, BinaryExpression, \
    LiteralExpression, Operator, SystemQueryOption, format_expand, format_select, ODataURI
from pyslet.py2 import to_text

from local import request

logger = logging.getLogger("odata-influxdb")

operator_symbols = {
    Operator.lt: ' < ',
    Operator.le: ' <= ',
    Operator.gt: ' > ',
    Operator.ge: ' >= ',
    Operator.eq: ' = ',
    Operator.ne: ' != ',
    getattr(Operator, 'and'): ' AND '  # Operator.and doesn't resolve in Python
}

@lru_cache()
def get_tags_and_field_keys(client, measurement_name, db_name):
    client.switch_database(db_name)
    field_keys = tuple(f['fieldKey'] for f in client.query('SHOW FIELD KEYS')[measurement_name])
    tag_keys = tuple(t['tagKey'] for t in client.query('SHOW TAG KEYS')[measurement_name])
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
        self.client = influxdb.InfluxDBClient.from_dsn(self.dsn)
        self._topmax = topmax
        for es in self.container.EntitySet:
            self.bind_entity_set(es)

    def bind_entity_set(self, entity_set):
        entity_set.bind(self.get_collection_class(), container=self)

    def get_collection_class(self):
        return InfluxDBMeasurement


def unmangle_db_name(db_name):
    """corresponds to mangle_db_name in influxdbmeta.py"""
    if db_name == u'internal':
        db_name = u'_internal'
    db_name = db_name.replace('_dsh_', '-')
    return db_name


# noinspection SqlDialectInspection
def unmangle_measurement_name(measurement_name):
    """corresponds to mangle_measurement_name in influxdbmeta.py"""
    measurement_name = measurement_name.replace('_sp_', ' ')
    measurement_name = measurement_name.replace('_dsh_', '-')
    return measurement_name


def unmangle_entity_set_name(name):
    db_name, m_name = name.split('__', 1)
    db_name = unmangle_db_name(db_name)
    m_name = unmangle_measurement_name(m_name)
    return db_name, m_name


def parse_influxdb_time(t_str):
    """
    returns a `datetime` object (some precision from influxdb may be lost)
    :type t_str: str
    :param t_str: a string representing the time from influxdb (ex. '2017-01-01T23:01:41.123456789Z')
    """
    try:
        return datetime.datetime.strptime(t_str[:26].rstrip('Z'), '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        return datetime.datetime.strptime(t_str[:19], '%Y-%m-%dT%H:%M:%S')


class InfluxDBMeasurement(EntityCollection):
    """represents a measurement query, containing points

    name should be "database.measurement"
    """
    def __init__(self, container, **kwargs):
        super(InfluxDBMeasurement, self).__init__(**kwargs)
        self.container = container
        self.db_name, self.measurement_name = unmangle_entity_set_name(self.entity_set.name)
        self.topmax = getattr(self.container, '_topmax', 50)
        self.default_user = self.container.client._username
        self.default_pass = self.container.client._password

    #@lru_cache()
    def _query_len(self):
        """influxdb only counts non-null values, so we return the count of the field with maximum non-null values"""
        q = u'SELECT COUNT(*) FROM "{}" {} {}'.format(
            self.measurement_name,
            self._where_expression(),
            self._groupby_expression()
        ).strip()
        self.container.client.switch_database(self.db_name)
        logger.info('Querying InfluxDB: {}'.format(q))
        rs = self.container.client.query(q)
        interval_list = list(rs.get_points())
        if request and request.args.get('aggregate'):
            max_count = len(interval_list)
        else:
            max_count = max(val for val in rs.get_points().next().values() if isinstance(val, numbers.Number))
        self._influxdb_len = max_count
        return max_count

    def __len__(self):
        return self._query_len()

    def set_expand(self, expand, select=None):
        """Sets the expand and select query options for this collection.

        The expand query option causes the named navigation properties
        to be expanded and the associated entities to be loaded in to
        the entity instances before they are returned by this collection.

        *expand* is a dictionary of expand rules.  Expansions can be chained,
        represented by the dictionary entry also being a dictionary::

                # expand the Customer navigation property...
                { 'Customer': None }
                # expand the Customer and Invoice navigation properties
                { 'Customer':None, 'Invoice':None }
                # expand the Customer property and then the Orders property within Customer
                { 'Customer': {'Orders':None} }

        The select query option restricts the properties that are set in
        returned entities.  The *select* option is a similar dictionary
        structure, the main difference being that it can contain the
        single key '*' indicating that all *data* properties are
        selected."""
        self.entity_set.entityType.ValidateExpansion(expand, select)
        self.expand = expand
        # in influxdb, you must always query at LEAST the time field
        if select is not None and 'timestamp' not in select:
            select['timestamp'] = None
        self.select = select
        self.lastEntity = None

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

    def non_aggregate_field_name(self, f):
        agg = request.args.get('aggregate').lower()
        parts = f.split('_', 1)
        if parts[0] == agg:
            return parts[1]
        else:
            raise KeyError('invalid field received from influxdb: {}'.format(f))

    def _generate_entities(self):
        # SELECT_clause [INTO_clause] FROM_clause [WHERE_clause]
        # [GROUP_BY_clause] [ORDER_BY_clause] LIMIT_clause OFFSET <N> [SLIMIT_clause]
        if request:
            auth = getattr(request, 'authorization', None)
        else:
            auth = None
        if auth is not None:
            self.container.client.switch_user(auth.username, auth.password)
        else:
            self.container.client.switch_user(self.default_user, self.default_pass)
        q = u'SELECT {} FROM "{}" {} {} {} {}'.format(
            self._select_expression(),
            self.measurement_name,
            self._where_expression(),
            self._groupby_expression(),
            self._orderby_expression(),
            self._limit_expression(),
        ).strip()
        logger.info('Querying InfluxDB: {}'.format(q))

        result = self.container.client.query(q, database=self.db_name)
        #fields = get_tags_and_field_keys(self.container.client, self.measurement_name, self.db_name)

        for measurement_name, tag_set in result.keys():
            for row in result[measurement_name, tag_set]:
                e = self.new_entity()
                t = parse_influxdb_time(row['time'])
                e['timestamp'].set_from_value(t)
                if self.select is None or '*' in self.select:
                    for influxdb_field_name, influxdb_field_value in row.items():
                        if influxdb_field_name == 'time':
                            continue  # time has already been set
                        try:
                            entity_property = e[influxdb_field_name]
                        except KeyError:
                            # assume aggregated field
                            entity_property = e[self.non_aggregate_field_name(influxdb_field_name)]
                        entity_property.set_from_value(influxdb_field_value)
                    if tag_set is not None:
                        for tag, value in tag_set.items():
                            e[tag].set_from_value(value)
                else:
                    for odata_field_name in self.select:
                        if odata_field_name == 'timestamp':
                            continue  # time has already been set
                        if request and request.args.get('aggregate'):
                            e[odata_field_name].set_from_value(
                                row[request.args.get('aggregate') + '_' + odata_field_name])
                        e[odata_field_name].set_from_value(row[odata_field_name])
                e.exists = True
                self.lastEntity = e
                yield e

    def _select_expression(self):
        """formats the list of fields for the SQL SELECT statement, with aggregation functions if specified
        with &aggregate=func in the querystring"""
        field_format = u'{}'
        if request:
            aggregate_func = request.args.get('aggregate', None)
            if aggregate_func is not None:
                field_format = u'{}({{0}}) as {{0}}'.format(aggregate_func)

        def select_key(spec_key):
            if spec_key == u'*':
                tmp = field_format.format(spec_key)
                if u"as *" in tmp:
                    return tmp[:tmp.find(u"as *")]  # ... inelegant
            return field_format.format(spec_key.strip())

        if self.select is None or '*' in self.select:
            return select_key(u'*')
        else:
            # join the selected fields
            # if specified, format aggregate: func(field) as field
            # influxdb always returns the time field, and doesn't like it if you ask when there's a groupby anyway
            return u','.join((select_key(k)
                              for k in self.select.keys() if k != u'timestamp'))

    def _where_expression(self):
        """generates a valid InfluxDB "WHERE" query part from the parsed filter (set with self.set_filter)"""
        if self.filter is None:
            return u''
        return u'WHERE {}'.format(self._sql_where_expression(self.filter))

    def _sql_where_expression(self, filter_expression):
        if filter_expression is None:
            return ''
        elif isinstance(filter_expression, BinaryExpression):
            expressions = (filter_expression.operands[0],
                           filter_expression.operands[1])
            symbol = operator_symbols[filter_expression.operator]
            return symbol.join(self._sql_expression(o) for o in expressions)
        else:
            raise NotImplementedError

    def _groupby_expression(self):
        group_by = []
        if request:
            group_by_raw = request.args.get(u'influxgroupby', None)
            if group_by_raw is not None and self.filter is not None:
                group_by_raw = group_by_raw.strip().split(',')
                for g in group_by_raw:
                    if g == u'*':
                        group_by.append(g)
                    else:
                        group_by.append(u'"{}"'.format(g))
            group_by_time_raw = request.args.get('groupByTime', None)
            if group_by_time_raw is not None:
                group_by.append('time({})'.format(group_by_time_raw))
        if len(group_by) == 0:
            return ''
        else:
            return 'GROUP BY {}'.format(','.join(group_by))

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
            if expression.name == 'timestamp':
                return 'time'
            return expression.name
        elif isinstance(expression, LiteralExpression):
            return self._format_literal(expression.value.value)
        elif isinstance(expression, BinaryExpression):
            return self._sql_where_expression(expression)

    def _format_literal(self, val):
        if isinstance(val, unicode):
            return u"'{}'".format(val)
        elif isinstance(val, TimePoint):
            return u"'{0.date} {0.time}'".format(val)
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
            self.nextSkiptoken = (self.skiptoken or 0) + min(len(self), self.top)
            for e in self.itervalues():
                yield e
            self.paging = False

    def get_next_page_location(self):
        """Returns the location of this page of the collection

        The result is a :py:class:`rfc2396.URI` instance."""
        token = self.next_skiptoken()
        if token is not None:
            baseURL = self.get_location()
            sysQueryOptions = {}
            if self.filter is not None:
                sysQueryOptions[
                    SystemQueryOption.filter] = unicode(self.filter)
            if self.expand is not None:
                sysQueryOptions[
                    SystemQueryOption.expand] = format_expand(self.expand)
            if self.select is not None:
                sysQueryOptions[
                    SystemQueryOption.select] = format_select(self.select)
            if self.orderby is not None:
                sysQueryOptions[
                    SystemQueryOption.orderby] = CommonExpression.OrderByToString(
                    self.orderby)
            sysQueryOptions[SystemQueryOption.skiptoken] = unicode(token)
            extraOptions = ''
            if request:
                extraOptions = u'&' + u'&'.join([
                                        u'{0}={1}'.format(k, v) for k, v in request.args.items() if k[0] != u'$'])
            return uri.URI.from_octets(
                str(baseURL) +
                "?" +
                ODataURI.format_sys_query_options(sysQueryOptions) +
                extraOptions
            )
        else:
            return None
