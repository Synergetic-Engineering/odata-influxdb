import random
import re
import unittest
import os
try:
    from responses import RequestsMock
except ImportError as e:
    print('unit tests require responses library: try `pip install responses`')
    raise e
from server import generate_metadata, get_sample_config, load_metadata
from influxdbmeta import db_name__measurement_name, mangle_db_name, mangle_measurement_name
from influxdbds import unmangle_measurement_name, unmangle_db_name, unmangle_entity_set_name
from pyslet.odata2 import core

NUM_TEST_POINTS = 100

json_database_list = {
    "results": [{
        "statement_id": 0,
        "series": [{
            "name": "databases",
            "columns": ["name"],
            "values": [
                ["_internal"],
                ["database1"]]}]}]}

json_measurement_list = {
    "results": [{
        "statement_id": 0,
        "series": [{
            "name": "measurements",
            "columns": ["name"],
            "values": [
                ["measurement1"],
                ["measurement with spaces"]]}]}]}

json_tag_keys = {
    "results": [{
        "statement_id": 0,
        "series": [{
            "name": "measurement1",
            "columns": ["tagKey"],
            "values": [["tag1"],
                       ["tag2"]]}]}]}

json_field_keys = {
    "results": [{
        "statement_id": 0,
        "series": [{
            "name": "measurement1",
            "columns": ["fieldKey", "fieldType"],
            "values": [["float_field", "float"],
                       ["int_field", "integer"]]}]}]}


def json_points_list(measurement_name, page_size=None):
    num_values = page_size or NUM_TEST_POINTS
    tag1_values = ["foo", "bar"]
    tag2_values = ["one", "zero"]
    value_list = [
        ["2017-01-01T00:00:00Z",
         random.choice(tag1_values), random.choice(tag2_values),
         random.random(), random.randint(-40,40)]
        for i in range(num_values)
    ]
    return {
        "results": [{
            "statement_id": 0,
            "series": [{
                "name": measurement_name,
                "columns": ["time", 'tag1', 'tag2', 'float_field', 'int_field'],
                "values": value_list}]}]}


def json_count(measurement_name):
    return {
        "results": [{
            "statement_id": 0,
            "series": [{
                "name": measurement_name,
                "columns": [
                    "time",
                    "float_field",
                    "int_field"],
                "values": [[
                    "1970-01-01T00:00:00Z", NUM_TEST_POINTS, NUM_TEST_POINTS]]}]}]}


class TestInfluxOData(unittest.TestCase):
    def setUp(self):
        self._config = get_sample_config()
        self._config.set('influxdb', 'dsn', 'influxdb://localhost:8086')
        self._config.set('metadata', 'autogenerate', 'no')
        self._config.set('metadata', 'metadata_file', os.path.join('test_data', 'test_metadata.xml'))
        self._doc = load_metadata(self._config)
        self._container = self._doc.root.DataServices['InfluxDBSchema.InfluxDB']

    def test_generate_metadata(self):
        with RequestsMock() as rsp:
            rsp.add(rsp.GET, re.compile('.*SHOW\+DATABASES.*'),
                    json=json_database_list, match_querystring=True)
            rsp.add(rsp.GET, re.compile('.*SHOW\+MEASUREMENTS.*'),
                    json=json_measurement_list, match_querystring=True)
            rsp.add(rsp.GET, re.compile('.*SHOW\+MEASUREMENTS.*'),
                    json=json_measurement_list, match_querystring=True)
            rsp.add(rsp.GET, re.compile('.*SHOW\+FIELD\+KEYS.*'),
                    json=json_field_keys, match_querystring=True)
            rsp.add(rsp.GET, re.compile('.*SHOW\+TAG\+KEYS.*'),
                    json=json_tag_keys, match_querystring=True)
            rsp.add(rsp.GET, re.compile('.*SHOW\+FIELD\+KEYS.*'),
                    json=json_field_keys, match_querystring=True)
            rsp.add(rsp.GET, re.compile('.*SHOW\+TAG\+KEYS.*'),
                    json=json_tag_keys, match_querystring=True)
            rsp.add(rsp.GET, re.compile('.*SHOW\+FIELD\+KEYS.*'),
                    json=json_field_keys, match_querystring=True)
            rsp.add(rsp.GET, re.compile('.*SHOW\+TAG\+KEYS.*'),
                    json=json_tag_keys, match_querystring=True)
            rsp.add(rsp.GET, re.compile('.*SHOW\+FIELD\+KEYS.*'),
                    json=json_field_keys, match_querystring=True)
            rsp.add(rsp.GET, re.compile('.*SHOW\+TAG\+KEYS.*'),
                    json=json_tag_keys, match_querystring=True)

            metadata = generate_metadata('influxdb://localhost:8086')
        file1 = open(os.path.join('test_data', 'test_metadata.xml'), 'r').read()
        open(os.path.join('test_data', 'tmp_metadata.xml'), 'wb').write(metadata)
        self.assert_(metadata == file1)

    def test_where_clause(self):
        first_feed = next(self._container.itervalues())
        collection = first_feed.OpenCollection()

        def where_clause_from_string(filter_str):
            e = core.CommonExpression.from_str(filter_str)
            collection.set_filter(e)
            where = collection._where_expression()
            return where

        where = where_clause_from_string(u"prop eq 'test'")
        self.assertEqual(where, u"WHERE prop = 'test'", msg="Correct where clause for eq operator")
        where = where_clause_from_string(u"prop gt 0")
        self.assertEqual(where, u"WHERE prop > 0", msg="Correct where clause for gt operator (Int)")
        where = where_clause_from_string(u"prop gt -32.53425D")
        self.assertEqual(where, u"WHERE prop > -32.53425", msg="Correct where clause for eq operator (Float)")
        where = where_clause_from_string(u"timestamp ge datetime'2016-01-01T00:00:00' and timestamp le datetime'2016-12-31T00:00:00'")
        self.assertEqual(where, u"WHERE time >= '2016-01-01 00:00:00' AND time <= '2016-12-31 00:00:00'")
        collection.close()

    def test_groupby_expression(self):
        first_feed = next(self._container.itervalues())
        collection = first_feed.OpenCollection()
        self.assertEqual(collection._groupby_expression(), '')

    def test_limit_expression(self):
        first_feed = next(self._container.itervalues())
        collection = first_feed.OpenCollection()
        expr = collection._limit_expression()
        self.assertEqual(expr, '')
        collection.set_page(top=100)
        collection.paging = True
        expr = collection._limit_expression()
        self.assertEqual(expr, 'LIMIT 100')
        collection.set_page(top=10, skip=10)
        collection.paging = True
        expr = collection._limit_expression()
        self.assertEqual(expr, 'LIMIT 10 OFFSET 10')

    def test_len_collection(self):
        first_feed = next(self._container.itervalues())
        collection = first_feed.OpenCollection()

        with RequestsMock() as rsp:
            rsp.add(rsp.GET, re.compile('.*SELECT\+COUNT.*'),
                    json=json_count(collection.name), match_querystring=True)

            len_collection = len(collection)
        self.assertEqual(len_collection, NUM_TEST_POINTS)

    def test_iterpage(self):
        first_feed = next(self._container.itervalues())
        collection = first_feed.OpenCollection()

        page_size = 200
        collection.set_page(top=page_size, skip=0)

        with RequestsMock() as rsp:
            re_limit = re.compile('.*q=SELECT\+%2A\+FROM\+%22measurement1.*LIMIT\+200&')
            re_limit_offset = re.compile('.*q=SELECT\+%2A\+FROM\+%22measurement1.*LIMIT\+200\+OFFSET\+200&')
            rsp.add(rsp.GET, re.compile('.*SELECT\+COUNT.*'),
                    json=json_count(collection.name), match_querystring=True)
            rsp.add(rsp.GET, re_limit,
                    json=json_points_list('measurement1', page_size=page_size), match_querystring=True)
            rsp.add(rsp.GET, re.compile('.*q=SHOW\+FIELD\+KEYS.*'),
                    json=json_field_keys, match_querystring=True)
            rsp.add(rsp.GET, re.compile('.*q=SHOW\+TAG\+KEYS.*'),
                    json=json_tag_keys, match_querystring=True)
            rsp.add(rsp.GET, re_limit_offset,
                    json=json_points_list('measurement1', page_size=page_size), match_querystring=True)

            first_page = list(collection.iterpage())
            collection.set_page(top=page_size, skip=page_size)
            second_page = list(collection.iterpage())
            collection.close()

    def test_generate_entities(self):
        first_feed = next(self._container.itervalues())
        with first_feed.OpenCollection() as collection:
            with RequestsMock() as rsp:
                rsp.add(rsp.GET, re.compile('.*q=SELECT\+%2A\+FROM\+%22measurement1%22&'),
                        json=json_points_list(collection.name), match_querystring=True)
                rsp.add(rsp.GET, re.compile('.*q=SHOW\+FIELD\+KEYS.*'),
                        json=json_field_keys, match_querystring=True)
                rsp.add(rsp.GET, re.compile('.*q=SHOW\+TAG\+KEYS.*'),
                        json=json_tag_keys, match_querystring=True)

                for e in collection._generate_entities():
                    self.assertIsInstance(e, core.Entity)


class TestUtilFunctions(unittest.TestCase):
    def test_name_mangling(self):
        mangled = mangle_db_name('test')
        unmangled = unmangle_db_name(mangled)
        self.assertEqual('test', unmangled)

        mangled = mangle_db_name('_internal')
        unmangled = unmangle_db_name(mangled)
        self.assertNotEqual(mangled[0], '_')
        self.assertEqual('_internal', unmangled)

        mangled = mangle_measurement_name('test with spaces')
        unmangled = unmangle_measurement_name(mangled)
        self.assertNotIn(' ', mangled)
        self.assertEqual('test with spaces', unmangled)

        mangled = db_name__measurement_name('testdb', 'Testing 123')
        db, unmangled = unmangle_entity_set_name(mangled)
        self.assertNotIn(' ', mangled)
        self.assertEqual('Testing 123', unmangled)


if __name__ == '__main__':
    unittest.main()
