import unittest
from server import load_metadata, make_server, get_config, configure_app, configure_server
from pyslet.odata2.client import Client
from pyslet.odata2 import core
import threading


class TestInfluxOData(unittest.TestCase):
    def setUp(self):
        self._config = get_config('testing.conf')
        self._doc = load_metadata(self._config)
        self._container = self._doc.root.DataServices['InfluxDBSchema.InfluxDB']

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
        collection.close()

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

    def test_iterpage(self):
        first_feed = next(self._container.itervalues())
        collection = first_feed.OpenCollection()
        if len(collection) < 20:
            collection.close()
            self.skipTest(reason="DB does not have enough entries to test pagination.")
        collection.set_page(top=10, skip=0)
        first_page = list(collection.iterpage())
        self.assertGreater(len(first_page), 0)
        self.assertLessEqual(len(first_page), 10)
        second_page = list(collection.iterpage())
        self.assertGreater(len(second_page), 0)
        self.assertLessEqual(len(second_page), 10)
        collection.set_page(top=10, skip=len(collection)-5)
        last_page = list(collection.iterpage())
        print (last_page)
        self.assertEqual(len(last_page), 5)  # note: may fail if data is added during testing
        collection.close()

    def test_itervalues(self):
        first_feed = next(self._container.itervalues())
        with first_feed.OpenCollection() as collection:
            for e in collection.itervalues():
                self.assertIsInstance(e, core.Entity)

    def test_generate_entities(self):
        first_feed = next(self._container.itervalues())
        with first_feed.OpenCollection() as collection:
            for e in collection._generate_entities():
                self.assertIsInstance(e, core.Entity)


class TestClient(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls._config = get_config('testing.conf')
        doc = load_metadata(cls._config)
        cls._app = configure_app(cls._config, doc)
        cls._server = configure_server(cls._config, cls._app)
        t = threading.Thread(target=cls._server.serve_forever)
        t.start()
        threading._sleep(5)

    @classmethod
    def tearDownClass(cls):
        cls._server.shutdown()
        cls._server.socket.close()

    def testClient(self):
        c = Client(self._config.get('server', 'server_root'))
        self.assertGreater(c.feeds, 0, msg="Found at least one odata feed")

    def testNextLink(self):
        c = Client(self._config.get('server', 'server_root'))
        test_feed = c.feeds['internal__write']
        coll = test_feed.OpenCollection()
        l = len(coll)
        coll.set_page(top=10, skiptoken=l-5)
        last_page = list(coll.iterpage())
        self.assertEqual(len(last_page), 5)
        coll.close()


if __name__ == '__main__':
    unittest.main()
