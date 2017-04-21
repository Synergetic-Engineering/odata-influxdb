import unittest
from server import load_metadata, make_server, get_config, configure_app, configure_server
from pyslet.odata2.client import Client, ClientCollection, ClientException
from pyslet.odata2.server import ReadOnlyServer
from pyslet.odata2 import csdl as edm
from pyslet.odata2 import core
import threading


from mock import Mock, MagicMock


class TestInfluxOData(unittest.TestCase):
    def setUp(self):
        self._config = get_config('testing.conf')
        self._doc = load_metadata(self._config)
        self._container = self._doc.root.DataServices['InfluxDBSchema.InfluxDB']

    def test_where_clause(self):
        first_feed = self._container.itervalues().next()
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



if __name__ == '__main__':
    unittest.main()
