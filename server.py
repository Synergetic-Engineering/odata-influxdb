import argparse
import logging
import os
import string
import sys
import traceback
from pydoc import html
from urlparse import urlparse
from ConfigParser import ConfigParser
from wsgiref.simple_server import make_server

import pyslet.odata2.metadata as edmx
import pyslet.odata2.core as core
from pyslet.http import params
from pyslet.odata2.server import ReadOnlyServer, WSGIWrapper, Server

from influxdbmeta import generate_metadata
from influxdbds import InfluxDBEntityContainer

cache_app = None  #: our Server instance

logging.basicConfig()
logger = logging.getLogger("odata-influxdb")
logger.setLevel(logging.INFO)


class ReadOnlyServerWithCustomOptions(ReadOnlyServer):
    def __init__(self, *args, **kwargs):
        super(ReadOnlyServerWithCustomOptions, self).__init__(*args, **kwargs)

    def ReturnEntityCollection(
            self,
            entities,
            request,
            environ,
            start_response,
            response_headers):
        """Returns an iterable of Entities."""
        responseType = self.ContentNegotiation(
            request, environ, self.FeedTypes)
        if responseType is None:
            return self.ODataError(
                request,
                environ,
                start_response,
                "Not Acceptable",
                'xml, json or plain text formats supported',
                406)
        entities.TopMax(self.topmax)
        ### ---- CUSTOM OPTIONS CODE  ---- ###

        if callable(getattr(entities, 'SetCustomQueryOptions', None)):
            entities.SetCustomQueryOptions(request.queryOptions)
        ### ---- /CUSTOM OPTIONS CODE ---- ###
        if responseType == "application/json":
            data = str('{"d":%s}' % string.join(
                entities.generate_entity_set_in_json(request.version), ''))
        else:
            # Here's a challenge, we want to pull data through the feed
            # by yielding strings just load in to memory at the moment
            f = core.Feed(None, entities)
            doc = core.Document(root=f)
            f.collection = entities
            f.SetBase(str(self.serviceRoot))
            data = str(doc)
        response_headers.append(("Content-Type", str(responseType)))
        response_headers.append(("Content-Length", str(len(data))))
        start_response("%i %s" % (200, "Success"), response_headers)
        return [data]




class FileExistsError(IOError):
    def __init__(self, path):
        self.__path = path

    def __str__(self):
        return 'file already exists: {}'.format(self.__path)


def load_metadata(config):
    """Regenerate and load the metadata file and connects the InfluxDBEntityContainer."""
    metadata_filename = config.get('metadata', 'metadata_file')
    dsn = config.get('influxdb', 'dsn')

    if config.getboolean('metadata', 'autogenerate'):
        logger.info("Generating OData metadata xml file from InfluxDB metadata")
        metadata = generate_metadata(dsn)
        with open(metadata_filename, 'wb') as f:
            f.write(metadata)

    doc = edmx.Document()
    with open(metadata_filename, 'rb') as f:
        doc.ReadFromStream(f)
    container = doc.root.DataServices['InfluxDBSchema.InfluxDB']
    try:
        topmax = config.getint('influxdb', 'max_items_per_query')
    except:
        topmax = 50
    InfluxDBEntityContainer(container=container, dsn=dsn, topmax=topmax)
    return doc


def configure_app(c, doc):
    service_root = c.get('server', 'server_root')
    app = ReadOnlyServerWithCustomOptions(serviceRoot=service_root)
    app.SetModel(doc)
    return app


def configure_server(c, app):
    url = urlparse(c.get('server', 'server_root'))
    server = make_server(url.hostname, url.port, app)
    return server


def start_server(c, doc):
    app = configure_app(c, doc)
    server = configure_server(c, app)
    logger.info("Starting HTTP server on port %i..." % server.server_port)
    # Respond to requests until process is killed
    server.serve_forever()


def get_sample_config():
    config = ConfigParser(allow_no_value=True)
    config.add_section('server')
    config.set('server', 'server_root', 'http://localhost:8080')
    config.add_section('metadata')
    config.set('metadata', '; set autogenerate to "no" for quicker startup of the server if you know your influxdb structure has not changed')
    config.set('metadata', 'autogenerate', 'yes')
    config.set('metadata', '; metadata_file specifies the location of the metadata file to generate')
    config.set('metadata', 'metadata_file', 'test_metadata.xml')
    config.add_section('influxdb')
    config.set('influxdb', '; supported schemes include https+influxdb:// and udp+influxdb://')
    config.set('influxdb', 'dsn', 'influxdb://user:pass@localhost:8086')
    config.set('influxdb', 'max_items_per_query', '50')
    return config

def make_sample_config():
    config = get_sample_config()
    sample_name = 'sample.conf'
    if os.path.exists(sample_name):
        raise FileExistsError(sample_name)
    with open(sample_name, 'w') as cf:
        config.write(cf)
    print('generated sample conf at: {}'.format(os.path.join(os.getcwd(), sample_name)))


def get_config(config):
    with open(config, 'r') as fp:
        c = ConfigParser()
        c.readfp(fp)
    return c


def main():
    """read config and start odata api server"""
    # parse arguments
    p = argparse.ArgumentParser()
    p.add_argument('-c', '--config',
                   help='specify a conf file (default=production.conf)',
                   default='production.conf')
    p.add_argument('-m', '--makeSampleConfig',
                   help='generates sample.conf in your current directory (does not start server)',
                   action="store_true")
    args = p.parse_args()

    if args.makeSampleConfig:
        make_sample_config()
        sys.exit()

    # parse config file
    c = get_config(args.config)

    # generate and load metadata
    doc = load_metadata(c)

    # start server
    start_server(c, doc)


if __name__ == '__main__':
    main()
