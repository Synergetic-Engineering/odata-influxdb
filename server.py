import argparse
import logging
import os
import sys
from urlparse import urlparse
from ConfigParser import ConfigParser
from wsgiref.simple_server import make_server
from werkzeug.wrappers import AuthorizationMixin, BaseRequest, Response
from local import local, local_manager


import pyslet.odata2.metadata as edmx
from pyslet.odata2.server import ReadOnlyServer

from influxdbmeta import generate_metadata
from influxdbds import InfluxDBEntityContainer

cache_app = None  #: our Server instance

logging.basicConfig()
logger = logging.getLogger("odata-influxdb")
logger.setLevel(logging.INFO)


class Request(BaseRequest, AuthorizationMixin):
    pass


class HTTPAuthPassThrough(object):
    def __init__(self, app):
        self.wrapped = app
        self.realm = 'influxdb'

    def __call__(self, environ, start_response):
        local.request = req = Request(environ)
        if req.authorization is None:
            resp = Response('Unauthorized. Please supply authorization.',
                            status=401,
                            headers={
                                ('WWW-Authenticate', 'Basic Realm="{}"'.format(self.realm)),
                            }
            )
            return resp(environ, start_response)
        return self.wrapped(environ, start_response)


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
    service_root = c.get('server', 'service_advertise_root')
    logger.info("Advertising service at %s" % service_root)
    app = ReadOnlyServer(serviceRoot=service_root)
    app.SetModel(doc)
    return app


def start_server(c, doc):
    app = configure_app(c, doc)
    if c.getboolean('influxdb', 'authentication_required'):
        app = HTTPAuthPassThrough(app)
        app = local_manager.make_middleware(app)
    from werkzeug.serving import run_simple
    listen_interface = c.get('server', 'server_listen_interface')
    listen_port = int(c.get('server', 'server_listen_port'))
    logger.info("Starting HTTP server on: interface: %s, port: %i..." % (listen_interface, listen_port))
    run_simple(listen_interface, listen_port, application=app)


def get_sample_config():
    config = ConfigParser(allow_no_value=True)
    config.add_section('server')
    config.set('server', 'service_advertise_root', 'http://localhost:8080')
    config.set('server', 'server_listen_interface', '127.0.0.1')
    config.set('server', 'server_listen_port', 8080)
    config.add_section('metadata')
    config.set('metadata', '; set autogenerate to "no" for quicker startup of the server if you know your influxdb structure has not changed')
    config.set('metadata', 'autogenerate', 'yes')
    config.set('metadata', '; metadata_file specifies the location of the metadata file to generate')
    config.set('metadata', 'metadata_file', 'test_metadata.xml')
    config.add_section('influxdb')
    config.set('influxdb', '; supported schemes include https+influxdb:// and udp+influxdb://')
    config.set('influxdb', '; user:pass in this dsn is used for generating metadata')
    config.set('influxdb', 'dsn', 'influxdb://user:pass@localhost:8086')
    config.set('influxdb', 'max_items_per_query', '50')
    config.set('influxdb', '; authentication_required will pass through http basic auth username')
    config.set('influxdb', '; and password to influxdb')
    config.set('influxdb', 'authentication_required', 'no')
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
        c = get_sample_config()
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
