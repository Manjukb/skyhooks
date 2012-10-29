"""Abstracted MongoDB connection and query utils
"""

from datetime import datetime
from skyhooks import IOLoop


class Backend(object):

    def __init__(self, config, ioloop=None):
        if config['system_type'] == 'twisted':
            raise NotImplemented('Twisted Matrix support is planned for the'
                                 ' future.')
        self.config = config

        # Sane defaults
        if 'mongodb' not in self.config:
            self.config['mongodb'] = {}
        if 'host' not in self.config['mongodb']:
            self.config['mongodb']['host'] = 'localhost'
        if 'dbname' not in self.config['mongodb']:
            self.config['mongodb']['dbname'] = 'skyhooks'
        if 'mongo_collection' not in self.config:
            self.config['mongodb_collection'] = 'skyhooks_webhooks'

        if ioloop is None:
            self.ioloop = IOLoop(config['system_type'])
        else:
            self.ioloop = ioloop

        if self.config['system_type'] == 'tornado':
            import asyncmongo
            self.db = asyncmongo.Client(pool_id='skyhooks',
                    **self.config['mongodb'])

        elif self.config['system_type'] == 'gevent':
            import pymongo
            db_name = self.config['mongodb'].pop('dbname')
            self.db = pymongo.Connection(pool_id='skyhooks',
                    use_greenlets=True,
                    **self.config['mongodb'])[db_name]

        self.collection = self.db[self.config['mongodb_collection']]

    def get_hooks(self, keys, url=None, callback=None):

        if callback is None:
            callback = lambda doc, error: None

        query = self._build_query(keys)

        if self.config['system_type'] == 'twisted':
            pass

        elif self.config['system_type'] == 'tornado':
            self.collection.find(query, callback=callback)

        elif self.config['system_type'] == 'gevent':
            def find():
                resp = None
                error = None
                try:
                    resp = self.collection.find(query)
                except Exception as e:
                    error = e

                callback(resp, error)

            self.ioloop.add_callback(find)

    def update_hooks(self, keys, url=None, callback=None, create=True):

        if callback is None:
            callback = lambda doc, error: None

        doc = {
            'updated': datetime.utcnow()
        }
        if url is not None:
            doc['url'] = url

        query = self._build_query(keys)

        if self.config['system_type'] == 'twisted':
            pass

        elif self.config['system_type'] == 'tornado':
            self.collection.update(query, doc, callback=callback,
                                   upsert=create, safe=True)

        elif self.config['system_type'] == 'gevent':
            def update():
                resp = None
                error = None
                try:
                    resp = self.collection.update(query, doc,
                                                  upsert=create,
                                                  safe=True)
                    if resp['err'] is not None:
                        error = resp['err']
                except Exception as e:
                    error = e

                callback(resp, error)

            self.ioloop.add_callback(update)

    def remove_hooks(self, keys, url, callback=None):

        if callback is None:
            callback = lambda doc, error: None

        query = self._build_query(keys, url)

        if self.config['system_type'] == 'twisted':
            pass

        elif self.config['system_type'] == 'tornado':
            self.collection.remove(query, callback=callback)

        elif self.config['system_type'] == 'gevent':
            def delete():
                resp = None
                error = None
                try:
                    resp = self.collection.remove(query)
                    if resp['err'] is not None:
                        error = resp['err']
                except Exception as e:
                    error = e

                callback(resp, error)

            self.ioloop.add_callback(delete)

    def _build_query(self, keys, url=None):

        query = {
            '$or': []
        }

        for name, values in keys.iteritems():
            subquery = {}
            if type(values) not in ('list', 'tuple'):
                values = [values]
            subquery[name] = {
                '$in': values
            }
            query['$or'].append(subquery)

        if url is not None:
            query['url'] = url

        return query
