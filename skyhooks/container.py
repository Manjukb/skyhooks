"""Container object for registering hook callbacks, and maintaining hook
pointers in a persistence layer (e.g. MongoDB) with TTLs.
"""

import logging
import asyncmongo
from bson.objectid import ObjectId

from skyhooks import IOLoop


class WebhookContainer(object):
    callbacks = {}

    def __init__(self, config):
        if config['system_type'] == 'twisted':
            raise NotImplemented('Twisted Matrix support is planned for the'
                                 ' future.')

        self.config = config
        self.ioloop = IOLoop(config['system_type'])

    @property
    def db(self):
        if not hasattr(self, '_db'):
            self._db = asyncmongo.Client(pool_id='skyhooks',
                    host=self.config['MONGO_HOST'],
                    port=self.config['MONGO_PORT'],
                    dbname='skyhooks')
        return self._db

    def register(self, key, callback, url, call_next=None):
        if key not in self.account_callbacks:
            self.account_callbacks[key] = []

        self.account_callbacks[key].append(callback)

        query = {
            'key': ObjectId(key),
            'url': url
        }

        callback_wrapper = lambda doc, error: self._mongo_callback(doc, error,
                                                                   call_next)
        logging.debug("Registering webhook for: %s", query)

        self.db.webhooks.update(query, query,
                callback=callback_wrapper,
                upsert=True)

    def unregister(self, key, callback, url, call_next=None):

        if key in self.account_callbacks:
            self.account_callbacks[key].remove(callback)

            query = {
                'key': ObjectId(key),
                'url': url
            }

            callback_wrapper = lambda doc, error: self._mongo_callback(doc,
                                                           error, call_next)

            logging.debug("Unregistering webhook for: %s", query)
            self.db.webhooks.remove(query,
                    callback=callback_wrapper)

    def _mongo_callback(self, doc, error, call_next=None):
        if error:
            logging.error(error)
        if call_next:
            call_next()

    def notify(self, key, data):
        if key in self.account_callbacks:
            for callback in self.account_callbacks[key]:
                self.ioloop.add_callback(lambda cb=callback: cb(data))

            return True

        return False
