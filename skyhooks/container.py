"""Container object for registering hook callbacks, and maintaining hook
pointers in a persistence layer (e.g. MongoDB) with TTLs.
"""

import logging
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
    def backend(self):
        """Property with lazyloaded Backend instance
        """

        if not hasattr(self, '_backend'):
            backend_path = '.backends.%s' % (self.config.get('backend',
                                                             'mongodb'))
            backend_module = __import__(name=backend_path, globals=globals(),
                   locals=locals(), fromlist="*")
            self._backend = backend_module.Backend(self.config, self.ioloop)

        return self._backend

    def _query_callback(self, doc, error, action, call_next=None):

        if error:
            logging.error('Webhook %s error: %s', action, error)
        if call_next:
            call_next()

    def register(self, keys, callback, url, call_next=None):

        if type(keys) in ('list', 'tuple'):
            keys = zip(keys)

        for key, value in keys.iteritems():
            if key not in self.callbacks:
                self.callbacks[key] = {}

            if value not in self.callbacks[key]:
                self.callbacks[key][value] = []

            self.callbacks[key][value].append(callback)

        callback_wrapper = lambda doc, error: self._query_callback(
                                                doc, error, 'registration',
                                                call_next)

        logging.info('Registering webhook for %s %s', keys, url)
        self.backend.update_hooks(key, url, callback_wrapper)

    def unregister(self, keys, callback, url, call_next=None):

        if type(keys) in ('list', 'tuple'):
            keys = zip(keys)

        for key, value in keys.iteritems():
            if key in self.callbacks and value in self.callbacks[key]:
                self.callbacks[key][value].remove(callback)

            callback_wrapper = lambda doc, error: self._query_callback(
                                                    doc, error, 'removal',
                                                    call_next)

            logging.info('Removing webhook for %s %s', keys, url)
            self.backend.remove_hooks(key, url, callback_wrapper)

    def notify(self, keys, data):

        if type(keys) in ('list', 'tuple'):
            keys = zip(keys)

        for key, value in keys.iteritems():
            if key in self.callbacks and value in self.callbacks[key]:
                for callback in self.callbacks[key][value]:
                    self.ioloop.add_callback(lambda cb=callback: cb(data))

    def renew(self, keys, url):

        if type(keys) in ('list', 'tuple'):
            keys = zip(keys)

        for key, value in keys.iteritems():
            if key in self.callbacks and value in self.callbacks[key]:
                self.backend.update_hooks(keys, url)
