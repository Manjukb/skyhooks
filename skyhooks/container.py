"""Container object for registering hook callbacks, and maintaining hook
pointers in a persistence layer (e.g. MongoDB) with TTLs.
"""

import logging
from skyhooks import IOLoop


class WebhookContainer(object):
    callbacks = {}

    def __init__(self, config=None, **kwargs):

        if config is None:
            config = {}
        config.update(kwargs)

        if 'system_type' not in config:
            raise AttributeError('Please set the system_type to either gevent '
                                 'or tornado')

        elif config['system_type'] == 'twisted':
            raise NotImplemented('Twisted Matrix support is planned for the'
                                 ' future.')

        self.config = config
        self.ioloop = IOLoop(config['system_type'])

        if self.config.get('auto_renew', True):
            if 'renew_seconds' not in self.config:
                self.config['renew_seconds'] = 120

            self.queue_renew_all()

    @property
    def backend(self):
        """Property with lazyloaded Backend instance
        """

        if not hasattr(self, '_backend'):
            backend_path = 'skyhooks.backends.%s' % (self.config.get('backend',
                                                             'mongodb'))
            backend_module = __import__(name=backend_path, globals=globals(),
                   locals=locals(), fromlist="*")
            self._backend = backend_module.Backend(self.config, self.ioloop)

        return self._backend

    def _query_callback(self, doc, error, action):

        if error:
            logging.error('Webhook %s error: %s', action, error)

    def register(self, keys, url, callback):

        if type(keys) in ('list', 'tuple'):
            keys = zip(keys)

        for key, value in keys.iteritems():
            if key not in self.callbacks:
                self.callbacks[key] = {}

            if value not in self.callbacks[key]:
                self.callbacks[key][value] = []

            self.callbacks[key][value].append(callback)

        callback_wrapper = lambda doc, error: self._query_callback(
                                                doc, error, 'registration')

        logging.info('Registering webhook for %s %s', keys, url)
        self.backend.update_hooks(keys, url, callback_wrapper)

    def unregister(self, keys, url, callback):

        if type(keys) in ('list', 'tuple'):
            keys = zip(keys)

        for key, value in keys.iteritems():
            if key in self.callbacks and value in self.callbacks[key]:
                self.callbacks[key][value].remove(callback)

            callback_wrapper = lambda doc, error: self._query_callback(
                                                    doc, error, 'removal')

            logging.info('Removing webhook for %s %s', keys, url)

        self.backend.remove_hooks(keys, url, callback_wrapper)

    def notify(self, keys, data):

        if type(keys) in ('list', 'tuple'):
            keys = zip(keys)

        for key, value in keys.iteritems():
            if key in self.callbacks and value in self.callbacks[key]:
                for callback in self.callbacks[key][value]:
                    self.ioloop.add_callback(lambda cb=callback: cb(data))

    def queue_renew_all(self, *args, **kwargs):

        logging.info('Queued webhook renewal cycle.')
        self.ioloop.add_timeout(self.renew_all, self.config['renew_seconds'])

    def renew_all(self):

        keys = dict((k, v.keys()) for (k, v) in self.callbacks.iteritems())
        if keys:
            logging.info('Renewing webhooks.')
            self.backend.update_hooks(keys, callback=self.queue_renew_all,
                                  create=False)
        else:
            logging.info('No webhooks to renew.')
