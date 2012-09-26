"""Webook handlers
"""

import logging
import asyncmongo
from bson.objectid import ObjectId

from tornado import ioloop
from skyhooks import settings


class WebhookContainer(object):
    account_callbacks = {}
    user_callbacks = {}

    def __init__(self):
        self.io_loop = ioloop.IOLoop.instance()

    @property
    def db(self):
        if not hasattr(self, '_db'):
            self._db = asyncmongo.Client(pool_id='skyhooks',
                    host=settings.MONGO_HOST, port=settings.MONGO_PORT,
                    dbname='skyhooks')
        return self._db

    def register(self, account_id, callback, url, user_id=None,
                 call_next=None):
        if account_id not in self.account_callbacks:
            self.account_callbacks[account_id] = []

        self.account_callbacks[account_id].append(callback)
        print self.account_callbacks

        query = {
            'accountId': ObjectId(account_id),
            'url': url
        }

        if user_id:
            if user_id not in self.user_callbacks:
                self.user_callbacks[user_id] = []

            self.user_callbacks[user_id].append(callback)

            query['userId'] = ObjectId(user_id)

        callback_wrapper = lambda doc, error: self._mongo_callback(doc, error,
                                                                   call_next)
        print "Registering webhook for: " + str(query)

        self.db.webhooks.update(query, query,
                callback=callback_wrapper,
                upsert=True)

    def unregister(self, account_id, callback, url, user_id=None,
                   call_next=None):

        if account_id in self.account_callbacks:
            self.account_callbacks[account_id].remove(callback)

            query = {
                'accountId': ObjectId(account_id),
                'url': url
            }

            if user_id in self.user_callbacks:
                self.user_callbacks[user_id].remove(callback)

                query['userId'] = ObjectId(user_id)

            callback_wrapper = lambda doc, error: self._mongo_callback(doc,
                                                           error, call_next)

            print "Unregistering webhook for: %s" % (str(query),)
            self.db.webhooks.remove(query,
                    callback=callback_wrapper)

    def _mongo_callback(self, doc, error, call_next=None):
        if error:
            logging.error(error)
        if call_next:
            call_next()

    def notify(self, account_id, data, user_id=None):
        print self.account_callbacks
        if account_id in self.account_callbacks:
            for callback in self.account_callbacks[account_id]:
                self.io_loop.add_callback(lambda cb=callback: cb(data))

            if user_id in self.user_callbacks:
                for callback in self.user_callbacks[user_id]:
                    self.io_loop.add_callback(lambda cb=callback: cb(data))

            print "notify true"
            return True

        print "notify false"
        return False
