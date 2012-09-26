from tornado.web import RequestHandler
from tornado.escape import json_decode


class WebhookHandler(RequestHandler):
    """Handle webhook post backs from celery tasks and route to websockets
    via registered callbacks.
    """

    def post(self, account_id=None, user_id=None):
        data = json_decode(self.request.body)
        print data

        print "received webhook postback for %s %s" % (account_id, user_id)

        if not self.application.webhook_container.notify(account_id, data,
                                                         user_id=user_id):
            self.set_status(404)
            return

        print "webhook ok"
        return '{"status": "ok"}'
