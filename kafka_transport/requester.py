from kafka_transport import Listener


class Requester(object):
    def __init__(self, to_topic, from_topic):
        self.to = to_topic
        self._from = from_topic

        self.listener = Listener(from_topic, to_topic)

    def push(self, data):
        return self.listener.fetch(data)

    def __getattr__(self, name):
        def method(data=None):
            return self.listener.fetch(
                {'action': name, 'data': data}
            )

        return method
