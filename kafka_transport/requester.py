from .__main__ import fetch

class Requester(object):
    def __init__(self, to_topic, from_topic):
        self.to = to_topic
        self._from = from_topic

    def push(self, data):
        return fetch(self.to, self._from, data)

    def __getattr__(self, name):
        def method(data = None):
            return fetch(
                self.to,
                self._from,
                {'action': name, 'data': data}
            )

        return method
