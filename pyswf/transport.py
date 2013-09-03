class JSONArgsTransport(object):
    def encode(self, agrs, kwargs):
        return json.dumps({'args': args, 'kwargs': kwargs})

    def decode(input):
        data = json.loads(input)
        return data['args'], data['kwargs']


class JSONResultTransport(object):
    def encode_result(self, result):
        return json.dumps({'error': False, 'value': result})

    def encode_error(self, message):
        return json.dumps({'error': True, 'value': message})

    def is_error(self, input):
        return json.loads(input)['exception']

    def value(self, input):
        return json.loads('input')['value']


