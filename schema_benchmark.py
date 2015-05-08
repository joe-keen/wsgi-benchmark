import jsonschema
import timeit

import voluptuous

sample_data = [{u'timestamp': 1431023622780.177,
                u'name': u'metric_perf',
                u'value': 0,
                u'dimensions': {u'dim1': u'agent-0'}},
               {u'timestamp': 1431023622780,
                u'name': u'metric_perf',
                u'value': 0, u'dimensions': {u'dim1': u'agent-0'}}]

sample_data = [{u'timestamp': 1431023622780.177,
                u'name': u'metric_perf',
                u'value': 0,
                u'dimensions': {u'dim1': u'agent-0'}}] * 200


metric_name_schema = voluptuous.Schema(
    voluptuous.All(voluptuous.Any(str, unicode), voluptuous.Length(max=64)))

dimensions_schema = voluptuous.Schema(
    {voluptuous.All(
        voluptuous.Any(str, unicode), voluptuous.Length(max=255)):
        voluptuous.All(voluptuous.Any(str, unicode), voluptuous.Length(max=255))})

metric_schema = {
    voluptuous.Required('name'): voluptuous.Any(str, unicode),
    voluptuous.Optional('dimensions'):
        {voluptuous.All(
         voluptuous.Any(str, unicode), voluptuous.Length(max=255)):
         voluptuous.All(voluptuous.Any(str, unicode), voluptuous.Length(max=255))},
    voluptuous.Required('timestamp'): voluptuous.All(voluptuous.Any(int, float), voluptuous.Range(min=0)),
    voluptuous.Required('value'): voluptuous.Any(int, float)}

request_body_schema = voluptuous.Schema(
    voluptuous.Any(metric_schema, [metric_schema]))


def val_voluptuous(msg):
    request_body_schema(msg)


test_metric_schema = {
    'type': 'object',
    'properties': {
        'timestamp': {'type': 'number'},
        'value': {'type': 'number'},
        'name': {
            'type': 'string',
            'maxLength': 64,
        },
        'dimensions': {'type': 'object'}
    }}

# test_schema = {'type': 'object'}


def val_jsonschema(msg):
    for m in msg:
        jsonschema.validate(m, test_metric_schema)


def val_custom(msg):
    if isinstance(msg, list):
        for m in msg:
            validate_single_metric(m)
    else:
        validate_single_metric(msg)


def validate_single_metric(metric):
    assert isinstance(metric['name'], (str, unicode))
    assert len(metric['name']) <= 64
    assert isinstance(metric['timestamp'], (int, float))
    assert isinstance(metric['value'], (int, long, float, complex))
    if "dimensions" in metric:
        for d in metric['dimensions']:
            assert isinstance(d, (str, unicode))
            assert len(d) <= 255
            assert isinstance(metric['dimensions'][d], (str, unicode))
            assert len(metric['dimensions'][d]) <= 255


if __name__ == '__main__':
    print(timeit.repeat("val_voluptuous({})".format(sample_data),
                        number=1000,
                        repeat=3,
                        setup="from __main__ import val_voluptuous"))
#    print(timeit.repeat("val_jsonschema({})".format(sample_data),
#                        number=1000,
#                        repeat=3,
#                        setup="from __main__ import val_jsonschema"))
    print(timeit.repeat("val_custom({})".format(sample_data),
                        number=1000,
                        repeat=3,
                        setup="from __main__ import val_custom"))
