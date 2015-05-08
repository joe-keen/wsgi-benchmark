import json
import logging
import time

import kafka
import pecan
import voluptuous

from oslo_utils import timeutils
from pecan import expose

logger = logging.getLogger(__name__)


kafka_url = "192.168.10.4:9092"

client = kafka.client.KafkaClient(kafka_url)
producer = kafka.producer.KeyedProducer(
    client,
    async=False,
    req_acks=kafka.producer.KeyedProducer.ACK_AFTER_LOCAL_WRITE,
    ack_timeout=2000)


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


def validate(msg):
    try:
        request_body_schema(msg)
    except Exception:
        logger.exception("")
        raise


def transform(metrics, tenant_id, region):
    transformed_metric = {'metric': {},
                          'meta': {'tenantId': tenant_id, 'region': region},
                          'creation_time': timeutils.utcnow_ts()}

    if isinstance(metrics, list):
        transformed_metrics = []
        for metric in metrics:
            transformed_metric['metric'] = metric
            transformed_metrics.append(json.dumps(transformed_metric))
        return transformed_metrics
    else:
        transformed_metric['metric'] = metrics
        return transformed_metric


class Metrics(object):
    @expose(generic=True, template='json')
    def index(self):
        return dict()

    # HTTP POST /
    @index.when(method='POST', template='json')
    def foo(self):
        metrics = json.loads(pecan.request.body)
        # validate(metrics)
        val_custom(metrics)
        transformed_metrics = transform(metrics, "foo", "bar")
        key = time.time() * 1000

        partition = producer._next_partition("metrics", key)
        producer.send_messages("metrics", partition, *transformed_metrics)

        # producer.send("metrics", key, json.dumps(transformed_metrics))
        # if isinstance(transformed_metrics, list):
        #    for metric in transformed_metrics:
        #        producer.send("metrics", key, json.dumps(metric))
        # else:
        #    producer.send("metrics", key, json.dumps(transformed_metrics))
        return ""


class V2(object):
    metrics = Metrics()


class RootController(object):
    v2 = V2()
