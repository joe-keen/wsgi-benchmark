import copy
import json
import logging
import time

import falcon
import kafka
import voluptuous

from oslo_utils import timeutils


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
            transformed_metrics.append(copy.deepcopy(transformed_metric))
        return transformed_metrics
    else:
        transformed_metric['metric'] = metrics
        return transformed_metric


class MetricCatch(object):
    def on_post(self, req, resp):
        metrics = json.loads(req.stream.read())
        validate(metrics)
        transformed_metrics = transform(metrics, "foo", "bar")

        key = time.time() * 1000
        producer.send("metrics", key, json.dumps(transformed_metrics))
        resp.status = falcon.HTTP_200


application = falcon.API()

m = MetricCatch()
application.add_route("/v2/metrics", m)
