import ray
import urllib
import logging
import yaml

from deltacat import logs
from enum import Enum
from typing import Dict, Any, List, Callable
from deltacat.aws.clients import resource_cache
from datetime import datetime

from botocore.exceptions import ClientError
from ray._private.services import get_node_ip_address

logger = logs.configure_deltacat_logger(logging.getLogger(__name__))

DEFAULT_DELTACAT_METRICS_NAMESPACE = "ray-deltacat-profiling"


class MetricsConfig:
    def __init__(self, region: str, job_run_id: str):
        self.region = region
        self.job_run_id = job_run_id


class MetricsType(str, Enum):
    TIMER = "timer"


class MetricsDimensionType(str, Enum):
    NODE_IP = "node_ip"
    RAY_TASK_ID = "task_id"
    RAY_WORKER_ID = "worker_id"


def _build_metrics_name(metrics_type: Enum, metrics_name: str) -> str:
    metrics_name_with_type = f"{metrics_name}_{metrics_type}"
    return metrics_name_with_type


def _get_current_node_ip() -> str:
    # current_node_ip = urllib.request.urlopen(
    #     "http://169.254.169.254/latest/meta-data/local-ipv4"
    # ).read().decode("utf-8")
    current_node_ip = get_node_ip_address()
    return {"Name": f"node_ip",
            "Value": f"{current_node_ip}"
            }


def _get_current_task_id() -> Dict[str, Any]:
    return {"Name": f"ray_task_id",
            "Value": f"{ray.get_runtime_context().get_task_id()}"
            }


def _get_current_worker_id() -> Dict[str, Any]:
    return {"Name": f"ray_worker_id",
            "Value": f"{ray.get_runtime_context().worker.core_worker.get_worker_id()}"
            }


METRICS_DIMENSION_TYPE_TO_VALUE_DICT: Dict[
            str, Callable] = {
            MetricsDimensionType.NODE_IP.value: _get_current_node_ip,
            MetricsDimensionType.RAY_TASK_ID.value: _get_current_task_id,
            MetricsDimensionType.RAY_WORKER_ID.value: _get_current_worker_id,
        }

# def _get_current_instance_region() -> str:
#     current_instance_region = urllib.request.urlopen(
#         "http://169.254.169.254/latest/meta-data/placement/region"
#     ).read().decode("utf-8")
#     return current_instance_region


# def _get_current_cluster_name() -> str:
#     with open("/home/ubuntu/ray_bootstrap_config.yaml", "r") as f:
#         cluster_config = yaml.load(f)
#     return cluster_config["cluster_name"]


def _build_cloudwatch_metrics(metrics_name: str, metrics_type: Enum, value: str, dimension_types: List[Enum], timestamp: datetime, **kwargs) -> Dict[str, Any]:
    metrics_name_with_type = _build_metrics_name(metrics_type, metrics_name)
    dimensions = []
    for dimension_type in dimension_types:
        dimensions.append(METRICS_DIMENSION_TYPE_TO_VALUE_DICT.get(dimension_type.value)())
    return [
        {
            "MetricName": f"{metrics_name_with_type}",
            "Dimensions": dimensions,
            "Timestamp": timestamp,
            "Value": value,
            **kwargs
        }
    ]


def _emit_metrics(metrics_name: str, metrics_type: Enum, metrics_config: MetricsConfig, value: str, dimension_types: List[Enum], **kwargs) -> None:
    ct = datetime.datetime.now()
    current_instance_region = metrics_config.region
    cloudwatch_resource = resource_cache("cloudwatch", current_instance_region)
    cloudwatch_client = cloudwatch_resource.meta.client
    metrics_data = _build_cloudwatch_metrics(metrics_name,
                                             metrics_type,
                                             value,
                                             dimension_types,
                                             ct,
                                             **kwargs)
    job_run_id = metrics_config.job_run_id
    try:
        response = cloudwatch_client.put_metric_data(
            Namespace=f"DEFAULT_DELTACAT_METRICS_NAMESPACE_{job_run_id}",
            MetricData=metrics_data)
    except ClientError as e:
        logger.warning(f"Failed to publish Cloudwatch metrics with name: {metrics_name}, "
                       f"type: {metrics_type}, with botocore client exception: {e}")


def emit_timer_metrics(metrics_name, value, metrics_config, **kwargs):
    metrics_dimension_type = [MetricsDimensionType.NODE_IP, MetricsDimensionType.RAY_TASK_ID,
                              MetricsDimensionType.RAY_WORKER_ID]
    _emit_metrics(metrics_name=metrics_name,
                  metrics_type=MetricsType.TIMER,
                  metrics_config=metrics_config,
                  value=value,
                  dimension_types=metrics_dimension_type,
                  **kwargs)