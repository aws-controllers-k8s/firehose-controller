# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
# 	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.

"""Integration tests for Delivery Stream API.
"""

import boto3
import pytest
import logging
from typing import Dict, Tuple

from acktest.resources import random_suffix_name
from acktest.tags import assert_equal_without_ack_tags
from acktest.k8s import resource as k8s, condition as condition
from e2e import (
    service_marker,
    CRD_GROUP,
    CRD_VERSION,
    load_firehose_resource,
)
from e2e.bootstrap_resources import get_bootstrap_resources
from e2e.replacement_values import REPLACEMENT_VALUES

# Bounds the wait for the controller to reconcile a patched spec change.
RESYNC_WAIT_PERIODS = 30
RESYNC_WAIT_PERIOD_LENGTH = 10


@pytest.fixture(scope="module")
def http_dest_delivery_stream():
    bootstrapped_resources = get_bootstrap_resources()

    resource_name = random_suffix_name("http-dest-delivery-stream", 48)
    dest_name = random_suffix_name("http-dest", 20)
    replacements = REPLACEMENT_VALUES.copy()
    replacements["DELIVERY_STREAM_NAME"] = resource_name
    replacements["DELIVERY_STREAM_TYPE"] = "DirectPut"
    replacements["HTTP_DEST_NAME"] = dest_name
    replacements["HTTP_DEST_URL"] = "https://example.com"
    replacements["S3_BUCKET_ARN"] = f"arn:aws:s3:::{bootstrapped_resources.HttpDestBucket.name}"
    replacements["S3_ROLE_ARN"] = bootstrapped_resources.HttpDestBucketRole.arn

    resource_data = load_firehose_resource(
        "delivery_stream_httpdest",
        additional_replacements=replacements,
    )

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, "deliverystreams",
        resource_name, namespace="default",
    )

    # Create delivery stream
    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    yield (ref, cr)

    # Delete delivery stream
    try:
        _, deleted = k8s.delete_custom_resource(ref, wait_periods=3, period_length=10)
        assert deleted
    except:
        pass

@pytest.fixture(scope="module")
def extended_s3_delivery_stream():
    bootstrapped_resources = get_bootstrap_resources()

    resource_name = random_suffix_name("extended-s3-delivery-stream", 48)
    replacements = REPLACEMENT_VALUES.copy()
    replacements["DELIVERY_STREAM_NAME"] = resource_name
    replacements["DELIVERY_STREAM_TYPE"] = "DirectPut"
    replacements["S3_BUCKET_ARN"] = f"arn:aws:s3:::{bootstrapped_resources.HttpDestBucket.name}"
    replacements["S3_ROLE_ARN"] = bootstrapped_resources.HttpDestBucketRole.arn

    resource_data = load_firehose_resource(
        "delivery_stream_extendeds3",
        additional_replacements=replacements,
    )

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, "deliverystreams",
        resource_name, namespace="default",
    )

    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    yield (ref, cr)

    try:
        _, deleted = k8s.delete_custom_resource(ref, wait_periods=3, period_length=10)
        assert deleted
    except:
        pass


@pytest.fixture(scope="module")
def splunk_delivery_stream():
    bootstrapped_resources = get_bootstrap_resources()

    resource_name = random_suffix_name("splunk-delivery-stream", 48)
    secret_name = random_suffix_name("hec-token", 24)
    secret_key = "token"
    k8s.create_opaque_secret("default", secret_name, secret_key, "00000000-0000-0000-0000-000000000000")

    replacements = REPLACEMENT_VALUES.copy()
    replacements["DELIVERY_STREAM_NAME"] = resource_name
    replacements["DELIVERY_STREAM_TYPE"] = "DirectPut"
    replacements["HEC_ENDPOINT"] = "https://splunk.example.com:8088"
    replacements["HEC_TOKEN_SECRET_NAME"] = secret_name
    replacements["HEC_TOKEN_SECRET_KEY"] = secret_key
    replacements["S3_BUCKET_ARN"] = f"arn:aws:s3:::{bootstrapped_resources.HttpDestBucket.name}"
    replacements["S3_ROLE_ARN"] = bootstrapped_resources.HttpDestBucketRole.arn

    resource_data = load_firehose_resource(
        "delivery_stream_splunk",
        additional_replacements=replacements,
    )

    ref = k8s.CustomResourceReference(
        CRD_GROUP, CRD_VERSION, "deliverystreams",
        resource_name, namespace="default",
    )

    k8s.create_custom_resource(ref, resource_data)
    cr = k8s.wait_resource_consumed_by_controller(ref)

    assert cr is not None
    assert k8s.get_resource_exists(ref)

    yield (ref, cr)

    try:
        _, deleted = k8s.delete_custom_resource(ref, wait_periods=3, period_length=10)
        assert deleted
    except:
        pass
    k8s.delete_secret("default", secret_name)


@service_marker
@pytest.mark.canary
class TestDeliveryStream:
    def test_update_delivery_stream_encryption(self, http_dest_delivery_stream, firehose_client):
        (ref, cr) = http_dest_delivery_stream
        k8s.wait_on_condition(ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True")
        cr = k8s.get_resource(ref)
        print(cr)
        condition.assert_synced(ref)

        # Confirm that server-side encryption is disabled 
        assert cr["spec"]["deliveryStreamName"] is not None
        assert cr["status"]["deliveryStreamStatus"] == "ACTIVE"
        assert cr["status"]["deliveryStreamEncryptionConfigurationStatus"] == "DISABLED"
        assert "deliveryStreamEncryptionConfiguration" not in cr["spec"]

        dsName = cr["spec"]["deliveryStreamName"]

        delivery_stream = firehose_client.describe_delivery_stream(DeliveryStreamName=dsName)
        assert delivery_stream is not None
        assert delivery_stream["DeliveryStreamDescription"]["DeliveryStreamStatus"] == "ACTIVE"
        assert delivery_stream["DeliveryStreamDescription"]["DeliveryStreamEncryptionConfiguration"]["Status"] == "DISABLED"

        # Update to enable server-side encryption
        updates = {
            "spec": {
                "deliveryStreamEncryptionConfiguration": {
                    "keyType": "AWS_OWNED_CMK"
                }
            }
        }

        synced_at = condition.get_synced_last_transition_time(ref)
        k8s.patch_custom_resource(ref, updates)

        # Wait for a fresh reconcile (implies AWS finished ENABLING).
        assert k8s.wait_on_condition_after(
            ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True",
            last_transition_after=synced_at,
            wait_periods=RESYNC_WAIT_PERIODS, period_length=RESYNC_WAIT_PERIOD_LENGTH,
        )
        condition.assert_synced(ref)

        # Confirm that server-side encryption has been successfully enabled.
        cr = k8s.get_resource(ref)
        assert cr is not None
        assert cr["status"]["deliveryStreamStatus"] == "ACTIVE"
        assert cr["status"]["deliveryStreamEncryptionConfigurationStatus"] == "ENABLED"

        delivery_stream = firehose_client.describe_delivery_stream(DeliveryStreamName=dsName)
        assert delivery_stream is not None
        assert delivery_stream["DeliveryStreamDescription"]["DeliveryStreamStatus"] == "ACTIVE"
        assert delivery_stream["DeliveryStreamDescription"]["DeliveryStreamEncryptionConfiguration"]["Status"] == "ENABLED"
        assert delivery_stream["DeliveryStreamDescription"]["DeliveryStreamEncryptionConfiguration"]["KeyType"] == "AWS_OWNED_CMK"

        # Explicitly disable server-side encryption by setting KeyType and KeyARN to null.
        updates = {
            "spec": {
                "deliveryStreamEncryptionConfiguration": {
                    "keyType": None,
                    "keyARN": None
                }
            }
        }

        synced_at = condition.get_synced_last_transition_time(ref)
        k8s.patch_custom_resource(ref, updates)

        # Wait for a fresh reconcile (implies AWS finished DISABLING, so the
        # fixture can delete the stream without ResourceInUseException).
        assert k8s.wait_on_condition_after(
            ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True",
            last_transition_after=synced_at,
            wait_periods=RESYNC_WAIT_PERIODS, period_length=RESYNC_WAIT_PERIOD_LENGTH,
        )
        condition.assert_synced(ref)

        # Confirm that server-side encryption has been successfully disabled.
        cr = k8s.get_resource(ref)
        assert cr is not None
        assert cr["status"]["deliveryStreamStatus"] == "ACTIVE"
        assert cr["status"]["deliveryStreamEncryptionConfigurationStatus"] == "DISABLED"

        delivery_stream = firehose_client.describe_delivery_stream(DeliveryStreamName=dsName)
        assert delivery_stream is not None
        assert delivery_stream["DeliveryStreamDescription"]["DeliveryStreamStatus"] == "ACTIVE"
        assert delivery_stream["DeliveryStreamDescription"]["DeliveryStreamEncryptionConfiguration"]["Status"] == "DISABLED"
        

    def test_create_delete_http_dest_delivery_stream(self, http_dest_delivery_stream, firehose_client):
        (ref, cr) = http_dest_delivery_stream

        k8s.wait_on_condition(ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True")
        cr = k8s.get_resource(ref)
        condition.assert_synced(ref)

        assert cr["spec"]["deliveryStreamName"] is not None
        assert cr["status"]["deliveryStreamStatus"] == "ACTIVE"
        assert cr["spec"]["httpEndpointDestinationConfiguration"]["endpointConfiguration"]["name"] is not None

        assert len(cr["spec"]["tags"]) == 2
        assert cr["spec"]["tags"][0]["key"] == "environment"
        assert cr["spec"]["tags"][0]["value"] == "dev"
        assert cr["spec"]["tags"][1]["key"] == "team"
        assert cr["spec"]["tags"][1]["value"] == "finops"

        ds_name = cr["spec"]["deliveryStreamName"]
        http_dest_name = cr["spec"]["httpEndpointDestinationConfiguration"]["endpointConfiguration"]["name"]

        delivery_stream = firehose_client.describe_delivery_stream(DeliveryStreamName=ds_name)
        assert delivery_stream is not None
        assert delivery_stream["DeliveryStreamDescription"]["DeliveryStreamStatus"] == "ACTIVE"
        assert delivery_stream["DeliveryStreamDescription"]["DeliveryStreamType"] == "DirectPut"
        assert len(delivery_stream["DeliveryStreamDescription"]["Destinations"]) == 1
        assert delivery_stream["DeliveryStreamDescription"]["Destinations"][0]["HttpEndpointDestinationDescription"]["EndpointConfiguration"]["Name"] == http_dest_name


        expected_tags = [{"Key": "environment", "Value": "dev"}, {"Key": "team", "Value": "finops"}]
        stream_tags = firehose_client.list_tags_for_delivery_stream(DeliveryStreamName=ds_name)
        assert_equal_without_ack_tags(expected_tags, stream_tags["Tags"])
        

        updated_dest_name = random_suffix_name("http-dest-updated", 32)
        updates = {
            "spec": {
                "httpEndpointDestinationConfiguration": {
                    "endpointConfiguration": {
                        "name": updated_dest_name
                    }
                },
                "tags": [
                    {"key": "environment", "value": "staging"}, 
                    {"key": "department", "value": "finance"}
                ]
            }
        }

        synced_at = condition.get_synced_last_transition_time(ref)
        k8s.patch_custom_resource(ref, updates)

        # Wait for a fresh reconcile after the patch.
        assert k8s.wait_on_condition_after(
            ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True",
            last_transition_after=synced_at,
            wait_periods=RESYNC_WAIT_PERIODS, period_length=RESYNC_WAIT_PERIOD_LENGTH,
        )
        condition.assert_synced(ref)

        cr = k8s.get_resource(ref)
        assert cr is not None
        assert cr["status"]["deliveryStreamStatus"] == "ACTIVE"
        assert cr["spec"]["httpEndpointDestinationConfiguration"]["endpointConfiguration"]["name"] == updated_dest_name

        assert len(cr["spec"]["tags"]) == 2
        assert cr["spec"]["tags"][0]["key"] == "environment"
        assert cr["spec"]["tags"][0]["value"] == "staging"
        assert cr["spec"]["tags"][1]["key"] == "department"
        assert cr["spec"]["tags"][1]["value"] == "finance"


        updated_delivery_stream = firehose_client.describe_delivery_stream(DeliveryStreamName=ds_name)
        assert updated_delivery_stream is not None
        assert updated_delivery_stream["DeliveryStreamDescription"]["DeliveryStreamStatus"] == "ACTIVE"
        assert updated_delivery_stream["DeliveryStreamDescription"]["DeliveryStreamType"] == "DirectPut"
        assert len(updated_delivery_stream["DeliveryStreamDescription"]["Destinations"]) == 1
        assert updated_delivery_stream["DeliveryStreamDescription"]["Destinations"][0]["HttpEndpointDestinationDescription"]["EndpointConfiguration"]["Name"] == updated_dest_name


        updated_tags = [{"Key": "environment", "Value": "staging"}, {"Key": "department", "Value": "finance"}]
        updated_stream_tags = firehose_client.list_tags_for_delivery_stream(DeliveryStreamName=ds_name)
        assert_equal_without_ack_tags(updated_tags, updated_stream_tags["Tags"])







        
        

        

    def test_create_update_delete_extended_s3_delivery_stream(self, extended_s3_delivery_stream, firehose_client):
        (ref, cr) = extended_s3_delivery_stream
        bootstrapped_resources = get_bootstrap_resources()
        bucket_arn = f"arn:aws:s3:::{bootstrapped_resources.HttpDestBucket.name}"

        k8s.wait_on_condition(ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True")
        cr = k8s.get_resource(ref)
        condition.assert_synced(ref)

        assert cr["status"]["deliveryStreamStatus"] == "ACTIVE"
        assert cr["spec"]["extendedS3DestinationConfiguration"]["bucketARN"] == bucket_arn
        assert cr["spec"]["extendedS3DestinationConfiguration"]["bufferingHints"]["intervalInSeconds"] == 60
        assert "httpEndpointDestinationConfiguration" not in cr["spec"]

        ds_name = cr["spec"]["deliveryStreamName"]
        delivery_stream = firehose_client.describe_delivery_stream(DeliveryStreamName=ds_name)
        description = delivery_stream["DeliveryStreamDescription"]
        assert description["DeliveryStreamStatus"] == "ACTIVE"
        assert len(description["Destinations"]) == 1
        destination = description["Destinations"][0]["ExtendedS3DestinationDescription"]
        assert destination["BucketARN"] == bucket_arn
        assert destination["BufferingHints"]["IntervalInSeconds"] == 60
        assert destination["BufferingHints"]["SizeInMBs"] == 1

        expected_tags = [{"Key": "environment", "Value": "dev"}]
        stream_tags = firehose_client.list_tags_for_delivery_stream(DeliveryStreamName=ds_name)
        assert_equal_without_ack_tags(expected_tags, stream_tags["Tags"])

        updates = {
            "spec": {
                "extendedS3DestinationConfiguration": {
                    "prefix": "updated/",
                    "bufferingHints": {
                        "intervalInSeconds": 120,
                        "sizeInMBs": 2,
                    },
                },
            }
        }

        synced_at = condition.get_synced_last_transition_time(ref)
        k8s.patch_custom_resource(ref, updates)

        assert k8s.wait_on_condition_after(
            ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True",
            last_transition_after=synced_at,
            wait_periods=RESYNC_WAIT_PERIODS, period_length=RESYNC_WAIT_PERIOD_LENGTH,
        )
        condition.assert_synced(ref)

        cr = k8s.get_resource(ref)
        assert cr["status"]["deliveryStreamStatus"] == "ACTIVE"
        assert cr["spec"]["extendedS3DestinationConfiguration"]["prefix"] == "updated/"

        delivery_stream = firehose_client.describe_delivery_stream(DeliveryStreamName=ds_name)
        destination = delivery_stream["DeliveryStreamDescription"]["Destinations"][0]["ExtendedS3DestinationDescription"]
        assert destination["Prefix"] == "updated/"
        assert destination["BufferingHints"]["IntervalInSeconds"] == 120
        assert destination["BufferingHints"]["SizeInMBs"] == 2

    def test_create_update_delete_splunk_delivery_stream(self, splunk_delivery_stream, firehose_client):
        (ref, cr) = splunk_delivery_stream
        bootstrapped_resources = get_bootstrap_resources()
        bucket_arn = f"arn:aws:s3:::{bootstrapped_resources.HttpDestBucket.name}"

        k8s.wait_on_condition(ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True")
        cr = k8s.get_resource(ref)
        condition.assert_synced(ref)

        assert cr["status"]["deliveryStreamStatus"] == "ACTIVE"
        splunk = cr["spec"]["splunkDestinationConfiguration"]
        assert splunk["hECEndpoint"] == "https://splunk.example.com:8088"
        assert splunk["hECEndpointType"] == "Raw"
        assert splunk["s3Configuration"]["bucketARN"] == bucket_arn
        assert "extendedS3DestinationConfiguration" not in cr["spec"]

        ds_name = cr["spec"]["deliveryStreamName"]
        delivery_stream = firehose_client.describe_delivery_stream(DeliveryStreamName=ds_name)
        description = delivery_stream["DeliveryStreamDescription"]
        assert description["DeliveryStreamStatus"] == "ACTIVE"
        assert len(description["Destinations"]) == 1
        destination = description["Destinations"][0]["SplunkDestinationDescription"]
        assert destination["HECEndpoint"] == "https://splunk.example.com:8088"
        assert destination["HECEndpointType"] == "Raw"
        assert destination["HECAcknowledgmentTimeoutInSeconds"] == 180
        assert destination["S3BackupMode"] == "FailedEventsOnly"
        assert destination["S3DestinationDescription"]["BucketARN"] == bucket_arn

        updates = {
            "spec": {
                "splunkDestinationConfiguration": {
                    "hECEndpointType": "Event",
                    "hECAcknowledgmentTimeoutInSeconds": 240,
                },
            }
        }

        synced_at = condition.get_synced_last_transition_time(ref)
        k8s.patch_custom_resource(ref, updates)

        assert k8s.wait_on_condition_after(
            ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True",
            last_transition_after=synced_at,
            wait_periods=RESYNC_WAIT_PERIODS, period_length=RESYNC_WAIT_PERIOD_LENGTH,
        )
        condition.assert_synced(ref)

        cr = k8s.get_resource(ref)
        assert cr["status"]["deliveryStreamStatus"] == "ACTIVE"
        assert cr["spec"]["splunkDestinationConfiguration"]["hECEndpointType"] == "Event"

        delivery_stream = firehose_client.describe_delivery_stream(DeliveryStreamName=ds_name)
        destination = delivery_stream["DeliveryStreamDescription"]["Destinations"][0]["SplunkDestinationDescription"]
        assert destination["HECEndpointType"] == "Event"
        assert destination["HECAcknowledgmentTimeoutInSeconds"] == 240
