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
import time
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

UPDATE_WAIT_SECONDS = 10


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

        k8s.patch_custom_resource(ref, updates)
        time.sleep(UPDATE_WAIT_SECONDS)

        k8s.wait_on_condition(ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True")
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

        k8s.patch_custom_resource(ref, updates)
        time.sleep(UPDATE_WAIT_SECONDS)

        k8s.wait_on_condition(ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True")
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

        k8s.patch_custom_resource(ref, updates)
        time.sleep(UPDATE_WAIT_SECONDS)

        k8s.wait_on_condition(ref, condition.CONDITION_TYPE_RESOURCE_SYNCED, "True")
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







        
        

        