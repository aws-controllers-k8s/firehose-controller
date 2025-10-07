# Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You may
# not use this file except in compliance with the License. A copy of the
# License is located at
#
#	 http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
"""Bootstraps the resources required to run the Firehose integration tests.
"""
import logging

from acktest.bootstrapping import Resources, BootstrapFailureException
from acktest.bootstrapping.s3 import Bucket
from acktest.bootstrapping.iam import Role

from e2e import bootstrap_directory
from e2e.bootstrap_resources import BootstrapResources

S3_ACCESS_ROLE_ARN = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
FIREHOSE_SERVICE_PRINCIPAL = "firehose.amazonaws.com"

def service_bootstrap() -> Resources:
    logging.getLogger().setLevel(logging.INFO)

    resources = BootstrapResources(
        HttpDestBucket=Bucket("firehose-http-dest"),
        HttpDestBucketRole=Role(
            "firehose-http-dest-role", 
            principal_service=FIREHOSE_SERVICE_PRINCIPAL, 
            managed_policies=[S3_ACCESS_ROLE_ARN]
        )
    )

    try:
        resources.bootstrap()
    except BootstrapFailureException as ex:
        exit(254)

    return resources

if __name__ == "__main__":
    config = service_bootstrap()
    # Write config to current directory by default
    config.serialize(bootstrap_directory)
