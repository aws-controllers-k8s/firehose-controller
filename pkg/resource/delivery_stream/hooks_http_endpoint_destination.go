// Copyright Amazon.com Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"). You may
// not use this file except in compliance with the License. A copy of the
// License is located at
//
//     http://aws.amazon.com/apache2.0/
//
// or in the "license" file accompanying this file. This file is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
// express or implied. See the License for the specific language governing
// permissions and limitations under the License.

package delivery_stream

import (
	svcapitypes "github.com/aws-controllers-k8s/firehose-controller/apis/v1alpha1"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
)

// readHttpDestinationDescription writes the described destination onto the
// spec, nil included: Firehose leaves unset fields out of the description, and
// keeping the desired value there hides a spec change from the delta. The
// access key secret reference and the resource reference wrappers are never
// described and stay as they are.
func readHttpDestinationDescription(ko *svcapitypes.DeliveryStream, respHttpDest *svcsdktypes.HttpEndpointDestinationDescription) {
	if respHttpDest == nil {
		return
	}

	if ko.Spec.HTTPEndpointDestinationConfiguration == nil {
		ko.Spec.HTTPEndpointDestinationConfiguration = &svcapitypes.HTTPEndpointDestinationConfiguration{}
	}
	spec := ko.Spec.HTTPEndpointDestinationConfiguration

	spec.S3BackupMode = nil
	if respHttpDest.S3BackupMode != "" {
		spec.S3BackupMode = aws.String(string(respHttpDest.S3BackupMode))
	}
	spec.RoleARN = respHttpDest.RoleARN

	readBufferingHints(spec, respHttpDest.BufferingHints)
	readEndpointConfiguration(spec, respHttpDest.EndpointConfiguration)
	readRetryOptions(spec, respHttpDest.RetryOptions)
	readCloudwatchLoggingOptions(spec, respHttpDest.CloudWatchLoggingOptions)
	readProcessingConfiguration(spec, respHttpDest.ProcessingConfiguration)
	readRequestConfiguration(spec, respHttpDest.RequestConfiguration)
	readS3DestinationDescription(spec, respHttpDest.S3DestinationDescription)
	readSecretsManagerConfiguration(spec, respHttpDest.SecretsManagerConfiguration)
}

func readBufferingHints(spec *svcapitypes.HTTPEndpointDestinationConfiguration, resp *svcsdktypes.HttpEndpointBufferingHints) {
	spec.BufferingHints = nil
	if resp == nil {
		return
	}
	spec.BufferingHints = &svcapitypes.HTTPEndpointBufferingHints{}
	if resp.IntervalInSeconds != nil {
		spec.BufferingHints.IntervalInSeconds = aws.Int64(int64(*resp.IntervalInSeconds))
	}
	if resp.SizeInMBs != nil {
		spec.BufferingHints.SizeInMBs = aws.Int64(int64(*resp.SizeInMBs))
	}
}

// readEndpointConfiguration keeps the access key secret reference, which the
// description never carries.
func readEndpointConfiguration(spec *svcapitypes.HTTPEndpointDestinationConfiguration, resp *svcsdktypes.HttpEndpointDescription) {
	if resp == nil {
		return
	}
	if spec.EndpointConfiguration == nil {
		spec.EndpointConfiguration = &svcapitypes.HTTPEndpointConfiguration{}
	}
	spec.EndpointConfiguration.Name = resp.Name
	spec.EndpointConfiguration.URL = resp.Url
}

func readRetryOptions(spec *svcapitypes.HTTPEndpointDestinationConfiguration, resp *svcsdktypes.HttpEndpointRetryOptions) {
	spec.RetryOptions = nil
	if resp == nil {
		return
	}
	spec.RetryOptions = &svcapitypes.HTTPEndpointRetryOptions{}
	if resp.DurationInSeconds != nil {
		spec.RetryOptions.DurationInSeconds = aws.Int64(int64(*resp.DurationInSeconds))
	}
}

func readCloudwatchLoggingOptions(spec *svcapitypes.HTTPEndpointDestinationConfiguration, resp *svcsdktypes.CloudWatchLoggingOptions) {
	spec.CloudWatchLoggingOptions = nil
	if resp == nil {
		return
	}
	spec.CloudWatchLoggingOptions = cloudWatchLoggingOptionsFromSDK(resp)
}

func readProcessingConfiguration(spec *svcapitypes.HTTPEndpointDestinationConfiguration, resp *svcsdktypes.ProcessingConfiguration) {
	spec.ProcessingConfiguration = nil
	if resp == nil {
		return
	}
	spec.ProcessingConfiguration = processingConfigurationFromSDK(resp)
}

func readRequestConfiguration(spec *svcapitypes.HTTPEndpointDestinationConfiguration, resp *svcsdktypes.HttpEndpointRequestConfiguration) {
	spec.RequestConfiguration = nil
	if resp == nil {
		return
	}
	spec.RequestConfiguration = &svcapitypes.HTTPEndpointRequestConfiguration{}
	if resp.ContentEncoding != "" {
		spec.RequestConfiguration.ContentEncoding = aws.String(string(resp.ContentEncoding))
	}
	if resp.CommonAttributes != nil {
		spec.RequestConfiguration.CommonAttributes = make([]*svcapitypes.HTTPEndpointCommonAttribute, len(resp.CommonAttributes))
		for i, attr := range resp.CommonAttributes {
			spec.RequestConfiguration.CommonAttributes[i] = &svcapitypes.HTTPEndpointCommonAttribute{
				AttributeName:  attr.AttributeName,
				AttributeValue: attr.AttributeValue,
			}
		}
	}
}

// readS3DestinationDescription merges in place so the bucket, role and KMS
// key reference wrappers survive.
func readS3DestinationDescription(spec *svcapitypes.HTTPEndpointDestinationConfiguration, resp *svcsdktypes.S3DestinationDescription) {
	if resp == nil {
		return
	}
	if spec.S3Configuration == nil {
		spec.S3Configuration = &svcapitypes.S3DestinationConfiguration{}
	}
	mergeS3DestinationDescription(spec.S3Configuration, resp)
}

// readSecretsManagerConfiguration merges in place so the role and secret
// reference wrappers survive, and clears the block when it is not described.
func readSecretsManagerConfiguration(spec *svcapitypes.HTTPEndpointDestinationConfiguration, resp *svcsdktypes.SecretsManagerConfiguration) {
	if resp == nil {
		spec.SecretsManagerConfiguration = nil
		return
	}
	if spec.SecretsManagerConfiguration == nil {
		spec.SecretsManagerConfiguration = &svcapitypes.SecretsManagerConfiguration{}
	}
	spec.SecretsManagerConfiguration.Enabled = resp.Enabled
	spec.SecretsManagerConfiguration.RoleARN = resp.RoleARN
	spec.SecretsManagerConfiguration.SecretARN = resp.SecretARN
}
