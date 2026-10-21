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

// Maps a SplunkDestinationDescription onto the Spec, keeping the reference
// wrappers and the HEC token secret reference the description cannot carry.
func readSplunkDestinationDescription(ko *svcapitypes.DeliveryStream, resp *svcsdktypes.SplunkDestinationDescription) {
	if resp == nil {
		return
	}

	if ko.Spec.SplunkDestinationConfiguration == nil {
		ko.Spec.SplunkDestinationConfiguration = &svcapitypes.SplunkDestinationConfiguration{}
	}
	spec := ko.Spec.SplunkDestinationConfiguration

	if resp.HECEndpoint != nil {
		spec.HECEndpoint = resp.HECEndpoint
	}
	if resp.HECEndpointType != "" {
		spec.HECEndpointType = aws.String(string(resp.HECEndpointType))
	}
	if resp.HECAcknowledgmentTimeoutInSeconds != nil {
		spec.HECAcknowledgmentTimeoutInSeconds = aws.Int64(int64(*resp.HECAcknowledgmentTimeoutInSeconds))
	}
	if resp.RetryOptions != nil {
		spec.RetryOptions = &svcapitypes.SplunkRetryOptions{}
		if resp.RetryOptions.DurationInSeconds != nil {
			spec.RetryOptions.DurationInSeconds = aws.Int64(int64(*resp.RetryOptions.DurationInSeconds))
		}
	}
	if resp.S3BackupMode != "" {
		spec.S3BackupMode = aws.String(string(resp.S3BackupMode))
	}
	if resp.S3DestinationDescription != nil {
		if spec.S3Configuration == nil {
			spec.S3Configuration = &svcapitypes.S3DestinationConfiguration{}
		}
		mergeS3DestinationDescription(spec.S3Configuration, resp.S3DestinationDescription)
	}
	if resp.ProcessingConfiguration != nil {
		spec.ProcessingConfiguration = processingConfigurationFromSDK(resp.ProcessingConfiguration)
	}
	if resp.CloudWatchLoggingOptions != nil {
		spec.CloudWatchLoggingOptions = cloudWatchLoggingOptionsFromSDK(resp.CloudWatchLoggingOptions)
	}
	if resp.BufferingHints != nil {
		spec.BufferingHints = &svcapitypes.SplunkBufferingHints{}
		if resp.BufferingHints.IntervalInSeconds != nil {
			spec.BufferingHints.IntervalInSeconds = aws.Int64(int64(*resp.BufferingHints.IntervalInSeconds))
		}
		if resp.BufferingHints.SizeInMBs != nil {
			spec.BufferingHints.SizeInMBs = aws.Int64(int64(*resp.BufferingHints.SizeInMBs))
		}
	}
	if resp.SecretsManagerConfiguration != nil {
		if spec.SecretsManagerConfiguration == nil {
			spec.SecretsManagerConfiguration = &svcapitypes.SecretsManagerConfiguration{}
		}
		mergeSecretsManagerConfiguration(spec.SecretsManagerConfiguration, resp.SecretsManagerConfiguration)
	}
}

// mergeSecretsManagerConfiguration writes the observed values onto spec and
// keeps the role and secret reference wrappers.
func mergeSecretsManagerConfiguration(spec *svcapitypes.SecretsManagerConfiguration, resp *svcsdktypes.SecretsManagerConfiguration) {
	if resp.Enabled != nil {
		spec.Enabled = resp.Enabled
	}
	if resp.RoleARN != nil {
		spec.RoleARN = resp.RoleARN
	}
	if resp.SecretARN != nil {
		spec.SecretARN = resp.SecretARN
	}
}
