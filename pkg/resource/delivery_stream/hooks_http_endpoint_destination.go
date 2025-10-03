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

// Maps an HttpEndpointDestinationDescription to relevant Spec and Status fields.
func readHttpDestinationDescription(ko *svcapitypes.DeliveryStream, respHttpDest *svcsdktypes.HttpEndpointDestinationDescription) {
	if respHttpDest == nil {
		return
	}

	if ko.Spec.HTTPEndpointDestinationConfiguration == nil {
		ko.Spec.HTTPEndpointDestinationConfiguration = &svcapitypes.HTTPEndpointDestinationConfiguration{}
	}
	specHttpEndpointDestConfig := ko.Spec.HTTPEndpointDestinationConfiguration

	if respHttpDest.S3BackupMode != "" {
		specHttpEndpointDestConfig.S3BackupMode = aws.String(string(respHttpDest.S3BackupMode))
	}
	if respHttpDest.RoleARN != nil {
		specHttpEndpointDestConfig.RoleARN = respHttpDest.RoleARN
	}

	readBufferingHints(specHttpEndpointDestConfig, respHttpDest.BufferingHints)
	readEndpointConfiguration(specHttpEndpointDestConfig, respHttpDest.EndpointConfiguration)
	readRetryOptions(specHttpEndpointDestConfig, respHttpDest.RetryOptions)
	readCloudwatchLoggingOptions(specHttpEndpointDestConfig, respHttpDest.CloudWatchLoggingOptions)
	readProcessingConfiguration(specHttpEndpointDestConfig, respHttpDest.ProcessingConfiguration)
	readRequestConfiguration(specHttpEndpointDestConfig, respHttpDest.RequestConfiguration)
	readS3DestinationDescription(specHttpEndpointDestConfig, respHttpDest.S3DestinationDescription)
	readSecretsManagerConfiguration(specHttpEndpointDestConfig, respHttpDest.SecretsManagerConfiguration)
}

func readBufferingHints(specHttpEndpointDestConfig *svcapitypes.HTTPEndpointDestinationConfiguration, respBufferingHints *svcsdktypes.HttpEndpointBufferingHints) {
	if respBufferingHints == nil {
		return
	}

	if specHttpEndpointDestConfig.BufferingHints == nil {
		specHttpEndpointDestConfig.BufferingHints = &svcapitypes.HTTPEndpointBufferingHints{}
	}
	if respBufferingHints.IntervalInSeconds != nil {
		specHttpEndpointDestConfig.BufferingHints.IntervalInSeconds = aws.Int64(int64(*respBufferingHints.IntervalInSeconds))
	}
	if respBufferingHints.SizeInMBs != nil {
		specHttpEndpointDestConfig.BufferingHints.SizeInMBs = aws.Int64(int64(*respBufferingHints.SizeInMBs))
	}
}

func readEndpointConfiguration(specHttpEndpointDestConfig *svcapitypes.HTTPEndpointDestinationConfiguration, respEndpointConfiguration *svcsdktypes.HttpEndpointDescription) {
	if respEndpointConfiguration == nil {
		return
	}

	if specHttpEndpointDestConfig.EndpointConfiguration == nil {
		specHttpEndpointDestConfig.EndpointConfiguration = &svcapitypes.HTTPEndpointConfiguration{}
	}
	if respEndpointConfiguration.Name != nil {
		specHttpEndpointDestConfig.EndpointConfiguration.Name = respEndpointConfiguration.Name
	}
	if respEndpointConfiguration.Url != nil {
		specHttpEndpointDestConfig.EndpointConfiguration.URL = respEndpointConfiguration.Url
	}
}

func readRetryOptions(specHttpEndpointDestConfig *svcapitypes.HTTPEndpointDestinationConfiguration, respRetryOptions *svcsdktypes.HttpEndpointRetryOptions) {
	if respRetryOptions == nil {
		return
	}

	if specHttpEndpointDestConfig.RetryOptions == nil {
		specHttpEndpointDestConfig.RetryOptions = &svcapitypes.HTTPEndpointRetryOptions{}
	}
	if respRetryOptions.DurationInSeconds != nil {
		specHttpEndpointDestConfig.RetryOptions.DurationInSeconds = aws.Int64(int64(*respRetryOptions.DurationInSeconds))
	}
}

func readCloudwatchLoggingOptions(specHttpEndpointDestConfig *svcapitypes.HTTPEndpointDestinationConfiguration, respCloudWatchLoggingOptions *svcsdktypes.CloudWatchLoggingOptions) {
	if respCloudWatchLoggingOptions == nil {
		return
	}

	if specHttpEndpointDestConfig.CloudWatchLoggingOptions == nil {
		specHttpEndpointDestConfig.CloudWatchLoggingOptions = &svcapitypes.CloudWatchLoggingOptions{}
	}
	if respCloudWatchLoggingOptions.Enabled != nil {
		specHttpEndpointDestConfig.CloudWatchLoggingOptions.Enabled = respCloudWatchLoggingOptions.Enabled
	}
	if respCloudWatchLoggingOptions.LogGroupName != nil {
		specHttpEndpointDestConfig.CloudWatchLoggingOptions.LogGroupName = respCloudWatchLoggingOptions.LogGroupName
	}
	if respCloudWatchLoggingOptions.LogStreamName != nil {
		specHttpEndpointDestConfig.CloudWatchLoggingOptions.LogStreamName = respCloudWatchLoggingOptions.LogStreamName
	}
}

func readProcessingConfiguration(specHttpEndpointDestConfig *svcapitypes.HTTPEndpointDestinationConfiguration, respProcessingConfiguration *svcsdktypes.ProcessingConfiguration) {
	if respProcessingConfiguration == nil {
		return
	}

	if specHttpEndpointDestConfig.ProcessingConfiguration == nil {
		specHttpEndpointDestConfig.ProcessingConfiguration = &svcapitypes.ProcessingConfiguration{}
	}
	if respProcessingConfiguration.Enabled != nil {
		specHttpEndpointDestConfig.ProcessingConfiguration.Enabled = respProcessingConfiguration.Enabled
	}
	if respProcessingConfiguration.Processors != nil {
		specHttpEndpointDestConfig.ProcessingConfiguration.Processors = make([]*svcapitypes.Processor, len(respProcessingConfiguration.Processors))
		for i, proc := range respProcessingConfiguration.Processors {
			specHttpEndpointDestConfig.ProcessingConfiguration.Processors[i] = &svcapitypes.Processor{}
			if proc.Type != "" {
				specHttpEndpointDestConfig.ProcessingConfiguration.Processors[i].Type = aws.String(string(proc.Type))
			}
			if proc.Parameters != nil {
				specHttpEndpointDestConfig.ProcessingConfiguration.Processors[i].Parameters = make([]*svcapitypes.ProcessorParameter, len(proc.Parameters))
				for j, param := range proc.Parameters {
					specHttpEndpointDestConfig.ProcessingConfiguration.Processors[i].Parameters[j] = &svcapitypes.ProcessorParameter{
						ParameterName:  aws.String(string(param.ParameterName)),
						ParameterValue: param.ParameterValue,
					}
				}
			}
		}
	}
}

func readRequestConfiguration(specHttpEndpointDestConfig *svcapitypes.HTTPEndpointDestinationConfiguration, respRequestConfiguration *svcsdktypes.HttpEndpointRequestConfiguration) {
	if respRequestConfiguration == nil {
		return
	}

	if specHttpEndpointDestConfig.RequestConfiguration == nil {
		specHttpEndpointDestConfig.RequestConfiguration = &svcapitypes.HTTPEndpointRequestConfiguration{}
	}
	if respRequestConfiguration.ContentEncoding != "" {
		specHttpEndpointDestConfig.RequestConfiguration.ContentEncoding = aws.String(string(respRequestConfiguration.ContentEncoding))
	}
	if respRequestConfiguration.CommonAttributes != nil {
		specHttpEndpointDestConfig.RequestConfiguration.CommonAttributes = make([]*svcapitypes.HTTPEndpointCommonAttribute, len(respRequestConfiguration.CommonAttributes))
		for i, attr := range respRequestConfiguration.CommonAttributes {
			specHttpEndpointDestConfig.RequestConfiguration.CommonAttributes[i] = &svcapitypes.HTTPEndpointCommonAttribute{
				AttributeName:  attr.AttributeName,
				AttributeValue: attr.AttributeValue,
			}
		}
	}
}

func readS3DestinationDescription(specHttpEndpointDestConfig *svcapitypes.HTTPEndpointDestinationConfiguration, respS3DestinationDescription *svcsdktypes.S3DestinationDescription) {
	if respS3DestinationDescription == nil {
		return
	}

	if specHttpEndpointDestConfig.S3Configuration == nil {
		specHttpEndpointDestConfig.S3Configuration = &svcapitypes.S3DestinationConfiguration{}
	}

	if specHttpEndpointDestConfig.S3Configuration == nil {
		specHttpEndpointDestConfig.S3Configuration = &svcapitypes.S3DestinationConfiguration{}
	}
	if respS3DestinationDescription.BucketARN != nil {
		specHttpEndpointDestConfig.S3Configuration.BucketARN = respS3DestinationDescription.BucketARN
	}
	if respS3DestinationDescription.BufferingHints != nil {
		if specHttpEndpointDestConfig.S3Configuration.BufferingHints == nil {
			specHttpEndpointDestConfig.S3Configuration.BufferingHints = &svcapitypes.BufferingHints{}
		}
		if respS3DestinationDescription.BufferingHints.IntervalInSeconds != nil {
			specHttpEndpointDestConfig.S3Configuration.BufferingHints.IntervalInSeconds = aws.Int64(int64(*respS3DestinationDescription.BufferingHints.IntervalInSeconds))
		}
		if respS3DestinationDescription.BufferingHints.SizeInMBs != nil {
			specHttpEndpointDestConfig.S3Configuration.BufferingHints.SizeInMBs = aws.Int64(int64(*respS3DestinationDescription.BufferingHints.SizeInMBs))
		}
	}
	if respS3DestinationDescription.CompressionFormat != "" {
		specHttpEndpointDestConfig.S3Configuration.CompressionFormat = aws.String(string(respS3DestinationDescription.CompressionFormat))
	}
	if respS3DestinationDescription.EncryptionConfiguration != nil {
		if specHttpEndpointDestConfig.S3Configuration.EncryptionConfiguration == nil {
			specHttpEndpointDestConfig.S3Configuration.EncryptionConfiguration = &svcapitypes.EncryptionConfiguration{}
		}
		if respS3DestinationDescription.EncryptionConfiguration.KMSEncryptionConfig != nil {
			if specHttpEndpointDestConfig.S3Configuration.EncryptionConfiguration.KMSEncryptionConfig == nil {
				specHttpEndpointDestConfig.S3Configuration.EncryptionConfiguration.KMSEncryptionConfig = &svcapitypes.KMSEncryptionConfig{}
			}
			if respS3DestinationDescription.EncryptionConfiguration.KMSEncryptionConfig.AWSKMSKeyARN != nil {
				specHttpEndpointDestConfig.S3Configuration.EncryptionConfiguration.KMSEncryptionConfig.AWSKMSKeyARN = respS3DestinationDescription.EncryptionConfiguration.KMSEncryptionConfig.AWSKMSKeyARN
			}
		}
		if respS3DestinationDescription.EncryptionConfiguration.NoEncryptionConfig != "" {
			specHttpEndpointDestConfig.S3Configuration.EncryptionConfiguration.NoEncryptionConfig = aws.String(string(respS3DestinationDescription.EncryptionConfiguration.NoEncryptionConfig))
		}
	}
	if respS3DestinationDescription.RoleARN != nil {
		specHttpEndpointDestConfig.S3Configuration.RoleARN = respS3DestinationDescription.RoleARN
	}
	if respS3DestinationDescription.CloudWatchLoggingOptions != nil {
		if specHttpEndpointDestConfig.S3Configuration.CloudWatchLoggingOptions == nil {
			specHttpEndpointDestConfig.S3Configuration.CloudWatchLoggingOptions = &svcapitypes.CloudWatchLoggingOptions{}
		}
		if respS3DestinationDescription.CloudWatchLoggingOptions.Enabled != nil {
			specHttpEndpointDestConfig.S3Configuration.CloudWatchLoggingOptions.Enabled = respS3DestinationDescription.CloudWatchLoggingOptions.Enabled
		}
		if respS3DestinationDescription.CloudWatchLoggingOptions.LogGroupName != nil {
			specHttpEndpointDestConfig.S3Configuration.CloudWatchLoggingOptions.LogGroupName = respS3DestinationDescription.CloudWatchLoggingOptions.LogGroupName
		}
		if respS3DestinationDescription.CloudWatchLoggingOptions.LogStreamName != nil {
			specHttpEndpointDestConfig.S3Configuration.CloudWatchLoggingOptions.LogStreamName = respS3DestinationDescription.CloudWatchLoggingOptions.LogStreamName
		}
	}
	if respS3DestinationDescription.ErrorOutputPrefix != nil {
		specHttpEndpointDestConfig.S3Configuration.ErrorOutputPrefix = respS3DestinationDescription.ErrorOutputPrefix
	}
	if respS3DestinationDescription.Prefix != nil {
		specHttpEndpointDestConfig.S3Configuration.Prefix = respS3DestinationDescription.Prefix
	}
}

func readSecretsManagerConfiguration(specHttpEndpointDestConfig *svcapitypes.HTTPEndpointDestinationConfiguration, respSecretsMgrConfig *svcsdktypes.SecretsManagerConfiguration) {
	if respSecretsMgrConfig == nil {
		return
	}
	if specHttpEndpointDestConfig.SecretsManagerConfiguration == nil {
		specHttpEndpointDestConfig.SecretsManagerConfiguration = &svcapitypes.SecretsManagerConfiguration{}
	}

	if respSecretsMgrConfig.Enabled != nil {
		specHttpEndpointDestConfig.SecretsManagerConfiguration.Enabled = respSecretsMgrConfig.Enabled
	}
	if respSecretsMgrConfig.RoleARN != nil {
		specHttpEndpointDestConfig.SecretsManagerConfiguration.RoleARN = respSecretsMgrConfig.RoleARN
	}
	if respSecretsMgrConfig.SecretARN != nil {
		specHttpEndpointDestConfig.SecretsManagerConfiguration.SecretARN = respSecretsMgrConfig.SecretARN
	}
}
