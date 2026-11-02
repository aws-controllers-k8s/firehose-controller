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

// readExtendedS3DestinationDescription writes the described destination onto
// the spec, nil included: Firehose leaves unset fields out of the description,
// and keeping the desired value there hides a spec change from the delta. The
// bucket, role and KMS key reference wrappers are never described and stay.
func readExtendedS3DestinationDescription(ko *svcapitypes.DeliveryStream, resp *svcsdktypes.ExtendedS3DestinationDescription) {
	if resp == nil {
		return
	}

	if ko.Spec.ExtendedS3DestinationConfiguration == nil {
		ko.Spec.ExtendedS3DestinationConfiguration = &svcapitypes.ExtendedS3DestinationConfiguration{}
	}
	spec := ko.Spec.ExtendedS3DestinationConfiguration

	spec.BucketARN = resp.BucketARN
	spec.RoleARN = resp.RoleARN
	spec.Prefix = resp.Prefix
	spec.ErrorOutputPrefix = resp.ErrorOutputPrefix
	spec.CompressionFormat = nil
	if resp.CompressionFormat != "" {
		spec.CompressionFormat = aws.String(string(resp.CompressionFormat))
	}
	spec.S3BackupMode = nil
	if resp.S3BackupMode != "" {
		spec.S3BackupMode = aws.String(string(resp.S3BackupMode))
	}
	spec.FileExtension = resp.FileExtension
	spec.CustomTimeZone = resp.CustomTimeZone
	spec.BufferingHints = nil
	if resp.BufferingHints != nil {
		spec.BufferingHints = bufferingHintsFromSDK(resp.BufferingHints)
	}
	spec.CloudWatchLoggingOptions = nil
	if resp.CloudWatchLoggingOptions != nil {
		spec.CloudWatchLoggingOptions = cloudWatchLoggingOptionsFromSDK(resp.CloudWatchLoggingOptions)
	}
	if resp.EncryptionConfiguration == nil {
		spec.EncryptionConfiguration = nil
	} else {
		if spec.EncryptionConfiguration == nil {
			spec.EncryptionConfiguration = &svcapitypes.EncryptionConfiguration{}
		}
		mergeEncryptionConfiguration(spec.EncryptionConfiguration, resp.EncryptionConfiguration)
	}
	spec.ProcessingConfiguration = nil
	if resp.ProcessingConfiguration != nil {
		spec.ProcessingConfiguration = processingConfigurationFromSDK(resp.ProcessingConfiguration)
	}
	if resp.S3BackupDescription == nil {
		spec.S3BackupConfiguration = nil
	} else {
		if spec.S3BackupConfiguration == nil {
			spec.S3BackupConfiguration = &svcapitypes.S3DestinationConfiguration{}
		}
		mergeS3DestinationDescription(spec.S3BackupConfiguration, resp.S3BackupDescription)
	}
	spec.DataFormatConversionConfiguration = nil
	if resp.DataFormatConversionConfiguration != nil {
		spec.DataFormatConversionConfiguration = dataFormatConversionConfigurationFromSDK(resp.DataFormatConversionConfiguration)
	}
	spec.DynamicPartitioningConfiguration = nil
	if resp.DynamicPartitioningConfiguration != nil {
		spec.DynamicPartitioningConfiguration = dynamicPartitioningConfigurationFromSDK(resp.DynamicPartitioningConfiguration)
	}
}

func bufferingHintsFromSDK(resp *svcsdktypes.BufferingHints) *svcapitypes.BufferingHints {
	hints := &svcapitypes.BufferingHints{}
	if resp.IntervalInSeconds != nil {
		hints.IntervalInSeconds = aws.Int64(int64(*resp.IntervalInSeconds))
	}
	if resp.SizeInMBs != nil {
		hints.SizeInMBs = aws.Int64(int64(*resp.SizeInMBs))
	}
	return hints
}

func cloudWatchLoggingOptionsFromSDK(resp *svcsdktypes.CloudWatchLoggingOptions) *svcapitypes.CloudWatchLoggingOptions {
	return &svcapitypes.CloudWatchLoggingOptions{
		Enabled:       resp.Enabled,
		LogGroupName:  resp.LogGroupName,
		LogStreamName: resp.LogStreamName,
	}
}

// mergeEncryptionConfiguration writes the described encryption settings onto
// spec, nil included, and keeps the KMS key reference wrapper.
func mergeEncryptionConfiguration(spec *svcapitypes.EncryptionConfiguration, resp *svcsdktypes.EncryptionConfiguration) {
	spec.NoEncryptionConfig = nil
	if resp.NoEncryptionConfig != "" {
		spec.NoEncryptionConfig = aws.String(string(resp.NoEncryptionConfig))
	}
	if resp.KMSEncryptionConfig == nil {
		spec.KMSEncryptionConfig = nil
		return
	}
	if spec.KMSEncryptionConfig == nil {
		spec.KMSEncryptionConfig = &svcapitypes.KMSEncryptionConfig{}
	}
	spec.KMSEncryptionConfig.AWSKMSKeyARN = resp.KMSEncryptionConfig.AWSKMSKeyARN
}

func processingConfigurationFromSDK(resp *svcsdktypes.ProcessingConfiguration) *svcapitypes.ProcessingConfiguration {
	config := &svcapitypes.ProcessingConfiguration{Enabled: resp.Enabled}
	if resp.Processors != nil {
		config.Processors = make([]*svcapitypes.Processor, len(resp.Processors))
		for i, proc := range resp.Processors {
			processor := &svcapitypes.Processor{}
			if proc.Type != "" {
				processor.Type = aws.String(string(proc.Type))
			}
			if proc.Parameters != nil {
				processor.Parameters = make([]*svcapitypes.ProcessorParameter, len(proc.Parameters))
				for j, param := range proc.Parameters {
					processor.Parameters[j] = &svcapitypes.ProcessorParameter{
						ParameterName:  aws.String(string(param.ParameterName)),
						ParameterValue: param.ParameterValue,
					}
				}
			}
			config.Processors[i] = processor
		}
	}
	return config
}

// mergeS3DestinationDescription writes the described S3 destination onto
// spec, nil included for the fields Firehose leaves out when unset, and keeps
// the bucket, role and KMS key reference wrappers.
func mergeS3DestinationDescription(spec *svcapitypes.S3DestinationConfiguration, resp *svcsdktypes.S3DestinationDescription) {
	spec.BucketARN = resp.BucketARN
	spec.RoleARN = resp.RoleARN
	spec.Prefix = resp.Prefix
	spec.ErrorOutputPrefix = resp.ErrorOutputPrefix
	spec.CompressionFormat = nil
	if resp.CompressionFormat != "" {
		spec.CompressionFormat = aws.String(string(resp.CompressionFormat))
	}
	spec.BufferingHints = nil
	if resp.BufferingHints != nil {
		spec.BufferingHints = bufferingHintsFromSDK(resp.BufferingHints)
	}
	spec.CloudWatchLoggingOptions = nil
	if resp.CloudWatchLoggingOptions != nil {
		spec.CloudWatchLoggingOptions = cloudWatchLoggingOptionsFromSDK(resp.CloudWatchLoggingOptions)
	}
	if resp.EncryptionConfiguration == nil {
		spec.EncryptionConfiguration = nil
		return
	}
	if spec.EncryptionConfiguration == nil {
		spec.EncryptionConfiguration = &svcapitypes.EncryptionConfiguration{}
	}
	mergeEncryptionConfiguration(spec.EncryptionConfiguration, resp.EncryptionConfiguration)
}

func dataFormatConversionConfigurationFromSDK(resp *svcsdktypes.DataFormatConversionConfiguration) *svcapitypes.DataFormatConversionConfiguration {
	config := &svcapitypes.DataFormatConversionConfiguration{Enabled: resp.Enabled}
	if resp.SchemaConfiguration != nil {
		config.SchemaConfiguration = &svcapitypes.SchemaConfiguration{
			CatalogID:    resp.SchemaConfiguration.CatalogId,
			DatabaseName: resp.SchemaConfiguration.DatabaseName,
			Region:       resp.SchemaConfiguration.Region,
			RoleARN:      resp.SchemaConfiguration.RoleARN,
			TableName:    resp.SchemaConfiguration.TableName,
			VersionID:    resp.SchemaConfiguration.VersionId,
		}
	}
	if resp.InputFormatConfiguration != nil {
		config.InputFormatConfiguration = &svcapitypes.InputFormatConfiguration{}
		if resp.InputFormatConfiguration.Deserializer != nil {
			deserializer := &svcapitypes.Deserializer{}
			if hive := resp.InputFormatConfiguration.Deserializer.HiveJsonSerDe; hive != nil {
				deserializer.HiveJSONSerDe = &svcapitypes.HiveJSONSerDe{}
				if hive.TimestampFormats != nil {
					deserializer.HiveJSONSerDe.TimestampFormats = aws.StringSlice(hive.TimestampFormats)
				}
			}
			if openX := resp.InputFormatConfiguration.Deserializer.OpenXJsonSerDe; openX != nil {
				deserializer.OpenXJSONSerDe = &svcapitypes.OpenXJSONSerDe{
					CaseInsensitive:                    openX.CaseInsensitive,
					ConvertDotsInJSONKeysToUnderscores: openX.ConvertDotsInJsonKeysToUnderscores,
				}
				if openX.ColumnToJsonKeyMappings != nil {
					deserializer.OpenXJSONSerDe.ColumnToJSONKeyMappings = aws.StringMap(openX.ColumnToJsonKeyMappings)
				}
			}
			config.InputFormatConfiguration.Deserializer = deserializer
		}
	}
	if resp.OutputFormatConfiguration != nil {
		config.OutputFormatConfiguration = &svcapitypes.OutputFormatConfiguration{}
		if resp.OutputFormatConfiguration.Serializer != nil {
			serializer := &svcapitypes.Serializer{}
			if orc := resp.OutputFormatConfiguration.Serializer.OrcSerDe; orc != nil {
				serializer.OrcSerDe = &svcapitypes.OrcSerDe{
					BloomFilterFalsePositiveProbability: orc.BloomFilterFalsePositiveProbability,
					DictionaryKeyThreshold:              orc.DictionaryKeyThreshold,
					EnablePadding:                       orc.EnablePadding,
					PaddingTolerance:                    orc.PaddingTolerance,
				}
				if orc.BlockSizeBytes != nil {
					serializer.OrcSerDe.BlockSizeBytes = aws.Int64(int64(*orc.BlockSizeBytes))
				}
				if orc.BloomFilterColumns != nil {
					serializer.OrcSerDe.BloomFilterColumns = aws.StringSlice(orc.BloomFilterColumns)
				}
				if orc.Compression != "" {
					serializer.OrcSerDe.Compression = aws.String(string(orc.Compression))
				}
				if orc.FormatVersion != "" {
					serializer.OrcSerDe.FormatVersion = aws.String(string(orc.FormatVersion))
				}
				if orc.RowIndexStride != nil {
					serializer.OrcSerDe.RowIndexStride = aws.Int64(int64(*orc.RowIndexStride))
				}
				if orc.StripeSizeBytes != nil {
					serializer.OrcSerDe.StripeSizeBytes = aws.Int64(int64(*orc.StripeSizeBytes))
				}
			}
			if parquet := resp.OutputFormatConfiguration.Serializer.ParquetSerDe; parquet != nil {
				serializer.ParquetSerDe = &svcapitypes.ParquetSerDe{
					EnableDictionaryCompression: parquet.EnableDictionaryCompression,
				}
				if parquet.BlockSizeBytes != nil {
					serializer.ParquetSerDe.BlockSizeBytes = aws.Int64(int64(*parquet.BlockSizeBytes))
				}
				if parquet.Compression != "" {
					serializer.ParquetSerDe.Compression = aws.String(string(parquet.Compression))
				}
				if parquet.MaxPaddingBytes != nil {
					serializer.ParquetSerDe.MaxPaddingBytes = aws.Int64(int64(*parquet.MaxPaddingBytes))
				}
				if parquet.PageSizeBytes != nil {
					serializer.ParquetSerDe.PageSizeBytes = aws.Int64(int64(*parquet.PageSizeBytes))
				}
				if parquet.WriterVersion != "" {
					serializer.ParquetSerDe.WriterVersion = aws.String(string(parquet.WriterVersion))
				}
			}
			config.OutputFormatConfiguration.Serializer = serializer
		}
	}
	return config
}

func dynamicPartitioningConfigurationFromSDK(resp *svcsdktypes.DynamicPartitioningConfiguration) *svcapitypes.DynamicPartitioningConfiguration {
	config := &svcapitypes.DynamicPartitioningConfiguration{Enabled: resp.Enabled}
	if resp.RetryOptions != nil {
		config.RetryOptions = &svcapitypes.RetryOptions{}
		if resp.RetryOptions.DurationInSeconds != nil {
			config.RetryOptions.DurationInSeconds = aws.Int64(int64(*resp.RetryOptions.DurationInSeconds))
		}
	}
	return config
}
