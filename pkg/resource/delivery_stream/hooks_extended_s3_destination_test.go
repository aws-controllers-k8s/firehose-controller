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
	"testing"

	ackv1alpha1 "github.com/aws-controllers-k8s/runtime/apis/core/v1alpha1"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/firehose"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"

	svcapitypes "github.com/aws-controllers-k8s/firehose-controller/apis/v1alpha1"
)

func TestReadExtendedS3DestinationDescription(t *testing.T) {
	bucketRef := &ackv1alpha1.AWSResourceReferenceWrapper{From: &ackv1alpha1.AWSResourceReference{Name: aws.String("bucket")}}
	keyRef := &ackv1alpha1.AWSResourceReferenceWrapper{From: &ackv1alpha1.AWSResourceReference{Name: aws.String("key")}}
	ko := &svcapitypes.DeliveryStream{}
	ko.Spec.ExtendedS3DestinationConfiguration = &svcapitypes.ExtendedS3DestinationConfiguration{
		BucketRef:             bucketRef,
		S3BackupConfiguration: &svcapitypes.S3DestinationConfiguration{BucketRef: bucketRef},
		EncryptionConfiguration: &svcapitypes.EncryptionConfiguration{
			KMSEncryptionConfig: &svcapitypes.KMSEncryptionConfig{AWSKMSKeyRef: keyRef},
		},
	}

	readExtendedS3DestinationDescription(ko, &svcsdktypes.ExtendedS3DestinationDescription{
		BucketARN:         aws.String("arn:aws:s3:::data"),
		RoleARN:           aws.String("arn:aws:iam::123456789012:role/firehose"),
		Prefix:            aws.String("logs/"),
		ErrorOutputPrefix: aws.String("errors/"),
		CompressionFormat: svcsdktypes.CompressionFormatGzip,
		S3BackupMode:      svcsdktypes.S3BackupModeEnabled,
		FileExtension:     aws.String(".json.gz"),
		CustomTimeZone:    aws.String("Europe/Paris"),
		BufferingHints:    &svcsdktypes.BufferingHints{IntervalInSeconds: aws.Int32(60), SizeInMBs: aws.Int32(5)},
		CloudWatchLoggingOptions: &svcsdktypes.CloudWatchLoggingOptions{
			Enabled: aws.Bool(true), LogGroupName: aws.String("/aws/firehose"), LogStreamName: aws.String("delivery"),
		},
		EncryptionConfiguration: &svcsdktypes.EncryptionConfiguration{
			KMSEncryptionConfig: &svcsdktypes.KMSEncryptionConfig{AWSKMSKeyARN: aws.String("arn:aws:kms:eu-west-1:123456789012:key/abc")},
		},
		ProcessingConfiguration: &svcsdktypes.ProcessingConfiguration{
			Enabled: aws.Bool(true),
			Processors: []svcsdktypes.Processor{{
				Type: svcsdktypes.ProcessorTypeLambda,
				Parameters: []svcsdktypes.ProcessorParameter{{
					ParameterName:  svcsdktypes.ProcessorParameterNameLambdaArn,
					ParameterValue: aws.String("arn:aws:lambda:eu-west-1:123456789012:function:f"),
				}},
			}},
		},
		S3BackupDescription: &svcsdktypes.S3DestinationDescription{
			BucketARN:         aws.String("arn:aws:s3:::backup"),
			RoleARN:           aws.String("arn:aws:iam::123456789012:role/firehose"),
			CompressionFormat: svcsdktypes.CompressionFormatUncompressed,
			BufferingHints:    &svcsdktypes.BufferingHints{IntervalInSeconds: aws.Int32(300), SizeInMBs: aws.Int32(5)},
			EncryptionConfiguration: &svcsdktypes.EncryptionConfiguration{
				NoEncryptionConfig: svcsdktypes.NoEncryptionConfigNoEncryption,
			},
		},
		DataFormatConversionConfiguration: &svcsdktypes.DataFormatConversionConfiguration{
			Enabled: aws.Bool(true),
			SchemaConfiguration: &svcsdktypes.SchemaConfiguration{
				CatalogId: aws.String("123456789012"), DatabaseName: aws.String("db"), TableName: aws.String("tbl"),
				Region: aws.String("eu-west-1"), RoleARN: aws.String("arn:aws:iam::123456789012:role/glue"), VersionId: aws.String("LATEST"),
			},
			InputFormatConfiguration: &svcsdktypes.InputFormatConfiguration{Deserializer: &svcsdktypes.Deserializer{
				OpenXJsonSerDe: &svcsdktypes.OpenXJsonSerDe{
					CaseInsensitive: aws.Bool(true), ConvertDotsInJsonKeysToUnderscores: aws.Bool(false),
					ColumnToJsonKeyMappings: map[string]string{"ts": "timestamp"},
				},
				HiveJsonSerDe: &svcsdktypes.HiveJsonSerDe{TimestampFormats: []string{"millis"}},
			}},
			OutputFormatConfiguration: &svcsdktypes.OutputFormatConfiguration{Serializer: &svcsdktypes.Serializer{
				ParquetSerDe: &svcsdktypes.ParquetSerDe{
					BlockSizeBytes: aws.Int32(268435456), Compression: svcsdktypes.ParquetCompressionSnappy,
					EnableDictionaryCompression: aws.Bool(true), MaxPaddingBytes: aws.Int32(0),
					PageSizeBytes: aws.Int32(1048576), WriterVersion: svcsdktypes.ParquetWriterVersionV1,
				},
				OrcSerDe: &svcsdktypes.OrcSerDe{
					BlockSizeBytes: aws.Int32(268435456), BloomFilterColumns: []string{"id"},
					BloomFilterFalsePositiveProbability: aws.Float64(0.05), Compression: svcsdktypes.OrcCompressionZlib,
					DictionaryKeyThreshold: aws.Float64(0.8), EnablePadding: aws.Bool(false),
					FormatVersion: svcsdktypes.OrcFormatVersionV012, PaddingTolerance: aws.Float64(0.05),
					RowIndexStride: aws.Int32(10000), StripeSizeBytes: aws.Int32(67108864),
				},
			}},
		},
		DynamicPartitioningConfiguration: &svcsdktypes.DynamicPartitioningConfiguration{
			Enabled: aws.Bool(true), RetryOptions: &svcsdktypes.RetryOptions{DurationInSeconds: aws.Int32(300)},
		},
	})

	spec := ko.Spec.ExtendedS3DestinationConfiguration
	conv := spec.DataFormatConversionConfiguration
	strings := []struct {
		name, want string
		got        *string
	}{
		{"BucketARN", "arn:aws:s3:::data", spec.BucketARN},
		{"RoleARN", "arn:aws:iam::123456789012:role/firehose", spec.RoleARN},
		{"Prefix", "logs/", spec.Prefix},
		{"ErrorOutputPrefix", "errors/", spec.ErrorOutputPrefix},
		{"CompressionFormat", "GZIP", spec.CompressionFormat},
		{"S3BackupMode", "Enabled", spec.S3BackupMode},
		{"FileExtension", ".json.gz", spec.FileExtension},
		{"CustomTimeZone", "Europe/Paris", spec.CustomTimeZone},
		{"CloudWatchLoggingOptions.LogGroupName", "/aws/firehose", spec.CloudWatchLoggingOptions.LogGroupName},
		{"EncryptionConfiguration.KMSEncryptionConfig.AWSKMSKeyARN", "arn:aws:kms:eu-west-1:123456789012:key/abc", spec.EncryptionConfiguration.KMSEncryptionConfig.AWSKMSKeyARN},
		{"ProcessingConfiguration.Processors[0].Type", "Lambda", spec.ProcessingConfiguration.Processors[0].Type},
		{"ProcessingConfiguration.Processors[0].Parameters[0].ParameterName", "LambdaArn", spec.ProcessingConfiguration.Processors[0].Parameters[0].ParameterName},
		{"S3BackupConfiguration.BucketARN", "arn:aws:s3:::backup", spec.S3BackupConfiguration.BucketARN},
		{"S3BackupConfiguration.EncryptionConfiguration.NoEncryptionConfig", "NoEncryption", spec.S3BackupConfiguration.EncryptionConfiguration.NoEncryptionConfig},
		{"SchemaConfiguration.TableName", "tbl", conv.SchemaConfiguration.TableName},
		{"OpenXJSONSerDe.ColumnToJSONKeyMappings[ts]", "timestamp", conv.InputFormatConfiguration.Deserializer.OpenXJSONSerDe.ColumnToJSONKeyMappings["ts"]},
		{"HiveJSONSerDe.TimestampFormats[0]", "millis", conv.InputFormatConfiguration.Deserializer.HiveJSONSerDe.TimestampFormats[0]},
		{"ParquetSerDe.Compression", "SNAPPY", conv.OutputFormatConfiguration.Serializer.ParquetSerDe.Compression},
		{"ParquetSerDe.WriterVersion", "V1", conv.OutputFormatConfiguration.Serializer.ParquetSerDe.WriterVersion},
		{"OrcSerDe.FormatVersion", "V0_12", conv.OutputFormatConfiguration.Serializer.OrcSerDe.FormatVersion},
		{"OrcSerDe.Compression", "ZLIB", conv.OutputFormatConfiguration.Serializer.OrcSerDe.Compression},
	}
	for _, tc := range strings {
		if got := aws.ToString(tc.got); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
	ints := []struct {
		name string
		want int64
		got  *int64
	}{
		{"BufferingHints.IntervalInSeconds", 60, spec.BufferingHints.IntervalInSeconds},
		{"BufferingHints.SizeInMBs", 5, spec.BufferingHints.SizeInMBs},
		{"S3BackupConfiguration.BufferingHints.IntervalInSeconds", 300, spec.S3BackupConfiguration.BufferingHints.IntervalInSeconds},
		{"ParquetSerDe.BlockSizeBytes", 268435456, conv.OutputFormatConfiguration.Serializer.ParquetSerDe.BlockSizeBytes},
		{"ParquetSerDe.MaxPaddingBytes", 0, conv.OutputFormatConfiguration.Serializer.ParquetSerDe.MaxPaddingBytes},
		{"OrcSerDe.RowIndexStride", 10000, conv.OutputFormatConfiguration.Serializer.OrcSerDe.RowIndexStride},
		{"DynamicPartitioningConfiguration.RetryOptions.DurationInSeconds", 300, spec.DynamicPartitioningConfiguration.RetryOptions.DurationInSeconds},
	}
	for _, tc := range ints {
		if tc.got == nil || *tc.got != tc.want {
			t.Errorf("%s: got %v, want %d", tc.name, tc.got, tc.want)
		}
	}
	if got := aws.ToFloat64(conv.OutputFormatConfiguration.Serializer.OrcSerDe.DictionaryKeyThreshold); got != 0.8 {
		t.Errorf("OrcSerDe.DictionaryKeyThreshold: got %v, want 0.8", got)
	}
	if !aws.ToBool(spec.ProcessingConfiguration.Enabled) || !aws.ToBool(conv.Enabled) || !aws.ToBool(spec.DynamicPartitioningConfiguration.Enabled) {
		t.Errorf("Enabled flags were not copied")
	}

	// The description carries no reference wrappers; the desired ones must survive the read.
	if spec.BucketRef != bucketRef {
		t.Errorf("BucketRef was dropped")
	}
	if spec.S3BackupConfiguration.BucketRef != bucketRef {
		t.Errorf("S3BackupConfiguration.BucketRef was dropped")
	}
	if spec.EncryptionConfiguration.KMSEncryptionConfig.AWSKMSKeyRef != keyRef {
		t.Errorf("AWSKMSKeyRef was dropped")
	}
}

func TestReadExtendedS3DestinationDescriptionEmptySpec(t *testing.T) {
	ko := &svcapitypes.DeliveryStream{}
	readExtendedS3DestinationDescription(ko, &svcsdktypes.ExtendedS3DestinationDescription{
		BucketARN: aws.String("arn:aws:s3:::data"),
		RoleARN:   aws.String("arn:aws:iam::123456789012:role/firehose"),
	})
	spec := ko.Spec.ExtendedS3DestinationConfiguration
	if spec == nil || aws.ToString(spec.BucketARN) != "arn:aws:s3:::data" {
		t.Fatalf("spec was not populated from an empty desired state: %+v", spec)
	}
	if spec.BufferingHints != nil || spec.ProcessingConfiguration != nil || spec.S3BackupConfiguration != nil {
		t.Errorf("absent description blocks must stay nil: %+v", spec)
	}
}

func TestSetDestinationsExtendedS3(t *testing.T) {
	ko := &svcapitypes.DeliveryStream{}
	resp := &svcsdk.DescribeDeliveryStreamOutput{
		DeliveryStreamDescription: &svcsdktypes.DeliveryStreamDescription{
			Destinations: []svcsdktypes.DestinationDescription{{
				ExtendedS3DestinationDescription: &svcsdktypes.ExtendedS3DestinationDescription{BucketARN: aws.String("arn:aws:s3:::data")},
			}},
		},
	}
	if err := setDestinations(ko, resp); err != nil {
		t.Fatal(err)
	}
	if got := aws.ToString(ko.Spec.ExtendedS3DestinationConfiguration.BucketARN); got != "arn:aws:s3:::data" {
		t.Errorf("BucketARN: got %q", got)
	}
	if ko.Spec.HTTPEndpointDestinationConfiguration != nil {
		t.Errorf("HTTP endpoint destination must stay nil")
	}
}

// Firehose fills in defaults on the backup block and on the dynamic
// partitioning retry options. After the read and the late initialization the
// desired resource must carry them, or every reconcile sees drift and calls
// UpdateDestination.
func TestExtendedS3LateInitializationLeavesNoDelta(t *testing.T) {
	desired := &resource{ko: &svcapitypes.DeliveryStream{}}
	desired.ko.Spec.ExtendedS3DestinationConfiguration = &svcapitypes.ExtendedS3DestinationConfiguration{
		BucketARN:    aws.String("arn:aws:s3:::data"),
		RoleARN:      aws.String("arn:aws:iam::123456789012:role/firehose"),
		S3BackupMode: aws.String("Enabled"),
		S3BackupConfiguration: &svcapitypes.S3DestinationConfiguration{
			BucketARN: aws.String("arn:aws:s3:::backup"),
			RoleARN:   aws.String("arn:aws:iam::123456789012:role/firehose"),
		},
		DynamicPartitioningConfiguration: &svcapitypes.DynamicPartitioningConfiguration{Enabled: aws.Bool(true)},
		ProcessingConfiguration: &svcapitypes.ProcessingConfiguration{
			Enabled: aws.Bool(true),
			Processors: []*svcapitypes.Processor{{
				Type: aws.String("MetadataExtraction"),
				Parameters: []*svcapitypes.ProcessorParameter{
					{ParameterName: aws.String("MetadataExtractionQuery"), ParameterValue: aws.String("{customer_id:.customer_id}")},
					{ParameterName: aws.String("JsonParsingEngine"), ParameterValue: aws.String("JQ-1.6")},
				},
			}},
		},
	}

	observed := &resource{ko: desired.ko.DeepCopy()}
	readExtendedS3DestinationDescription(observed.ko, &svcsdktypes.ExtendedS3DestinationDescription{
		BucketARN:                aws.String("arn:aws:s3:::data"),
		RoleARN:                  aws.String("arn:aws:iam::123456789012:role/firehose"),
		BufferingHints:           &svcsdktypes.BufferingHints{IntervalInSeconds: aws.Int32(300), SizeInMBs: aws.Int32(5)},
		CompressionFormat:        svcsdktypes.CompressionFormatUncompressed,
		EncryptionConfiguration:  &svcsdktypes.EncryptionConfiguration{NoEncryptionConfig: svcsdktypes.NoEncryptionConfigNoEncryption},
		CloudWatchLoggingOptions: &svcsdktypes.CloudWatchLoggingOptions{Enabled: aws.Bool(false)},
		S3BackupMode:             svcsdktypes.S3BackupModeEnabled,
		S3BackupDescription: &svcsdktypes.S3DestinationDescription{
			BucketARN:                aws.String("arn:aws:s3:::backup"),
			RoleARN:                  aws.String("arn:aws:iam::123456789012:role/firehose"),
			BufferingHints:           &svcsdktypes.BufferingHints{IntervalInSeconds: aws.Int32(300), SizeInMBs: aws.Int32(5)},
			CompressionFormat:        svcsdktypes.CompressionFormatUncompressed,
			EncryptionConfiguration:  &svcsdktypes.EncryptionConfiguration{NoEncryptionConfig: svcsdktypes.NoEncryptionConfigNoEncryption},
			CloudWatchLoggingOptions: &svcsdktypes.CloudWatchLoggingOptions{Enabled: aws.Bool(false)},
		},
		DynamicPartitioningConfiguration: &svcsdktypes.DynamicPartitioningConfiguration{
			Enabled:      aws.Bool(true),
			RetryOptions: &svcsdktypes.RetryOptions{DurationInSeconds: aws.Int32(300)},
		},
		ProcessingConfiguration: &svcsdktypes.ProcessingConfiguration{
			Enabled: aws.Bool(true),
			Processors: []svcsdktypes.Processor{{
				Type: svcsdktypes.ProcessorTypeMetadataExtraction,
				Parameters: []svcsdktypes.ProcessorParameter{
					{ParameterName: svcsdktypes.ProcessorParameterNameMetadataExtractionQuery, ParameterValue: aws.String("{customer_id:.customer_id}")},
					{ParameterName: svcsdktypes.ProcessorParameterNameJsonParsingEngine, ParameterValue: aws.String("JQ-1.6")},
				},
			}},
		},
	})

	if before := newResourceDelta(desired, observed); len(before.Differences) == 0 {
		t.Fatalf("expected the defaults to differ from the bare spec before late initialization")
	}

	rm := &resourceManager{}
	lateInitialized := rm.concreteResource(rm.lateInitializeFromReadOneOutput(observed, desired))
	if delta := newResourceDelta(lateInitialized, observed); len(delta.Differences) != 0 {
		t.Errorf("late initialization left %d differences: %+v", len(delta.Differences), delta.Differences)
	}
}
