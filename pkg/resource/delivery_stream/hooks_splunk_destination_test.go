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

func TestReadSplunkDestinationDescription(t *testing.T) {
	bucketRef := &ackv1alpha1.AWSResourceReferenceWrapper{From: &ackv1alpha1.AWSResourceReference{Name: aws.String("bucket")}}
	secretRef := &ackv1alpha1.AWSResourceReferenceWrapper{From: &ackv1alpha1.AWSResourceReference{Name: aws.String("secret")}}
	token := &ackv1alpha1.SecretKeyReference{Key: "token"}
	ko := &svcapitypes.DeliveryStream{}
	ko.Spec.SplunkDestinationConfiguration = &svcapitypes.SplunkDestinationConfiguration{
		HECToken:                    token,
		S3Configuration:             &svcapitypes.S3DestinationConfiguration{BucketRef: bucketRef},
		SecretsManagerConfiguration: &svcapitypes.SecretsManagerConfiguration{SecretRef: secretRef},
	}

	readSplunkDestinationDescription(ko, &svcsdktypes.SplunkDestinationDescription{
		HECEndpoint:                       aws.String("https://splunk.example.com:8088"),
		HECEndpointType:                   svcsdktypes.HECEndpointTypeRaw,
		HECAcknowledgmentTimeoutInSeconds: aws.Int32(180),
		RetryOptions:                      &svcsdktypes.SplunkRetryOptions{DurationInSeconds: aws.Int32(300)},
		S3BackupMode:                      svcsdktypes.SplunkS3BackupModeAllEvents,
		S3DestinationDescription: &svcsdktypes.S3DestinationDescription{
			BucketARN:         aws.String("arn:aws:s3:::backup"),
			RoleARN:           aws.String("arn:aws:iam::123456789012:role/firehose"),
			CompressionFormat: svcsdktypes.CompressionFormatGzip,
			BufferingHints:    &svcsdktypes.BufferingHints{IntervalInSeconds: aws.Int32(300), SizeInMBs: aws.Int32(5)},
		},
		ProcessingConfiguration: &svcsdktypes.ProcessingConfiguration{Enabled: aws.Bool(false)},
		CloudWatchLoggingOptions: &svcsdktypes.CloudWatchLoggingOptions{
			Enabled: aws.Bool(true), LogGroupName: aws.String("/aws/firehose"), LogStreamName: aws.String("splunk"),
		},
		BufferingHints: &svcsdktypes.SplunkBufferingHints{IntervalInSeconds: aws.Int32(60), SizeInMBs: aws.Int32(5)},
		SecretsManagerConfiguration: &svcsdktypes.SecretsManagerConfiguration{
			Enabled:   aws.Bool(true),
			RoleARN:   aws.String("arn:aws:iam::123456789012:role/secrets"),
			SecretARN: aws.String("arn:aws:secretsmanager:eu-west-1:123456789012:secret:hec"),
		},
	})

	spec := ko.Spec.SplunkDestinationConfiguration
	strings := []struct {
		name, want string
		got        *string
	}{
		{"HECEndpoint", "https://splunk.example.com:8088", spec.HECEndpoint},
		{"HECEndpointType", "Raw", spec.HECEndpointType},
		{"S3BackupMode", "AllEvents", spec.S3BackupMode},
		{"S3Configuration.BucketARN", "arn:aws:s3:::backup", spec.S3Configuration.BucketARN},
		{"S3Configuration.CompressionFormat", "GZIP", spec.S3Configuration.CompressionFormat},
		{"CloudWatchLoggingOptions.LogStreamName", "splunk", spec.CloudWatchLoggingOptions.LogStreamName},
		{"SecretsManagerConfiguration.RoleARN", "arn:aws:iam::123456789012:role/secrets", spec.SecretsManagerConfiguration.RoleARN},
		{"SecretsManagerConfiguration.SecretARN", "arn:aws:secretsmanager:eu-west-1:123456789012:secret:hec", spec.SecretsManagerConfiguration.SecretARN},
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
		{"HECAcknowledgmentTimeoutInSeconds", 180, spec.HECAcknowledgmentTimeoutInSeconds},
		{"RetryOptions.DurationInSeconds", 300, spec.RetryOptions.DurationInSeconds},
		{"BufferingHints.IntervalInSeconds", 60, spec.BufferingHints.IntervalInSeconds},
		{"BufferingHints.SizeInMBs", 5, spec.BufferingHints.SizeInMBs},
		{"S3Configuration.BufferingHints.IntervalInSeconds", 300, spec.S3Configuration.BufferingHints.IntervalInSeconds},
	}
	for _, tc := range ints {
		if tc.got == nil || *tc.got != tc.want {
			t.Errorf("%s: got %v, want %d", tc.name, tc.got, tc.want)
		}
	}
	if aws.ToBool(spec.ProcessingConfiguration.Enabled) || !aws.ToBool(spec.SecretsManagerConfiguration.Enabled) {
		t.Errorf("Enabled flags were not copied")
	}

	// The description carries neither the secret reference nor the resource
	// references; the desired ones must survive the read.
	if spec.HECToken != token {
		t.Errorf("HECToken secret reference was dropped")
	}
	if spec.S3Configuration.BucketRef != bucketRef {
		t.Errorf("S3Configuration.BucketRef was dropped")
	}
	if spec.SecretsManagerConfiguration.SecretRef != secretRef {
		t.Errorf("SecretsManagerConfiguration.SecretRef was dropped")
	}
}

func TestReadSplunkDestinationDescriptionEmptySpec(t *testing.T) {
	ko := &svcapitypes.DeliveryStream{}
	readSplunkDestinationDescription(ko, &svcsdktypes.SplunkDestinationDescription{
		HECEndpoint:     aws.String("https://splunk.example.com:8088"),
		HECEndpointType: svcsdktypes.HECEndpointTypeEvent,
	})
	spec := ko.Spec.SplunkDestinationConfiguration
	if spec == nil || aws.ToString(spec.HECEndpointType) != "Event" {
		t.Fatalf("spec was not populated from an empty desired state: %+v", spec)
	}
	if spec.HECToken != nil || spec.S3Configuration != nil || spec.BufferingHints != nil || spec.SecretsManagerConfiguration != nil {
		t.Errorf("absent description blocks must stay nil: %+v", spec)
	}
}

func TestReadSplunkDestinationDescriptionClearsOmittedFields(t *testing.T) {
	// A stream described without processing or Secrets Manager settings must
	// not keep the desired ones, or a patch that only adds them never reaches
	// UpdateDestination.
	secretRef := &ackv1alpha1.AWSResourceReferenceWrapper{From: &ackv1alpha1.AWSResourceReference{Name: aws.String("secret")}}
	token := &ackv1alpha1.SecretKeyReference{Key: "token"}
	ko := &svcapitypes.DeliveryStream{}
	ko.Spec.SplunkDestinationConfiguration = &svcapitypes.SplunkDestinationConfiguration{
		HECToken:                          token,
		HECAcknowledgmentTimeoutInSeconds: aws.Int64(600),
		RetryOptions:                      &svcapitypes.SplunkRetryOptions{DurationInSeconds: aws.Int64(60)},
		ProcessingConfiguration:           &svcapitypes.ProcessingConfiguration{Enabled: aws.Bool(true)},
		SecretsManagerConfiguration: &svcapitypes.SecretsManagerConfiguration{
			Enabled:   aws.Bool(true),
			SecretARN: aws.String("arn:aws:secretsmanager:eu-west-1:123456789012:secret:hec"),
			SecretRef: secretRef,
		},
	}

	readSplunkDestinationDescription(ko, &svcsdktypes.SplunkDestinationDescription{
		HECEndpoint:     aws.String("https://splunk.example.com:8088"),
		HECEndpointType: svcsdktypes.HECEndpointTypeRaw,
	})

	spec := ko.Spec.SplunkDestinationConfiguration
	if spec.HECAcknowledgmentTimeoutInSeconds != nil || spec.RetryOptions != nil {
		t.Errorf("scalar fields missing from the description must be cleared: %+v", spec)
	}
	if spec.ProcessingConfiguration != nil {
		t.Errorf("ProcessingConfiguration missing from the description must be cleared: %+v", spec.ProcessingConfiguration)
	}
	if spec.SecretsManagerConfiguration != nil {
		t.Errorf("SecretsManagerConfiguration missing from the description must be cleared: %+v", spec.SecretsManagerConfiguration)
	}
	if spec.HECToken != token {
		t.Errorf("HECToken secret reference was dropped")
	}
	if aws.ToString(spec.HECEndpoint) != "https://splunk.example.com:8088" {
		t.Errorf("HECEndpoint: got %q", aws.ToString(spec.HECEndpoint))
	}
}

func TestSetDestinationsSplunk(t *testing.T) {
	ko := &svcapitypes.DeliveryStream{}
	resp := &svcsdk.DescribeDeliveryStreamOutput{
		DeliveryStreamDescription: &svcsdktypes.DeliveryStreamDescription{
			Destinations: []svcsdktypes.DestinationDescription{{
				SplunkDestinationDescription: &svcsdktypes.SplunkDestinationDescription{HECEndpoint: aws.String("https://splunk.example.com:8088")},
			}},
		},
	}
	if err := setDestinations(ko, resp); err != nil {
		t.Fatal(err)
	}
	if got := aws.ToString(ko.Spec.SplunkDestinationConfiguration.HECEndpoint); got != "https://splunk.example.com:8088" {
		t.Errorf("HECEndpoint: got %q", got)
	}
	if ko.Spec.ExtendedS3DestinationConfiguration != nil || ko.Spec.HTTPEndpointDestinationConfiguration != nil {
		t.Errorf("other destinations must stay nil")
	}
}
