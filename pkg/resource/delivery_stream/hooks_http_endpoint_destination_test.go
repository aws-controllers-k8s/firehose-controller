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
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"

	svcapitypes "github.com/aws-controllers-k8s/firehose-controller/apis/v1alpha1"
)

func TestReadHttpDestinationDescription(t *testing.T) {
	bucketRef := &ackv1alpha1.AWSResourceReferenceWrapper{From: &ackv1alpha1.AWSResourceReference{Name: aws.String("bucket")}}
	secretRef := &ackv1alpha1.AWSResourceReferenceWrapper{From: &ackv1alpha1.AWSResourceReference{Name: aws.String("secret")}}
	accessKey := &ackv1alpha1.SecretKeyReference{Key: "key"}
	ko := &svcapitypes.DeliveryStream{}
	ko.Spec.HTTPEndpointDestinationConfiguration = &svcapitypes.HTTPEndpointDestinationConfiguration{
		EndpointConfiguration:       &svcapitypes.HTTPEndpointConfiguration{AccessKey: accessKey},
		S3Configuration:             &svcapitypes.S3DestinationConfiguration{BucketRef: bucketRef},
		SecretsManagerConfiguration: &svcapitypes.SecretsManagerConfiguration{SecretRef: secretRef},
	}

	readHttpDestinationDescription(ko, &svcsdktypes.HttpEndpointDestinationDescription{
		EndpointConfiguration: &svcsdktypes.HttpEndpointDescription{Name: aws.String("dest"), Url: aws.String("https://example.com/v1")},
		RoleARN:               aws.String("arn:aws:iam::123456789012:role/firehose"),
		S3BackupMode:          svcsdktypes.HttpEndpointS3BackupModeFailedDataOnly,
		BufferingHints:        &svcsdktypes.HttpEndpointBufferingHints{IntervalInSeconds: aws.Int32(60), SizeInMBs: aws.Int32(5)},
		RetryOptions:          &svcsdktypes.HttpEndpointRetryOptions{DurationInSeconds: aws.Int32(300)},
		RequestConfiguration: &svcsdktypes.HttpEndpointRequestConfiguration{
			ContentEncoding:  svcsdktypes.ContentEncodingGzip,
			CommonAttributes: []svcsdktypes.HttpEndpointCommonAttribute{{AttributeName: aws.String("env"), AttributeValue: aws.String("dev")}},
		},
		ProcessingConfiguration: &svcsdktypes.ProcessingConfiguration{Enabled: aws.Bool(false)},
		S3DestinationDescription: &svcsdktypes.S3DestinationDescription{
			BucketARN: aws.String("arn:aws:s3:::backup"),
			Prefix:    aws.String("backup/"),
		},
		SecretsManagerConfiguration: &svcsdktypes.SecretsManagerConfiguration{
			Enabled:   aws.Bool(true),
			SecretARN: aws.String("arn:aws:secretsmanager:eu-west-1:123456789012:secret:key"),
		},
	})

	spec := ko.Spec.HTTPEndpointDestinationConfiguration
	strings := []struct {
		name, want string
		got        *string
	}{
		{"EndpointConfiguration.Name", "dest", spec.EndpointConfiguration.Name},
		{"EndpointConfiguration.URL", "https://example.com/v1", spec.EndpointConfiguration.URL},
		{"RoleARN", "arn:aws:iam::123456789012:role/firehose", spec.RoleARN},
		{"S3BackupMode", "FailedDataOnly", spec.S3BackupMode},
		{"RequestConfiguration.ContentEncoding", "GZIP", spec.RequestConfiguration.ContentEncoding},
		{"RequestConfiguration.CommonAttributes[0].AttributeValue", "dev", spec.RequestConfiguration.CommonAttributes[0].AttributeValue},
		{"S3Configuration.BucketARN", "arn:aws:s3:::backup", spec.S3Configuration.BucketARN},
		{"S3Configuration.Prefix", "backup/", spec.S3Configuration.Prefix},
		{"SecretsManagerConfiguration.SecretARN", "arn:aws:secretsmanager:eu-west-1:123456789012:secret:key", spec.SecretsManagerConfiguration.SecretARN},
	}
	for _, tc := range strings {
		if got := aws.ToString(tc.got); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}
	if aws.ToInt64(spec.BufferingHints.IntervalInSeconds) != 60 || aws.ToInt64(spec.RetryOptions.DurationInSeconds) != 300 {
		t.Errorf("buffering or retry values were not copied: %+v %+v", spec.BufferingHints, spec.RetryOptions)
	}
	if aws.ToBool(spec.ProcessingConfiguration.Enabled) || !aws.ToBool(spec.SecretsManagerConfiguration.Enabled) {
		t.Errorf("Enabled flags were not copied")
	}
	if spec.EndpointConfiguration.AccessKey != accessKey || spec.S3Configuration.BucketRef != bucketRef || spec.SecretsManagerConfiguration.SecretRef != secretRef {
		t.Errorf("secret or resource references were dropped: %+v", spec)
	}
}

func TestReadHttpDestinationDescriptionClearsOmittedFields(t *testing.T) {
	// A stream described without processing, request, retry or Secrets
	// Manager settings, or without a backup prefix, must not keep the desired
	// ones, or a patch that only adds them never reaches UpdateDestination.
	accessKey := &ackv1alpha1.SecretKeyReference{Key: "key"}
	secretRef := &ackv1alpha1.AWSResourceReferenceWrapper{From: &ackv1alpha1.AWSResourceReference{Name: aws.String("secret")}}
	ko := &svcapitypes.DeliveryStream{}
	ko.Spec.HTTPEndpointDestinationConfiguration = &svcapitypes.HTTPEndpointDestinationConfiguration{
		EndpointConfiguration:       &svcapitypes.HTTPEndpointConfiguration{AccessKey: accessKey, URL: aws.String("https://example.com/v1")},
		ProcessingConfiguration:     &svcapitypes.ProcessingConfiguration{Enabled: aws.Bool(true)},
		RequestConfiguration:        &svcapitypes.HTTPEndpointRequestConfiguration{ContentEncoding: aws.String("GZIP")},
		RetryOptions:                &svcapitypes.HTTPEndpointRetryOptions{DurationInSeconds: aws.Int64(60)},
		S3Configuration:             &svcapitypes.S3DestinationConfiguration{Prefix: aws.String("backup/"), ErrorOutputPrefix: aws.String("errors/")},
		SecretsManagerConfiguration: &svcapitypes.SecretsManagerConfiguration{Enabled: aws.Bool(true), SecretRef: secretRef},
	}

	readHttpDestinationDescription(ko, &svcsdktypes.HttpEndpointDestinationDescription{
		EndpointConfiguration:    &svcsdktypes.HttpEndpointDescription{Url: aws.String("https://example.com/v1")},
		S3DestinationDescription: &svcsdktypes.S3DestinationDescription{BucketARN: aws.String("arn:aws:s3:::backup")},
	})

	spec := ko.Spec.HTTPEndpointDestinationConfiguration
	if spec.ProcessingConfiguration != nil || spec.RequestConfiguration != nil || spec.RetryOptions != nil || spec.SecretsManagerConfiguration != nil {
		t.Errorf("blocks missing from the description must be cleared: %+v", spec)
	}
	if spec.S3Configuration.Prefix != nil || spec.S3Configuration.ErrorOutputPrefix != nil {
		t.Errorf("backup prefixes missing from the description must be cleared: %+v", spec.S3Configuration)
	}
	if spec.EndpointConfiguration.AccessKey != accessKey {
		t.Errorf("the access key secret reference was dropped")
	}
}
