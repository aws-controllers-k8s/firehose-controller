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
	"context"
	"fmt"
	"time"

	svcapitypes "github.com/aws-controllers-k8s/firehose-controller/apis/v1alpha1"
	"github.com/aws-controllers-k8s/firehose-controller/pkg/resource/tags"
	ackrequeue "github.com/aws-controllers-k8s/runtime/pkg/requeue"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/firehose"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
)

var (
	ErrDeliveryStreamCreating = fmt.Errorf(
		"delivery stream in %v state, cannot be modified",
		svcsdktypes.DeliveryStreamStatusCreating,
	)
	ErrDeliveryStreamEncryptionEnabling = fmt.Errorf(
		"delivery stream cannot be modified while server-side encryption %v",
		svcsdktypes.DeliveryStreamEncryptionStatusEnabling,
	)
	ErrDeliveryStreamEncryptionDisabling = fmt.Errorf(
		"delivery stream cannot be modified while server-side encryption %v",
		svcsdktypes.DeliveryStreamEncryptionStatusDisabling,
	)
)

var (
	requeueWhileCreating = ackrequeue.NeededAfter(
		ErrDeliveryStreamCreating,
		5*time.Second,
	)
	requeueWhileEncryptionEnabling = ackrequeue.NeededAfter(
		ErrDeliveryStreamEncryptionEnabling,
		5*time.Second,
	)
	requeueWhileEncryptionDisabling = ackrequeue.NeededAfter(
		ErrDeliveryStreamEncryptionDisabling,
		5*time.Second,
	)
)

// getTags retrieves the resource's associated tags.
func (rm *resourceManager) getTags(
	ctx context.Context,
	resourceARN string,
) ([]*svcapitypes.Tag, error) {
	return tags.GetResourceTags(ctx, rm.sdkapi, rm.metrics, resourceARN)
}

// syncTags keeps the resource's tags in sync.
func (rm *resourceManager) syncTags(
	ctx context.Context,
	desired *resource,
	latest *resource,
) (err error) {
	return tags.SyncResourceTags(
		ctx,
		rm.sdkapi,
		rm.metrics,
		string(*latest.ko.Spec.DeliveryStreamName),
		desired.ko.Spec.Tags,
		latest.ko.Spec.Tags,
	)
}

// deliveryStreamEncryptionDisabled checks whether or not server-side encryption is disabled or not.
func deliveryStreamEncryptionDisabled(r *resource) bool {
	return r.ko.Spec.DeliveryStreamEncryptionConfiguration == nil ||
		(r.ko.Spec.DeliveryStreamEncryptionConfiguration.KeyARN == nil && r.ko.Spec.DeliveryStreamEncryptionConfiguration.KeyType == nil)
}

// isDeliveryStreamCreating checks whether or not the delivery stream is in the creating state.
func isDeliveryStreamCreating(r *resource) bool {
	return r.ko.Status.DeliveryStreamStatus != nil &&
		*r.ko.Status.DeliveryStreamStatus == *aws.String(string(svcsdktypes.DeliveryStreamStatusCreating))
}

// isDeliveryStreamEncryptionEnabling checks whether or not the delivery stream's server-side encryption is enabling
func isDeliveryStreamEncryptionEnabling(r *resource) bool {
	return r.ko.Status.DeliveryStreamEncryptionConfigurationStatus != nil &&
		*r.ko.Status.DeliveryStreamEncryptionConfigurationStatus == *aws.String(string(svcsdktypes.DeliveryStreamEncryptionStatusEnabling))
}

// isDeliveryStreamEncryptionDisabling checks whether or not the delivery stream's server-side encryption is enabling
func isDeliveryStreamEncryptionDisabling(r *resource) bool {
	return r.ko.Status.DeliveryStreamEncryptionConfigurationStatus != nil &&
		*r.ko.Status.DeliveryStreamEncryptionConfigurationStatus == *aws.String(string(svcsdktypes.DeliveryStreamEncryptionStatusDisabling))
}

type metricsRecorder interface {
	RecordAPICall(string, string, error)
}

type deliveryStreamEncryptionClient interface {
	StartDeliveryStreamEncryption(ctx context.Context, params *svcsdk.StartDeliveryStreamEncryptionInput, optFns ...func(*svcsdk.Options)) (*svcsdk.StartDeliveryStreamEncryptionOutput, error)
	StopDeliveryStreamEncryption(ctx context.Context, params *svcsdk.StopDeliveryStreamEncryptionInput, optFns ...func(*svcsdk.Options)) (*svcsdk.StopDeliveryStreamEncryptionOutput, error)
}

// updateDeliveryStreamEncryptionConfiguration checks the state of DeliveryStreamEncryptionConfiguration. If any
// child fields are set will call StartDeliveryStreamEncryption to enable server-side encryption with the specified values.
// If no child fields are set or DeliveryStreamEncryptionConfiguration is nil will call StopDeliveryStreamEncryption
// to disable server-side encryption. If the operation does not return an API error a requeue error will be returned
// as other updated will need to wait until after the encryption status has updated.
func updateDeliveryStreamEncryptionConfiguration(
	ctx context.Context,
	desired *resource,
	client deliveryStreamEncryptionClient,
	metrics metricsRecorder,
) (err error) {
	if desired.ko.Spec.DeliveryStreamEncryptionConfiguration != nil && desired.ko.Spec.DeliveryStreamEncryptionConfiguration.KeyType != nil {
		startDeliveryStreamEncryptionInput := svcsdk.StartDeliveryStreamEncryptionInput{}
		startDeliveryStreamEncryptionInput.DeliveryStreamEncryptionConfigurationInput = &svcsdktypes.DeliveryStreamEncryptionConfigurationInput{}
		if desired.ko.Spec.DeliveryStreamName != nil {
			startDeliveryStreamEncryptionInput.DeliveryStreamName = desired.ko.Spec.DeliveryStreamName
		}
		if desired.ko.Spec.DeliveryStreamEncryptionConfiguration.KeyType != nil {
			startDeliveryStreamEncryptionInput.DeliveryStreamEncryptionConfigurationInput.KeyType = svcsdktypes.KeyType(*desired.ko.Spec.DeliveryStreamEncryptionConfiguration.KeyType)
		}
		if desired.ko.Spec.DeliveryStreamEncryptionConfiguration.KeyARN != nil {
			startDeliveryStreamEncryptionInput.DeliveryStreamEncryptionConfigurationInput.KeyARN = desired.ko.Spec.DeliveryStreamEncryptionConfiguration.KeyARN
		}

		_, err = client.StartDeliveryStreamEncryption(ctx, &startDeliveryStreamEncryptionInput)
		metrics.RecordAPICall("UPDATE", "StartDeliveryStreamEncryption", err)
		if err != nil {
			return err
		}
	} else {
		stopDeliveryStreamInput := svcsdk.StopDeliveryStreamEncryptionInput{}
		if desired.ko.Spec.DeliveryStreamName != nil {
			stopDeliveryStreamInput.DeliveryStreamName = desired.ko.Spec.DeliveryStreamName
		}
		_, err = client.StopDeliveryStreamEncryption(ctx, &stopDeliveryStreamInput)
		metrics.RecordAPICall("UPDATE", "StopDeliveryStreamEncryption", err)
		if err != nil {
			return err
		}
	}

	return ackrequeue.Needed(fmt.Errorf("requeue after updating delivery stream encryption"))
}

// setDestinations copies the populated destination entry to the relevant ko Spec and Status fields.
// This is needed because DescribeDeliveryStream returns the destination description as an array with only one
// entry.
func setDestinations(ko *svcapitypes.DeliveryStream, resp *svcsdk.DescribeDeliveryStreamOutput) error {
	if len(resp.DeliveryStreamDescription.Destinations) == 0 {
		return nil
	}

	// From the Firehose Delivery Stream docs only one destination is set.
	respDestination := resp.DeliveryStreamDescription.Destinations[0]
	switch {
	case respDestination.HttpEndpointDestinationDescription != nil:
		readHttpDestinationDescription(ko, respDestination.HttpEndpointDestinationDescription)
	}

	return nil
}
