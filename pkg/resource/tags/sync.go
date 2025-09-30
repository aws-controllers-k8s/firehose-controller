package tags

import (
	"context"

	svcapitypes "github.com/aws-controllers-k8s/firehose-controller/apis/v1alpha1"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	"github.com/aws/aws-sdk-go-v2/aws"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/firehose"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
)

type metricsRecorder interface {
	RecordAPICall(opType string, opID string, err error)
}

type tagsClient interface {
	TagDeliveryStream(context.Context, *svcsdk.TagDeliveryStreamInput, ...func(*svcsdk.Options)) (*svcsdk.TagDeliveryStreamOutput, error)
	ListTagsForDeliveryStream(context.Context, *svcsdk.ListTagsForDeliveryStreamInput, ...func(*svcsdk.Options)) (*svcsdk.ListTagsForDeliveryStreamOutput, error)
	UntagDeliveryStream(context.Context, *svcsdk.UntagDeliveryStreamInput, ...func(*svcsdk.Options)) (*svcsdk.UntagDeliveryStreamOutput, error)
}

// GetResourceTags retrieves a resource list of tags.
func GetResourceTags(
	ctx context.Context,
	client tagsClient,
	mr metricsRecorder,
	deliveryStreamName string,
) ([]*svcapitypes.Tag, error) {
	listTagsForResourceResponse, err := client.ListTagsForDeliveryStream(
		ctx,
		&svcsdk.ListTagsForDeliveryStreamInput{
			DeliveryStreamName: &deliveryStreamName,
		},
	)
	mr.RecordAPICall("GET", "ListTagsForResource", err)
	if err != nil {
		return nil, err
	}

	tags := make([]*svcapitypes.Tag, 0)
	for _, tag := range listTagsForResourceResponse.Tags {
		tags = append(tags, &svcapitypes.Tag{
			Key:   tag.Key,
			Value: tag.Value,
		})
	}

	return tags, nil
}

// SyncResourceTags uses TagDeliveryStream and UntagDeliveryStream API Calls to add, remove
// and update resource tags.
func SyncResourceTags(
	ctx context.Context,
	client tagsClient,
	mr metricsRecorder,
	deliveryStreamName string,
	desiredTags []*svcapitypes.Tag,
	latestTags []*svcapitypes.Tag,
) error {
	var err error
	rlog := ackrtlog.FromContext(ctx)
	exit := rlog.Trace("common.SyncResourceTags")
	defer func() {
		exit(err)
	}()

	addedOrUpdated, removed := computeTagsDelta(desiredTags, latestTags)

	if len(removed) > 0 {
		_, err = client.UntagDeliveryStream(
			ctx,
			&svcsdk.UntagDeliveryStreamInput{
				DeliveryStreamName: aws.String(deliveryStreamName),
				TagKeys:            removed,
			},
		)
		mr.RecordAPICall("UPDATE", "UntagDeliveryStream", err)
		if err != nil {
			return err
		}
	}

	if len(addedOrUpdated) > 0 {
		_, err = client.TagDeliveryStream(
			ctx,
			&svcsdk.TagDeliveryStreamInput{
				DeliveryStreamName: aws.String(deliveryStreamName),
				Tags:               addedOrUpdated,
			},
		)
		mr.RecordAPICall("UPDATE", "TagDeliveryStream", err)
		if err != nil {
			return err
		}
	}
	return nil
}

// computeTagsDelta compares two Tag arrays and return two different list
// containing the addedOrupdated and removed tags. The removed tags array
// only contains the tags Keys.
func computeTagsDelta(
	a []*svcapitypes.Tag,
	b []*svcapitypes.Tag,
) (addedOrUpdated []svcsdktypes.Tag, removed []string) {

	// Find the keys in the Spec have either been added or updated.
	addedOrUpdated = make([]svcsdktypes.Tag, 0)
	for _, aTag := range a {
		found := false
		for _, bTag := range b {
			if *aTag.Key == *bTag.Key {
				if *aTag.Value == *bTag.Value {
					found = true
				}

				break
			}
		}
		if !found {
			addedOrUpdated = append(addedOrUpdated, svcsdktypes.Tag{
				Key:   aTag.Key,
				Value: aTag.Value,
			})
		}
	}

	for _, bTag := range b {
		found := false
		for _, aTag := range a {
			if *aTag.Key == *bTag.Key {
				found = true
				break
			}
		}

		if !found {
			removed = append(removed, *bTag.Key)
		}
	}

	return addedOrUpdated, removed
}

// equalTags returns true if two Tag arrays are equal regardless of the order
// of their elements.
func EqualTags(
	a []*svcapitypes.Tag,
	b []*svcapitypes.Tag,
) bool {
	addedOrUpdated, removed := computeTagsDelta(a, b)
	return len(addedOrUpdated) == 0 && len(removed) == 0
}
