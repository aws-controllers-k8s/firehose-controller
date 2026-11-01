package tags

import (
	"context"
	"maps"
	"slices"

	svcapitypes "github.com/aws-controllers-k8s/firehose-controller/apis/v1alpha1"
	ackcompare "github.com/aws-controllers-k8s/runtime/pkg/compare"
	ackrtlog "github.com/aws-controllers-k8s/runtime/pkg/runtime/log"
	acktags "github.com/aws-controllers-k8s/runtime/pkg/tags"
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

	from := toACKTags(latestTags)
	to := toACKTags(desiredTags)
	added, _, removed := ackcompare.GetTagsDifference(from, to)

	if len(added) == 0 && len(removed) == 0 {
		return nil
	}

	if len(removed) > 0 {
		_, err = client.UntagDeliveryStream(
			ctx,
			&svcsdk.UntagDeliveryStreamInput{
				DeliveryStreamName: aws.String(deliveryStreamName),
				TagKeys:            slices.Sorted(maps.Keys(removed)),
			},
		)
		mr.RecordAPICall("UPDATE", "UntagDeliveryStream", err)
		if err != nil {
			return err
		}
	}

	if len(added) > 0 {
		addedOrUpdated := make([]svcsdktypes.Tag, 0, len(added))
		for _, k := range slices.Sorted(maps.Keys(added)) {
			addedOrUpdated = append(addedOrUpdated, svcsdktypes.Tag{
				Key:   aws.String(k),
				Value: aws.String(added[k]),
			})
		}
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

// toACKTags converts a list of Tags into the map representation used by the
// ACK runtime tag comparison. Nil tags or tags without a key are skipped, and
// a nil value is represented as an empty string.
func toACKTags(tags []*svcapitypes.Tag) acktags.Tags {
	m := make(acktags.Tags)
	for _, t := range tags {
		if t == nil || t.Key == nil {
			continue
		}
		v := ""
		if t.Value != nil {
			v = *t.Value
		}
		m[*t.Key] = v
	}
	return m
}
