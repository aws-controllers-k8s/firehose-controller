package tags

import (
	"context"
	"reflect"
	"testing"

	svcapitypes "github.com/aws-controllers-k8s/firehose-controller/apis/v1alpha1"
	svcsdk "github.com/aws/aws-sdk-go-v2/service/firehose"
)

type fakeTagsClient struct {
	tagInputs   []*svcsdk.TagDeliveryStreamInput
	untagInputs []*svcsdk.UntagDeliveryStreamInput
}

func (f *fakeTagsClient) TagDeliveryStream(_ context.Context, in *svcsdk.TagDeliveryStreamInput, _ ...func(*svcsdk.Options)) (*svcsdk.TagDeliveryStreamOutput, error) {
	f.tagInputs = append(f.tagInputs, in)
	return &svcsdk.TagDeliveryStreamOutput{}, nil
}

func (f *fakeTagsClient) ListTagsForDeliveryStream(_ context.Context, _ *svcsdk.ListTagsForDeliveryStreamInput, _ ...func(*svcsdk.Options)) (*svcsdk.ListTagsForDeliveryStreamOutput, error) {
	return &svcsdk.ListTagsForDeliveryStreamOutput{}, nil
}

func (f *fakeTagsClient) UntagDeliveryStream(_ context.Context, in *svcsdk.UntagDeliveryStreamInput, _ ...func(*svcsdk.Options)) (*svcsdk.UntagDeliveryStreamOutput, error) {
	f.untagInputs = append(f.untagInputs, in)
	return &svcsdk.UntagDeliveryStreamOutput{}, nil
}

type fakeMetrics struct{}

func (fakeMetrics) RecordAPICall(string, string, error) {}

func newTag(key, value string) *svcapitypes.Tag {
	return &svcapitypes.Tag{Key: &key, Value: &value}
}

func TestSyncResourceTags_NoOp(t *testing.T) {
	client := &fakeTagsClient{}
	latest := []*svcapitypes.Tag{newTag("a", "1"), newTag("b", "2")}
	desired := []*svcapitypes.Tag{newTag("b", "2"), newTag("a", "1")}

	if err := SyncResourceTags(context.Background(), client, fakeMetrics{}, "stream", desired, latest); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(client.tagInputs) != 0 || len(client.untagInputs) != 0 {
		t.Errorf("expected no API calls, got tag=%d untag=%d", len(client.tagInputs), len(client.untagInputs))
	}
}

func TestSyncResourceTags_Add(t *testing.T) {
	client := &fakeTagsClient{}
	desired := []*svcapitypes.Tag{newTag("a", "1")}

	if err := SyncResourceTags(context.Background(), client, fakeMetrics{}, "stream", desired, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(client.untagInputs) != 0 {
		t.Errorf("expected no UntagDeliveryStream call, got %d", len(client.untagInputs))
	}
	if len(client.tagInputs) != 1 {
		t.Fatalf("expected 1 TagDeliveryStream call, got %d", len(client.tagInputs))
	}
	got := client.tagInputs[0].Tags
	if len(got) != 1 || *got[0].Key != "a" || *got[0].Value != "1" {
		t.Errorf("unexpected tags payload: %+v", got)
	}
}

func TestSyncResourceTags_Remove(t *testing.T) {
	client := &fakeTagsClient{}
	latest := []*svcapitypes.Tag{newTag("a", "1"), newTag("b", "2")}

	if err := SyncResourceTags(context.Background(), client, fakeMetrics{}, "stream", nil, latest); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(client.tagInputs) != 0 {
		t.Errorf("expected no TagDeliveryStream call, got %d", len(client.tagInputs))
	}
	if len(client.untagInputs) != 1 {
		t.Fatalf("expected 1 UntagDeliveryStream call, got %d", len(client.untagInputs))
	}
	if want := []string{"a", "b"}; !reflect.DeepEqual(client.untagInputs[0].TagKeys, want) {
		t.Errorf("TagKeys = %v, want %v", client.untagInputs[0].TagKeys, want)
	}
}

func TestSyncResourceTags_ValueChangeAddsOnly(t *testing.T) {
	client := &fakeTagsClient{}
	latest := []*svcapitypes.Tag{newTag("a", "1")}
	desired := []*svcapitypes.Tag{newTag("a", "2")}

	if err := SyncResourceTags(context.Background(), client, fakeMetrics{}, "stream", desired, latest); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(client.untagInputs) != 0 {
		t.Errorf("expected no UntagDeliveryStream call on value change, got %d", len(client.untagInputs))
	}
	if len(client.tagInputs) != 1 {
		t.Fatalf("expected 1 TagDeliveryStream call, got %d", len(client.tagInputs))
	}
	got := client.tagInputs[0].Tags
	if len(got) != 1 || *got[0].Key != "a" || *got[0].Value != "2" {
		t.Errorf("unexpected tags payload: %+v", got)
	}
}

func TestSyncResourceTags_AddAndRemove(t *testing.T) {
	client := &fakeTagsClient{}
	latest := []*svcapitypes.Tag{newTag("a", "1"), newTag("b", "2")}
	desired := []*svcapitypes.Tag{newTag("a", "1"), newTag("c", "3")}

	if err := SyncResourceTags(context.Background(), client, fakeMetrics{}, "stream", desired, latest); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(client.untagInputs) != 1 {
		t.Fatalf("expected 1 UntagDeliveryStream call, got %d", len(client.untagInputs))
	}
	if want := []string{"b"}; !reflect.DeepEqual(client.untagInputs[0].TagKeys, want) {
		t.Errorf("UntagDeliveryStream TagKeys = %v, want %v", client.untagInputs[0].TagKeys, want)
	}
	if len(client.tagInputs) != 1 {
		t.Fatalf("expected 1 TagDeliveryStream call, got %d", len(client.tagInputs))
	}
	got := client.tagInputs[0].Tags
	if len(got) != 1 || *got[0].Key != "c" || *got[0].Value != "3" {
		t.Errorf("TagDeliveryStream tags = %+v, want [{c:3}]", got)
	}
}

func TestSyncResourceTags_NilTagsDoNotPanic(t *testing.T) {
	client := &fakeTagsClient{}
	latest := []*svcapitypes.Tag{nil, {Key: nil, Value: nil}, newTag("a", "1")}
	desired := []*svcapitypes.Tag{nil, {Key: nil, Value: nil}, newTag("a", "1")}

	if err := SyncResourceTags(context.Background(), client, fakeMetrics{}, "stream", desired, latest); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(client.tagInputs) != 0 || len(client.untagInputs) != 0 {
		t.Errorf("expected no API calls, got tag=%d untag=%d", len(client.tagInputs), len(client.untagInputs))
	}
}
