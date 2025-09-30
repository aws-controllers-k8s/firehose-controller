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
	"github.com/aws/aws-sdk-go-v2/service/firehose/types"
	svcsdktypes "github.com/aws/aws-sdk-go-v2/service/firehose/types"
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

// requeueNeededForDeliveryStreamEncryptionModifying checks if the Delivery Stream's encryption status is in
// a transitory state and if it is returns a requeue error.
func requeueNeededForDeliveryStreamEncryptionModifying(latest *resource) (err error) {
	if latest.ko.Status.DeliveryStreamEncryptionConfigurationStatus == nil {
		return nil
	}

	encryptionStatus := *latest.ko.Status.DeliveryStreamEncryptionConfigurationStatus
	if encryptionStatus == *aws.String(string(svcsdktypes.DeliveryStreamEncryptionStatusEnabling)) ||
		encryptionStatus == *aws.String(string(svcsdktypes.DeliveryStreamEncryptionStatusDisabling)) {
		return ackrequeue.NeededAfter(
			fmt.Errorf(
				"delivery stream cannot be modified while server-side encryption %v",
				encryptionStatus,
			),
			5*time.Second,
		)
	}

	return nil
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

// Maps an HttpEndpointDestinationDescription to relevant Spec and Status fields.
func readHttpDestinationDescription(ko *svcapitypes.DeliveryStream, respHttpDest *types.HttpEndpointDestinationDescription) (err error) {
	if respHttpDest == nil {
		return nil
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
	if respHttpDest.BufferingHints != nil {
		if specHttpEndpointDestConfig.BufferingHints == nil {
			specHttpEndpointDestConfig.BufferingHints = &svcapitypes.HTTPEndpointBufferingHints{}
		}
		if respHttpDest.BufferingHints.IntervalInSeconds != nil {
			specHttpEndpointDestConfig.BufferingHints.IntervalInSeconds = aws.Int64(int64(*respHttpDest.BufferingHints.IntervalInSeconds))
		}
		if respHttpDest.BufferingHints.SizeInMBs != nil {
			specHttpEndpointDestConfig.BufferingHints.SizeInMBs = aws.Int64(int64(*respHttpDest.BufferingHints.SizeInMBs))
		}
	}
	if respHttpDest.EndpointConfiguration != nil {
		if specHttpEndpointDestConfig.EndpointConfiguration == nil {
			specHttpEndpointDestConfig.EndpointConfiguration = &svcapitypes.HTTPEndpointConfiguration{}
		}
		if respHttpDest.EndpointConfiguration.Name != nil {
			specHttpEndpointDestConfig.EndpointConfiguration.Name = respHttpDest.EndpointConfiguration.Name
		}
		if respHttpDest.EndpointConfiguration.Url != nil {
			specHttpEndpointDestConfig.EndpointConfiguration.URL = respHttpDest.EndpointConfiguration.Url
		}
	}
	if respHttpDest.RetryOptions != nil {
		if specHttpEndpointDestConfig.RetryOptions == nil {
			specHttpEndpointDestConfig.RetryOptions = &svcapitypes.HTTPEndpointRetryOptions{}
		}
		if respHttpDest.RetryOptions.DurationInSeconds != nil {
			specHttpEndpointDestConfig.RetryOptions.DurationInSeconds = aws.Int64(int64(*respHttpDest.RetryOptions.DurationInSeconds))
		}
	}
	if respHttpDest.CloudWatchLoggingOptions != nil {
		if specHttpEndpointDestConfig.CloudWatchLoggingOptions == nil {
			specHttpEndpointDestConfig.CloudWatchLoggingOptions = &svcapitypes.CloudWatchLoggingOptions{}
		}
		if respHttpDest.CloudWatchLoggingOptions.Enabled != nil {
			specHttpEndpointDestConfig.CloudWatchLoggingOptions.Enabled = respHttpDest.CloudWatchLoggingOptions.Enabled
		}
		if respHttpDest.CloudWatchLoggingOptions.LogGroupName != nil {
			specHttpEndpointDestConfig.CloudWatchLoggingOptions.LogGroupName = respHttpDest.CloudWatchLoggingOptions.LogGroupName
		}
		if respHttpDest.CloudWatchLoggingOptions.LogStreamName != nil {
			specHttpEndpointDestConfig.CloudWatchLoggingOptions.LogStreamName = respHttpDest.CloudWatchLoggingOptions.LogStreamName
		}
	}
	if respHttpDest.ProcessingConfiguration != nil {
		if specHttpEndpointDestConfig.ProcessingConfiguration == nil {
			specHttpEndpointDestConfig.ProcessingConfiguration = &svcapitypes.ProcessingConfiguration{}
		}
		if respHttpDest.ProcessingConfiguration.Enabled != nil {
			specHttpEndpointDestConfig.ProcessingConfiguration.Enabled = respHttpDest.ProcessingConfiguration.Enabled
		}
		if respHttpDest.ProcessingConfiguration.Processors != nil {
			specHttpEndpointDestConfig.ProcessingConfiguration.Processors = make([]*svcapitypes.Processor, len(respHttpDest.ProcessingConfiguration.Processors))
			for i, proc := range respHttpDest.ProcessingConfiguration.Processors {
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
	if respHttpDest.RequestConfiguration != nil {
		if specHttpEndpointDestConfig.RequestConfiguration == nil {
			specHttpEndpointDestConfig.RequestConfiguration = &svcapitypes.HTTPEndpointRequestConfiguration{}
		}
		if respHttpDest.RequestConfiguration.ContentEncoding != "" {
			specHttpEndpointDestConfig.RequestConfiguration.ContentEncoding = aws.String(string(respHttpDest.RequestConfiguration.ContentEncoding))
		}
		if respHttpDest.RequestConfiguration.CommonAttributes != nil {
			specHttpEndpointDestConfig.RequestConfiguration.CommonAttributes = make([]*svcapitypes.HTTPEndpointCommonAttribute, len(respHttpDest.RequestConfiguration.CommonAttributes))
			for i, attr := range respHttpDest.RequestConfiguration.CommonAttributes {
				specHttpEndpointDestConfig.RequestConfiguration.CommonAttributes[i] = &svcapitypes.HTTPEndpointCommonAttribute{
					AttributeName:  attr.AttributeName,
					AttributeValue: attr.AttributeValue,
				}
			}
		}
	}
	if respHttpDest.S3DestinationDescription != nil {
		if specHttpEndpointDestConfig.S3Configuration == nil {
			specHttpEndpointDestConfig.S3Configuration = &svcapitypes.S3DestinationConfiguration{}
		}
		if respHttpDest.S3DestinationDescription.BucketARN != nil {
			specHttpEndpointDestConfig.S3Configuration.BucketARN = respHttpDest.S3DestinationDescription.BucketARN
		}
		if respHttpDest.S3DestinationDescription.BufferingHints != nil {
			if specHttpEndpointDestConfig.S3Configuration.BufferingHints == nil {
				specHttpEndpointDestConfig.S3Configuration.BufferingHints = &svcapitypes.BufferingHints{}
			}
			if respHttpDest.S3DestinationDescription.BufferingHints.IntervalInSeconds != nil {
				specHttpEndpointDestConfig.S3Configuration.BufferingHints.IntervalInSeconds = aws.Int64(int64(*respHttpDest.S3DestinationDescription.BufferingHints.IntervalInSeconds))
			}
			if respHttpDest.S3DestinationDescription.BufferingHints.SizeInMBs != nil {
				specHttpEndpointDestConfig.S3Configuration.BufferingHints.SizeInMBs = aws.Int64(int64(*respHttpDest.S3DestinationDescription.BufferingHints.SizeInMBs))
			}
		}
		if respHttpDest.S3DestinationDescription.CompressionFormat != "" {
			specHttpEndpointDestConfig.S3Configuration.CompressionFormat = aws.String(string(respHttpDest.S3DestinationDescription.CompressionFormat))
		}
		if respHttpDest.S3DestinationDescription.EncryptionConfiguration != nil {
			if specHttpEndpointDestConfig.S3Configuration.EncryptionConfiguration == nil {
				specHttpEndpointDestConfig.S3Configuration.EncryptionConfiguration = &svcapitypes.EncryptionConfiguration{}
			}
			if respHttpDest.S3DestinationDescription.EncryptionConfiguration.KMSEncryptionConfig != nil {
				if specHttpEndpointDestConfig.S3Configuration.EncryptionConfiguration.KMSEncryptionConfig == nil {
					specHttpEndpointDestConfig.S3Configuration.EncryptionConfiguration.KMSEncryptionConfig = &svcapitypes.KMSEncryptionConfig{}
				}
				if respHttpDest.S3DestinationDescription.EncryptionConfiguration.KMSEncryptionConfig.AWSKMSKeyARN != nil {
					specHttpEndpointDestConfig.S3Configuration.EncryptionConfiguration.KMSEncryptionConfig.AWSKMSKeyARN = respHttpDest.S3DestinationDescription.EncryptionConfiguration.KMSEncryptionConfig.AWSKMSKeyARN
				}
			}
			if respHttpDest.S3DestinationDescription.EncryptionConfiguration.NoEncryptionConfig != "" {
				specHttpEndpointDestConfig.S3Configuration.EncryptionConfiguration.NoEncryptionConfig = aws.String(string(respHttpDest.S3DestinationDescription.EncryptionConfiguration.NoEncryptionConfig))
			}
		}
		if respHttpDest.S3DestinationDescription.RoleARN != nil {
			specHttpEndpointDestConfig.S3Configuration.RoleARN = respHttpDest.S3DestinationDescription.RoleARN
		}
		if respHttpDest.S3DestinationDescription.CloudWatchLoggingOptions != nil {
			if specHttpEndpointDestConfig.S3Configuration.CloudWatchLoggingOptions == nil {
				specHttpEndpointDestConfig.S3Configuration.CloudWatchLoggingOptions = &svcapitypes.CloudWatchLoggingOptions{}
			}
			if respHttpDest.S3DestinationDescription.CloudWatchLoggingOptions.Enabled != nil {
				specHttpEndpointDestConfig.S3Configuration.CloudWatchLoggingOptions.Enabled = respHttpDest.S3DestinationDescription.CloudWatchLoggingOptions.Enabled
			}
			if respHttpDest.S3DestinationDescription.CloudWatchLoggingOptions.LogGroupName != nil {
				specHttpEndpointDestConfig.S3Configuration.CloudWatchLoggingOptions.LogGroupName = respHttpDest.S3DestinationDescription.CloudWatchLoggingOptions.LogGroupName
			}
			if respHttpDest.S3DestinationDescription.CloudWatchLoggingOptions.LogStreamName != nil {
				specHttpEndpointDestConfig.S3Configuration.CloudWatchLoggingOptions.LogStreamName = respHttpDest.S3DestinationDescription.CloudWatchLoggingOptions.LogStreamName
			}
		}
		if respHttpDest.S3DestinationDescription.ErrorOutputPrefix != nil {
			specHttpEndpointDestConfig.S3Configuration.ErrorOutputPrefix = respHttpDest.S3DestinationDescription.ErrorOutputPrefix
		}
		if respHttpDest.S3DestinationDescription.Prefix != nil {
			specHttpEndpointDestConfig.S3Configuration.Prefix = respHttpDest.S3DestinationDescription.Prefix
		}

	}
	if respHttpDest.SecretsManagerConfiguration != nil {
		if specHttpEndpointDestConfig.SecretsManagerConfiguration == nil {
			specHttpEndpointDestConfig.SecretsManagerConfiguration = &svcapitypes.SecretsManagerConfiguration{}
		}
		if respHttpDest.SecretsManagerConfiguration.Enabled != nil {
			specHttpEndpointDestConfig.SecretsManagerConfiguration.Enabled = respHttpDest.SecretsManagerConfiguration.Enabled
		}
		if respHttpDest.SecretsManagerConfiguration.RoleARN != nil {
			specHttpEndpointDestConfig.SecretsManagerConfiguration.RoleARN = respHttpDest.SecretsManagerConfiguration.RoleARN
		}
		if respHttpDest.SecretsManagerConfiguration.SecretARN != nil {
			specHttpEndpointDestConfig.SecretsManagerConfiguration.SecretARN = respHttpDest.SecretsManagerConfiguration.SecretARN
		}
	}

	return nil
}
