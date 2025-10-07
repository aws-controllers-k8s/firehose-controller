    
	if resp.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration != nil {
		if resp.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration.Status != "" {
			ko.Status.DeliveryStreamEncryptionConfigurationStatus = aws.String(string(resp.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration.Status))
		}

		if resp.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration.FailureDescription != nil {
			ko.Status.DeliveryStreamEncryptionConfigurationFailureDescription = &svcapitypes.FailureDescription{}
			if resp.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration.FailureDescription.Type != "" {
				ko.Status.DeliveryStreamEncryptionConfigurationFailureDescription.Type = aws.String(string(resp.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration.FailureDescription.Type))
			}
			if resp.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration.FailureDescription.Details != nil {
				ko.Status.DeliveryStreamEncryptionConfigurationFailureDescription.Details = resp.DeliveryStreamDescription.DeliveryStreamEncryptionConfiguration.FailureDescription.Details
			}
		}
	}

	// From the DeliveryStream API docs there is only ever one Destination per Delivery Stream.
	if len(resp.DeliveryStreamDescription.Destinations) > 0 {
		ko.Status.DestinationID = resp.DeliveryStreamDescription.Destinations[0].DestinationId
	}

	setDestinations(ko, resp)

	ko.Spec.Tags, err = rm.getTags(ctx, *r.ko.Spec.DeliveryStreamName)
	if err != nil {
		return nil, err
	}