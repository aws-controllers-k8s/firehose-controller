    err = requeueNeededForDeliveryStreamEncryptionModifying(latest)
	if err != nil {
		return nil, err
	}

	if delta.DifferentAt("Spec.DeliveryStreamEncryptionConfiguration") {
		err = updateDeliveryStreamEncryptionConfiguration(ctx, desired, rm.sdkapi, rm.metrics)
		if err != nil {
			return nil, err
		}
	}

	if delta.DifferentAt("Spec.Tags") {
		err = rm.syncTags(ctx, desired, latest)
		if err != nil {
			return nil, err
		}
	}

	if !delta.DifferentExcept("Spec.DeliveryStreamEncryptionConfiguration", "Spec.Tags") {
		return desired, nil
	}