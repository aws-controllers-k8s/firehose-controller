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