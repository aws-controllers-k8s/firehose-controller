    // Set CurrentDeliveryStreamVersionId from latest to ensure most
    // recent version ID is used in the update request.
    if latest.ko.Status.VersionID != nil {
		input.CurrentDeliveryStreamVersionId = latest.ko.Status.VersionID
	}

    // DestinationID a
    if latest.ko.Status.DestinationID != nil {
		input.DestinationId = latest.ko.Status.DestinationID
	}