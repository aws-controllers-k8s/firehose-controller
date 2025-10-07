    // When server-side encryption is disabled DescribeDeliveryStream will return an empty DeliveryStreamEncryptionConfiguration
    // object. 
    if deliveryStreamEncryptionDisabled(a) && deliveryStreamEncryptionDisabled(b) {
		a.ko.Spec.DeliveryStreamEncryptionConfiguration = b.ko.Spec.DeliveryStreamEncryptionConfiguration
	}