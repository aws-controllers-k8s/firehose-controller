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
	"testing"

	svcapitypes "github.com/aws-controllers-k8s/firehose-controller/apis/v1alpha1"
	"github.com/aws/aws-sdk-go/aws"
)

func TestDeliveryStreamEncryptionConfigurationComparison(t *testing.T) {
	tests := []struct {
		name     string
		a        *resource
		b        *resource
		expected bool // true if difference expected
	}{
		{
			name: "Both nil expects no difference",
			a: &resource{
				ko: &svcapitypes.DeliveryStream{
					Spec: svcapitypes.DeliveryStreamSpec{
						DeliveryStreamEncryptionConfiguration: nil,
					},
				},
			},
			b: &resource{
				ko: &svcapitypes.DeliveryStream{
					Spec: svcapitypes.DeliveryStreamSpec{
						DeliveryStreamEncryptionConfiguration: nil,
					},
				},
			},
			expected: false,
		},
		{
			name: "a nil and b empty expects no difference",
			a: &resource{
				ko: &svcapitypes.DeliveryStream{
					Spec: svcapitypes.DeliveryStreamSpec{
						DeliveryStreamEncryptionConfiguration: nil,
					},
				},
			},
			b: &resource{
				ko: &svcapitypes.DeliveryStream{
					Spec: svcapitypes.DeliveryStreamSpec{
						DeliveryStreamEncryptionConfiguration: &svcapitypes.DeliveryStreamEncryptionConfigurationInput{},
					},
				},
			},
			expected: false,
		},
		{
			name: "a empty and b nil expects no difference",
			a: &resource{
				ko: &svcapitypes.DeliveryStream{
					Spec: svcapitypes.DeliveryStreamSpec{
						DeliveryStreamEncryptionConfiguration: &svcapitypes.DeliveryStreamEncryptionConfigurationInput{},
					},
				},
			},
			b: &resource{
				ko: &svcapitypes.DeliveryStream{
					Spec: svcapitypes.DeliveryStreamSpec{
						DeliveryStreamEncryptionConfiguration: nil,
					},
				},
			},
			expected: false,
		},
		{
			name: "Both empty expects no difference",
			a: &resource{
				ko: &svcapitypes.DeliveryStream{
					Spec: svcapitypes.DeliveryStreamSpec{
						DeliveryStreamEncryptionConfiguration: &svcapitypes.DeliveryStreamEncryptionConfigurationInput{},
					},
				},
			},
			b: &resource{
				ko: &svcapitypes.DeliveryStream{
					Spec: svcapitypes.DeliveryStreamSpec{
						DeliveryStreamEncryptionConfiguration: &svcapitypes.DeliveryStreamEncryptionConfigurationInput{},
					},
				},
			},
			expected: false,
		},
		{
			name: "a nil and b with non-empty has difference",
			a: &resource{
				ko: &svcapitypes.DeliveryStream{
					Spec: svcapitypes.DeliveryStreamSpec{
						DeliveryStreamEncryptionConfiguration: nil,
					},
				},
			},
			b: &resource{
				ko: &svcapitypes.DeliveryStream{
					Spec: svcapitypes.DeliveryStreamSpec{
						DeliveryStreamEncryptionConfiguration: &svcapitypes.DeliveryStreamEncryptionConfigurationInput{
							KeyType: aws.String("AWS_OWNED_CMK"),
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "a empty and b non-empty has difference",
			a: &resource{
				ko: &svcapitypes.DeliveryStream{
					Spec: svcapitypes.DeliveryStreamSpec{
						DeliveryStreamEncryptionConfiguration: &svcapitypes.DeliveryStreamEncryptionConfigurationInput{},
					},
				},
			},
			b: &resource{
				ko: &svcapitypes.DeliveryStream{
					Spec: svcapitypes.DeliveryStreamSpec{
						DeliveryStreamEncryptionConfiguration: &svcapitypes.DeliveryStreamEncryptionConfigurationInput{
							KeyType: aws.String("AWS_OWNED_CMK"),
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "a and b non-empty but the same expects no difference",
			a: &resource{
				ko: &svcapitypes.DeliveryStream{
					Spec: svcapitypes.DeliveryStreamSpec{
						DeliveryStreamEncryptionConfiguration: &svcapitypes.DeliveryStreamEncryptionConfigurationInput{
							KeyType: aws.String("AWS_OWNED_CMK"),
							KeyARN:  aws.String("arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"),
						},
					},
				},
			},
			b: &resource{
				ko: &svcapitypes.DeliveryStream{
					Spec: svcapitypes.DeliveryStreamSpec{
						DeliveryStreamEncryptionConfiguration: &svcapitypes.DeliveryStreamEncryptionConfigurationInput{
							KeyType: aws.String("AWS_OWNED_CMK"),
							KeyARN:  aws.String("arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"),
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "a and b non-empty but different values has difference",
			a: &resource{
				ko: &svcapitypes.DeliveryStream{
					Spec: svcapitypes.DeliveryStreamSpec{
						DeliveryStreamEncryptionConfiguration: &svcapitypes.DeliveryStreamEncryptionConfigurationInput{
							KeyType: aws.String("AWS_OWNED_CMK"),
							KeyARN:  aws.String("arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"),
						},
					},
				},
			},
			b: &resource{
				ko: &svcapitypes.DeliveryStream{
					Spec: svcapitypes.DeliveryStreamSpec{
						DeliveryStreamEncryptionConfiguration: &svcapitypes.DeliveryStreamEncryptionConfigurationInput{
							KeyType: aws.String("CUSTOMER_MANAGED_CMK"),
							KeyARN:  aws.String("arn:aws:kms:us-east-1:123456789012:key/87654321-4321-4321-4321-210987654321"),
						},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delta := newResourceDelta(tt.a, tt.b)
			hasDifference := delta.DifferentAt("Spec.DeliveryStreamEncryptionConfiguration")

			if hasDifference != tt.expected {
				t.Errorf("Expected difference: %v, got: %v", tt.expected, hasDifference)
			}
		})
	}
}
