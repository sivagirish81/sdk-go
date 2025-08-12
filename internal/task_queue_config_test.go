package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
)

func TestUpdateTaskQueueConfigOptions_validateAndConvertToProto(t *testing.T) {
	tests := []struct {
		name      string
		options   UpdateTaskQueueConfigOptions
		namespace string
		wantErr   bool
	}{
		{
			name: "valid options with queue rate limit",
			options: UpdateTaskQueueConfigOptions{
				TaskQueue:     "test-queue",
				Identity:      "test-identity",
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				QueueRateLimit: &QueueRateLimitUpdate{
					RateLimit: float64Ptr(100.0),
					Reason:    "test reason",
				},
			},
			namespace: "test-namespace",
			wantErr:   false,
		},
		{
			name: "valid options with fairness key rate limit",
			options: UpdateTaskQueueConfigOptions{
				TaskQueue:     "test-queue",
				Identity:      "test-identity",
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_ACTIVITY,
				FairnessKeyRateLimitDefault: &QueueRateLimitUpdate{
					RateLimit: float64Ptr(45.0),
					Reason:    "Fairness key test",
				},
			},
			namespace: "test-namespace",
			wantErr:   false,
		},
		{
			name: "valid options with both rate limits",
			options: UpdateTaskQueueConfigOptions{
				TaskQueue:     "test-queue",
				Identity:      "test-identity",
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_WORKFLOW,
				QueueRateLimit: &QueueRateLimitUpdate{
					RateLimit: nil, // Remove existing rate limit
					Reason:    "gRPC test",
				},
				FairnessKeyRateLimitDefault: &QueueRateLimitUpdate{
					RateLimit: float64Ptr(45.0),
					Reason:    "Fairness key test",
				},
			},
			namespace: "test-namespace",
			wantErr:   false,
		},
		{
			name: "valid options without rate limits",
			options: UpdateTaskQueueConfigOptions{
				TaskQueue:     "test-queue",
				Identity:      "test-identity",
				TaskQueueType: enumspb.TASK_QUEUE_TYPE_NEXUS,
			},
			namespace: "test-namespace",
			wantErr:   false,
		},
		{
			name: "missing namespace",
			options: UpdateTaskQueueConfigOptions{
				TaskQueue: "test-queue",
			},
			namespace: "",
			wantErr:   true,
		},
		{
			name: "missing task queue",
			options: UpdateTaskQueueConfigOptions{
				TaskQueue: "",
			},
			namespace: "test-namespace",
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.options.validateAndConvertToProto(tt.namespace)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, result)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.Equal(t, tt.namespace, result.Namespace)
				assert.Equal(t, tt.options.Identity, result.Identity)
				assert.Equal(t, tt.options.TaskQueue, result.TaskQueue)
				assert.Equal(t, tt.options.TaskQueueType, result.TaskQueueType)

				// Check queue rate limit
				if tt.options.QueueRateLimit != nil {
					assert.NotNil(t, result.UpdateQueueRateLimit)
					assert.Equal(t, tt.options.QueueRateLimit.Reason, result.UpdateQueueRateLimit.Reason)
					if tt.options.QueueRateLimit.RateLimit != nil {
						assert.NotNil(t, result.UpdateQueueRateLimit.RateLimit)
						assert.Equal(t, float32(*tt.options.QueueRateLimit.RateLimit), result.UpdateQueueRateLimit.RateLimit.RequestsPerSecond)
					} else {
						assert.Nil(t, result.UpdateQueueRateLimit.RateLimit)
					}
				} else {
					assert.Nil(t, result.UpdateQueueRateLimit)
				}

				// Check fairness key rate limit
				if tt.options.FairnessKeyRateLimitDefault != nil {
					assert.NotNil(t, result.UpdateFairnessKeyRateLimitDefault)
					assert.Equal(t, tt.options.FairnessKeyRateLimitDefault.Reason, result.UpdateFairnessKeyRateLimitDefault.Reason)
					if tt.options.FairnessKeyRateLimitDefault.RateLimit != nil {
						assert.NotNil(t, result.UpdateFairnessKeyRateLimitDefault.RateLimit)
						assert.Equal(t, float32(*tt.options.FairnessKeyRateLimitDefault.RateLimit), result.UpdateFairnessKeyRateLimitDefault.RateLimit.RequestsPerSecond)
					} else {
						assert.Nil(t, result.UpdateFairnessKeyRateLimitDefault.RateLimit)
					}
				} else {
					assert.Nil(t, result.UpdateFairnessKeyRateLimitDefault)
				}
			}
		})
	}
}

func TestTaskQueueConfigFromProtoResponse(t *testing.T) {
	// Test with nil response
	result := taskQueueConfigFromProtoResponse(nil)
	assert.NotNil(t, result)
	assert.Nil(t, result.QueueRateLimit)
	assert.Nil(t, result.FairnessKeyRateLimitDefault)
}

func float64Ptr(v float64) *float64 {
	return &v
}
