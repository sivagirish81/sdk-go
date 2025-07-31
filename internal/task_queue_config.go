package internal

import (
	"errors"

	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
)

// UpdateTaskQueueConfigOptions is the input to [Client.UpdateTaskQueueConfig].
//
// Exposed as: [go.temporal.io/sdk/client.UpdateTaskQueueConfigOptions]
type UpdateTaskQueueConfigOptions struct {
	// The task queue to update the configuration of.
	TaskQueue string
	// The identity of the client making the request.
	Identity string
	// The type of task queue (workflow, activity, nexus).
	TaskQueueType enumspb.TaskQueueType
	// The overall queue rate limit. If unset, the worker-set rate limit takes effect.
	// The rate limit set by this API overrides the worker-set rate limit.
	QueueRateLimit *QueueRateLimitUpdate
	// The default fairness key rate limit. If unset, this configuration is unchanged.
	FairnessKeyRateLimitDefault *QueueRateLimitUpdate
}

// QueueRateLimitUpdate represents a rate limit update with an optional reason.
//
// Exposed as: [go.temporal.io/sdk/client.QueueRateLimitUpdate]
type QueueRateLimitUpdate struct {
	// The rate limit value in requests per second. If nil, removes the existing rate limit.
	RateLimit *float64
	// The reason for setting this rate limit.
	Reason string
}

// TaskQueueConfig is the response to [Client.UpdateTaskQueueConfig].
//
// Exposed as: [go.temporal.io/sdk/client.TaskQueueConfig]
type TaskQueueConfig struct {
	// The overall queue rate limit. If nil, the worker-set rate limit is in effect.
	QueueRateLimit *float64
	// The default fairness key rate limit. If nil, no default fairness key rate limit is set.
	FairnessKeyRateLimitDefault *float64
}

func (utc *UpdateTaskQueueConfigOptions) validateAndConvertToProto(namespace string) (*workflowservice.UpdateTaskQueueConfigRequest, error) {
	if namespace == "" {
		return nil, errors.New("missing namespace argument")
	}
	if utc.TaskQueue == "" {
		return nil, errors.New("missing TaskQueue field")
	}

	req := &workflowservice.UpdateTaskQueueConfigRequest{
		Namespace:     namespace,
		Identity:      utc.Identity,
		TaskQueue:     utc.TaskQueue,
		TaskQueueType: utc.TaskQueueType,
	}

	// Set queue rate limit if provided
	if utc.QueueRateLimit != nil {
		req.UpdateQueueRateLimit = &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			Reason: utc.QueueRateLimit.Reason,
		}
		if utc.QueueRateLimit.RateLimit != nil {
			req.UpdateQueueRateLimit.RateLimit = &taskqueuepb.RateLimit{
				RequestsPerSecond: float32(*utc.QueueRateLimit.RateLimit),
			}
		}
	}

	// Set fairness key rate limit default if provided
	if utc.FairnessKeyRateLimitDefault != nil {
		req.UpdateFairnessKeyRateLimitDefault = &workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate{
			Reason: utc.FairnessKeyRateLimitDefault.Reason,
		}
		if utc.FairnessKeyRateLimitDefault.RateLimit != nil {
			req.UpdateFairnessKeyRateLimitDefault.RateLimit = &taskqueuepb.RateLimit{
				RequestsPerSecond: float32(*utc.FairnessKeyRateLimitDefault.RateLimit),
			}
		}
	}

	return req, nil
}

func taskQueueConfigFromProtoResponse(response *workflowservice.UpdateTaskQueueConfigResponse) *TaskQueueConfig {
	if response == nil || response.Config == nil {
		return &TaskQueueConfig{}
	}

	var queueRateLimit *float64
	if response.Config.QueueRateLimit != nil && response.Config.QueueRateLimit.RateLimit != nil {
		value := float64(response.Config.QueueRateLimit.RateLimit.RequestsPerSecond)
		queueRateLimit = &value
	}

	var fairnessKeyRateLimitDefault *float64
	if response.Config.FairnessKeysRateLimitDefault != nil && response.Config.FairnessKeysRateLimitDefault.RateLimit != nil {
		value := float64(response.Config.FairnessKeysRateLimitDefault.RateLimit.RequestsPerSecond)
		fairnessKeyRateLimitDefault = &value
	}

	return &TaskQueueConfig{
		QueueRateLimit:              queueRateLimit,
		FairnessKeyRateLimitDefault: fairnessKeyRateLimitDefault,
	}
}
