package api

import (
	"errors"
	apiv1 "github.com/yarefs/carnax/gen/api/v1"
)

// ValidateTopicConfig will run some validations on the given TopicConfig
// returns any errors and a boolean value signalling if the topic config is
// valid (i.e. OK).
func ValidateTopicConfig(config *apiv1.TopicConfig) ([]error, bool) {
	var errs []error

	if config.PartitionCount <= 0 {
		errs = append(errs, errors.New("invalid partition count"))
	}

	if config.Name == "" {
		errs = append(errs, errors.New("no topic name specified"))
	}

	return errs, len(errs) == 0
}
