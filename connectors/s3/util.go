package s3

import "github.com/aws/smithy-go"

func IsS3OptimisticLockFailedError(apiError smithy.APIError) bool {
	return apiError.ErrorCode() == "PreconditionFailed" || apiError.ErrorCode() == "412" || apiError.ErrorCode() == "409" || apiError.ErrorCode() == "ConditionalRequestConflict"
}

// ErrS3OptimisticLockFailed is a custom error to signal an ETag mismatch
type ErrS3OptimisticLockFailed struct {
	err error
}

func (e *ErrS3OptimisticLockFailed) Error() string {
	return "s3 precondition failed (etag mismatch): " + e.err.Error()
}

func (e *ErrS3OptimisticLockFailed) Unwrap() error {
	return e.err
}
