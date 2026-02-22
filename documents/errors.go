package documents

import "fmt"

// BatchError collects per-document errors from a batch operation.
// The Errors map is keyed by document ID.
type BatchError struct {
	Op     string
	Total  int
	Errors map[string]error
}

func (e *BatchError) Error() string {
	return fmt.Sprintf("batch %s: %d of %d documents failed", e.Op, len(e.Errors), e.Total)
}

// Unwrap returns all inner errors, enabling errors.Is and errors.As
// to match through the batch.
func (e *BatchError) Unwrap() []error {
	errs := make([]error, 0, len(e.Errors))
	for _, err := range e.Errors {
		errs = append(errs, err)
	}
	return errs
}

func newBatchError(op string, total int, errs ...map[string]error) *BatchError {
	merged := map[string]error{}
	for _, m := range errs {
		for k, v := range m {
			merged[k] = v
		}
	}
	if len(merged) == 0 {
		return nil
	}
	return &BatchError{Op: op, Total: total, Errors: merged}
}
