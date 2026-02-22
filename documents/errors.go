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
