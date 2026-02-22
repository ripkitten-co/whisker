package whisker

import "errors"

var (
	// ErrNotFound is returned when a document or stream does not exist.
	ErrNotFound = errors.New("not found")

	// ErrConcurrencyConflict is returned when an optimistic locking check fails.
	ErrConcurrencyConflict = errors.New("concurrency conflict")

	// ErrStreamExists is returned when appending to an already-existing stream
	// with expectedVersion 0.
	ErrStreamExists = errors.New("stream already exists")
)
