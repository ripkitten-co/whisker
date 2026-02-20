package whisker

import "errors"

var (
	ErrNotFound            = errors.New("not found")
	ErrConcurrencyConflict = errors.New("concurrency conflict")
	ErrStreamExists        = errors.New("stream already exists")
)
