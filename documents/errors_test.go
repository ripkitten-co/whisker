package documents

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ripkitten-co/whisker"
)

func TestBatchError_Error(t *testing.T) {
	tests := []struct {
		name string
		err  *BatchError
		want string
	}{
		{
			name: "insert with failures",
			err: &BatchError{
				Op:    "insert",
				Total: 5,
				Errors: map[string]error{
					"doc-1": whisker.ErrDuplicateID,
					"doc-3": fmt.Errorf("some failure"),
				},
			},
			want: "batch insert: 2 of 5 documents failed",
		},
		{
			name: "update single failure",
			err: &BatchError{
				Op:    "update",
				Total: 3,
				Errors: map[string]error{
					"doc-2": whisker.ErrVersionConflict,
				},
			},
			want: "batch update: 1 of 3 documents failed",
		},
		{
			name: "delete all failed",
			err: &BatchError{
				Op:    "delete",
				Total: 2,
				Errors: map[string]error{
					"doc-1": whisker.ErrNotFound,
					"doc-2": whisker.ErrNotFound,
				},
			},
			want: "batch delete: 2 of 2 documents failed",
		},
		{
			name: "load operation",
			err: &BatchError{
				Op:    "load",
				Total: 10,
				Errors: map[string]error{
					"doc-7": whisker.ErrNotFound,
				},
			},
			want: "batch load: 1 of 10 documents failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.err.Error()
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBatchError_Unwrap(t *testing.T) {
	err1 := whisker.ErrDuplicateID
	err2 := fmt.Errorf("wrapped: %w", whisker.ErrNotFound)

	be := &BatchError{
		Op:    "insert",
		Total: 3,
		Errors: map[string]error{
			"doc-1": err1,
			"doc-2": err2,
		},
	}

	inner := be.Unwrap()
	if len(inner) != 2 {
		t.Fatalf("got %d errors, want 2", len(inner))
	}

	found := map[error]bool{}
	for _, e := range inner {
		found[e] = true
	}
	if !found[err1] {
		t.Error("missing err1 in unwrapped errors")
	}
	if !found[err2] {
		t.Error("missing err2 in unwrapped errors")
	}
}

func TestBatchError_IsMatchesInnerErrors(t *testing.T) {
	be := &BatchError{
		Op:    "insert",
		Total: 5,
		Errors: map[string]error{
			"doc-1": whisker.ErrDuplicateID,
			"doc-2": fmt.Errorf("collection users: update doc-2: %w", whisker.ErrVersionConflict),
		},
	}

	if !errors.Is(be, whisker.ErrDuplicateID) {
		t.Error("errors.Is should match ErrDuplicateID through Unwrap")
	}
	if !errors.Is(be, whisker.ErrVersionConflict) {
		t.Error("errors.Is should match ErrVersionConflict through Unwrap")
	}
	if errors.Is(be, whisker.ErrNotFound) {
		t.Error("errors.Is should not match ErrNotFound when not present")
	}
}

func TestBatchError_NilWhenNoErrors(t *testing.T) {
	got := newBatchError("insert", 5)
	if got != nil {
		t.Errorf("expected nil, got %v", got)
	}

	got = newBatchError("insert", 5, nil)
	if got != nil {
		t.Errorf("expected nil for nil map, got %v", got)
	}

	got = newBatchError("insert", 5, map[string]error{})
	if got != nil {
		t.Errorf("expected nil for empty map, got %v", got)
	}
}
