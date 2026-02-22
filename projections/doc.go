// Package projections provides async read-model projections and side-effect
// handlers for Whisker event streams. Projections transform events into
// queryable document collections; handlers react to events for external
// side effects. Both share checkpoint infrastructure for at-least-once
// delivery and PostgreSQL advisory locks for single-writer coordination.
package projections
