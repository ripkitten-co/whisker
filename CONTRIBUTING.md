# Contributing to Whisker

Thanks for your interest in contributing! This guide will help you get started.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/YOUR_USERNAME/whisker.git`
3. Create a branch: `git checkout -b feature/your-feature`
4. Make your changes
5. Push and open a pull request against `main`

## Development Setup

**Requirements:** Go 1.22+, Docker (for integration tests)

```bash
go mod download          # install dependencies
go build ./...           # verify it compiles
go test ./...            # run unit tests
go test -tags=integration -race ./...  # run all tests (needs Docker)
```

## Code Style

- Run `golangci-lint run ./...` before submitting
- Run `gofumpt -w .` for formatting
- Follow existing patterns in the codebase

## Commit Messages

We use [Conventional Commits](https://www.conventionalcommits.org/):

- `feat:` new feature
- `fix:` bug fix
- `docs:` documentation
- `test:` test changes
- `refactor:` code restructuring
- `chore:` maintenance

Keep the subject under 72 characters. Focus on *why*, not *what*.

## Pull Requests

- Keep PRs focused on a single change
- Add tests for new functionality
- Update documentation if behavior changes
- Fill out the PR template

## Testing

- Unit tests: `go test ./...`
- Integration tests: `go test -tags=integration ./...` (requires Docker)
- Race detection: `go test -race ./...`
- Every bug fix should include a regression test

## Reporting Issues

- Use the [bug report template](https://github.com/ripkitten-co/whisker/issues/new?template=bug_report.yml) for bugs
- Use the [feature request template](https://github.com/ripkitten-co/whisker/issues/new?template=feature_request.yml) for ideas
- Ask questions in [Discussions](https://github.com/ripkitten-co/whisker/discussions)
