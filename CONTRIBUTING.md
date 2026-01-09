# Contributing to casbin-informer-watcher

Thank you for your interest in contributing to casbin-informer-watcher! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Go 1.19 or later
- Kubernetes cluster (for testing, you can use kind, minikube, or k3s)
- kubectl configured to access your cluster

### Getting Started

1. Fork the repository
2. Clone your fork:
   ```bash
   git clone https://github.com/YOUR_USERNAME/casbin-informer-watcher.git
   cd casbin-informer-watcher
   ```

3. Install dependencies:
   ```bash
   go mod download
   ```

4. Run tests:
   ```bash
   go test -v ./...
   ```

## Making Changes

### Code Style

- Follow standard Go conventions
- Run `go fmt ./...` before committing
- Run `go vet ./...` to catch common issues
- Write clear, concise comments for exported functions and types

### Testing

- Write unit tests for new functionality
- Ensure all tests pass before submitting a PR
- Add integration tests for complex features
- Aim for good test coverage

### Commit Messages

- Use clear and descriptive commit messages
- Follow the format: `<type>: <description>`
- Types: `feat`, `fix`, `docs`, `test`, `refactor`, `chore`

Examples:
```
feat: add support for custom policy types
fix: handle nil pointer in callback
docs: update README with new examples
test: add integration tests for concurrent updates
```

## Testing

### Unit Tests

Run unit tests with:
```bash
go test -v ./...
```

### Integration Tests

Run integration tests with:
```bash
go test -v -run TestIntegration ./...
```

### Testing with a Real Cluster

1. Deploy the CRD:
   ```bash
   kubectl apply -f config/crd/casbinpolicy.yaml
   ```

2. Apply sample policies:
   ```bash
   kubectl apply -f examples/policies.yaml
   ```

3. Run your application with the watcher enabled

## Pull Request Process

1. Update the README.md with details of changes if applicable
2. Update documentation and examples as needed
3. Ensure all tests pass
4. Update the CHANGELOG.md (if one exists)
5. Submit your PR with a clear description of the changes

### PR Guidelines

- Keep PRs focused on a single feature or fix
- Include tests for new functionality
- Update documentation as needed
- Link to any related issues

## Code Review

- Be respectful and constructive
- Address all review comments
- Be patient - maintainers review PRs as time allows

## Questions?

If you have questions, feel free to:
- Open an issue for discussion
- Ask in the PR if it's related to your contribution

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
