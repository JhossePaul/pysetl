# Contributing to PySetl

Thank you for your interest in contributing to PySetl! 🎉

We welcome contributions of all kinds: bug reports, feature requests, code, documentation, and more. This guide will help you get started as a PySetl contributor.

---

## Development Tech Stack
- **Project Management:** [Hatch](https://hatch.pypa.io/) (build, test, envs)
- **Dependency Management:** [uv](https://github.com/astral-sh/uv) (fast installer, works with pyproject.toml)
- **Linting & Formatting:** [Ruff](https://docs.astral.sh/ruff/)
- **Type Checking:** [mypy](https://mypy-lang.org/)
- **Pre-commit Hooks:** [pre-commit](https://pre-commit.com/)
- **Testing:** [pytest](https://docs.pytest.org/) with coverage
- **Security:** [bandit](https://bandit.readthedocs.io/), [safety](https://pyup.io/safety/), [pip-audit](https://pypi.org/project/pip-audit/)
- **Documentation:** [MkDocs](https://www.mkdocs.org/) with Material theme and plugins

---

## Getting Started

1. **Fork and Clone**
   ```bash
   git clone https://github.com/JhossePaul/pysetl.git
   cd pysetl
   ```

2. **Set Up the Dev Environment**
   ```bash
   # (All dependencies are managed via pyproject.toml)
   hatch shell
   hatch env show
   ```

3. **Install Pre-commit Hooks**
   ```bash
   pre-commit install
   ```

---

## Development Workflow

- Create a new branch for your feature or bugfix:
  ```bash
  git checkout -b my-feature
  ```
- Make your changes and commit with clear messages.
- Run all checks locally before pushing:
  ```bash
  hatch run type
  hatch run lint
  hatch run format
  hatch test  --all
  ```
- If you update docs, check them locally:
  ```bash
  hatch run docs:serve
  ```
- Push your branch and open a Pull Request (PR) on GitHub.
- Ensure all CI checks pass and respond to review feedback.

---

## Code Style & Quality

- Code must pass **Ruff** (PEP8 + best practices):
  ```bash
  hatch run lint
  ```
- Code formatting with **Ruff**:
  ```bash
  hatch run format
  ```
- Type annotations are required and checked with **mypy**:
  ```bash
  hatch run type
  ```
- All new code should include tests (pytest) and docstrings.
- Use pre-commit to auto-format and catch issues before commit.

---

## Documentation

- User and API docs are in `docs/` (Markdown, MkDocs).
- To build docs locally:
  ```bash
  hatch run docs:docs
  ```
- For API docs, use Google-style docstrings.

---

## Tests

- All tests are in the `tests/` directory.
- Run the test suite (default environment only):
  ```bash
  hatch test
  ```
- Run all test matrix:
  ```bash
  hatch test --all
  ```
- Run all tests with coverage (all matrix):
  ```bash
  hatch test --cover --all
  ```
- Security checks are available:
  ```bash
  hatch run security:all
  ```

---

## Project Structure

### Hatch Environments

The project uses Hatch environments for different development tasks:

- **`default`**: Basic development tools (ruff, mypy, pytest, pre-commit, coverage)
- **`test`**: Testing environment with PySpark for integration tests (Python 3.9–3.13)
- **`docs`**: Documentation building tools
- **`security`**: Security scanning tools (bandit, safety, pip-audit)

### Available Commands

- **`hatch run type`**: Type checking with mypy
- **`hatch run lint`**: Lint code with Ruff
- **`hatch run format`**: Format code with Ruff
- **`hatch run docs:docs`**: Build documentation
- **`hatch run docs:serve`**: Serve documentation locally
- **`hatch run security:all`**: Run all security checks
- **`hatch test`**: Run tests (default environment only)
- **`hatch test --all`**: Run all test matrix
- **`hatch test --cover --all`**: Run all test matrix with coverage

---

## Community & Support
- Please be respectful and follow our [Code of Conduct](code-of-conduct.md).
- Open issues for bugs, questions, or feature requests.
- Join discussions on GitHub for ideas and feedback.

---

Thank you for making PySetl better! 🚀
