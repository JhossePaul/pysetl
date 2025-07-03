# Contributing to PySetl

Thank you for your interest in contributing to PySetl! 🎉

We welcome contributions of all kinds: bug reports, feature requests, code, documentation, and more. This guide will help you get started as a PySetl contributor.

---

## Development Tech Stack
- **Project Management:** [Hatch](https://hatch.pypa.io/) (build, test, envs)
- **Dependency Management:** [uv](https://github.com/astral-sh/uv) (fast installer, works with pyproject.toml)
- **Linting:** [Ruff](https://docs.astral.sh/ruff/)
- **Type Checking:** [mypy](https://mypy-lang.org/)
- **Pre-commit Hooks:** [pre-commit](https://pre-commit.com/)
- **Docs:** [MkDocs](https://www.mkdocs.org/) with Material theme and plugins
- **Tests:** [pytest](https://docs.pytest.org/)

---

## Getting Started

1. **Fork and Clone**
   ```bash
   git clone https://github.com/JhossePaul/pysetl.git
   cd pysetl
   ```

2. **Set Up the Dev Environment**
   ```bash
   hatch shell
   # (All dependencies are managed via pyproject.toml)
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
- Run tests, lint, and type checks locally before pushing:
  ```bash
  hatch run lint
  hatch run type
  hatch run test
  ```
- If you update docs, check them locally:
  ```bash
  hatch run docs:serve
  # Visit http://localhost:8000
  ```
- Push your branch and open a Pull Request (PR) on GitHub.
- Ensure all CI checks pass and respond to review feedback.

---

## Code Style & Quality
- Code must pass **Ruff** (PEP8 + best practices):
  ```bash
  hatch run lint
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
- To build and preview docs locally:
  ```bash
  hatch run docs:serve
  ```
- For API docs, use Google-style docstrings.

---

## Tests
- All tests are in the `tests/` directory.
- Run the full suite with:
  ```bash
  hatch run test
  ```
- Coverage and security checks are available via Hatch tasks.

---

## Community & Support
- Please be respectful and follow our [Code of Conduct](code-of-conduct.md).
- Open issues for bugs, questions, or feature requests.
- Join discussions on GitHub for ideas and feedback.

---

Thank you for making PySetl better! 🚀
