# Installation

This guide covers all the ways to install and set up PySetl in your environment.

---

## Requirements

PySetl requires:

- **Python**: 3.9, 3.10, 3.11, 3.12, or 3.13
- **PySpark**: 3.4 or higher
- **Java**: 8 or higher (for Spark)

---

## Quick Install

### Using pip

```bash
pip install pysetl
```

### Using uv (recommended)

```bash
uv add pysetl
```

---

## Development Installation

For contributing to PySetl or running from source:

```bash
# Clone the repository
git clone https://github.com/JhossePaul/pysetl.git
cd pysetl

# Install Hatch (project manager)
pip install hatch

# Set up the development environment
hatch env create  # Creates the default environment
hatch env show    # See available scripts and environments
hatch shell       # Activate the development environment
```

---

## Environment Setup

PySetl uses Hatch for environment management. Available environments:

- **`default`**: Basic development tools (ruff, mypy, pytest, pre-commit, coverage)
- **`test`**: Testing environment with PySpark for integration tests
- **`docs`**: Documentation building tools
- **`security`**: Security scanning tools

```bash
# Activate default environment
hatch shell

# Or activate specific environment
hatch shell test
```
```

---

## Verification

Test your installation:

```python
import pysetl
from pysetl.workflow import Pipeline

# Should work without errors
pipeline = Pipeline()
print("PySetl installed successfully!")
```

---

## Development Commands

Once installed for development, you can use these commands:

```bash
# Code quality
hatch run type      # Type checking with mypy
hatch run lint      # Lint code with Ruff
hatch run format    # Format code with Ruff

# Testing
hatch test          # Run tests (default environment only)
hatch test --all    # Run all test matrix
hatch test --cover --all  # Run all tests with coverage

# Documentation
hatch run docs:docs # Build documentation
hatch run docs:serve # Serve documentation locally

# Security
hatch run security:all  # Run all security checks
```

---

## Troubleshooting

### Common Issues

1. **Import Error**: Make sure you're in the correct Python environment
2. **Spark Issues**: Ensure Java 8+ is installed and `JAVA_HOME` is set
3. **Type Checking Errors**: Run `hatch run type` to see detailed type issues

### Getting Help

- 📖 [User Guide](../user-guide/configuration.md) - Detailed configuration guide
- 🐛 [GitHub Issues](https://github.com/JhossePaul/pysetl/issues) - Report problems
- 💬 [Discussions](https://github.com/JhossePaul/pysetl/discussions) - Ask questions

---

## Next Steps

- 🚀 [QuickStart](quickstart.md) - Run your first PySetl pipeline
- 📚 [Examples](examples.md) - See real-world usage patterns
- 🔧 [Configuration](../user-guide/configuration.md) - Learn about PySetl configuration
