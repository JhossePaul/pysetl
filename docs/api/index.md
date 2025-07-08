# API Reference Overview

Welcome to the PySetl API Reference! This section provides comprehensive documentation for all PySetl modules and their components.

## 📚 API Structure

PySetl is organized into several core modules, each serving a specific purpose in the ETL workflow:

### Core Modules

- **`pysetl`** - Main package and entry points
- **`pysetl.workflow`** - ETL workflow orchestration and execution
- **`pysetl.storage`** - Data storage and retrieval abstractions
- **`pysetl.config`** - Configuration management
- **`pysetl.enums`** - Enumerations for various PySetl components
- **`pysetl.utils`** - Utility functions and helpers

## 🎯 How to Use This Documentation

### For New Users
Start with the **User Guide** sections to understand PySetl concepts before diving into the API reference.

### For Developers
- **Class Documentation**: Each class includes detailed method descriptions, parameters, and examples
- **Type Information**: All methods include type hints for better IDE support
- **Code Examples**: Most classes include usage examples
- **Source Links**: Click "View source" to see the actual implementation

### Navigation Tips
- Use the **search functionality** to quickly find specific classes or methods
- The **right sidebar** shows the current page's table of contents for quick navigation within a module.
- **Breadcrumbs** help you understand your current location in the documentation.

## 🔍 Quick Reference

### Most Important Classes

| Class | Module | Purpose |
|-------|----------------|---------|
| `Pipeline` | `pysetl.workflow` | Main orchestrator for ETL workflows |
| `Factory` | `pysetl.workflow` | Core abstraction for ETL logic |
| `Stage` | `pysetl.workflow` | Sequential execution units |
| `Delivery` | `pysetl.workflow` | Declarative dependency injection |
| `Deliverable` | `pysetl.workflow` | Runtime data containers |
| `FileConnector` | `pysetl.storage.connector` | Handles file-based data I/O |
| `ConfigBuilder` | `pysetl.config` | Builds and validates configurations |

## 📖 Module Documentation

Click on any module below to explore its API:

  - **[pysetl](https://www.google.com/search?q=pysetl.md)** - Main package and entry points
  - **[pysetl.workflow](https://www.google.com/search?q=workflow.md)** - ETL workflow orchestration
  - **[pysetl.storage](https://www.google.com/search?q=storage.md)** - Data storage abstractions
  - **[pysetl.config](https://www.google.com/search?q=config.md)** - Configuration management
  - **[pysetl.enums](https://www.google.com/search?q=enums.md)** - Enumerations for various PySetl components
  - **[pysetl.utils](https://www.google.com/search?q=utils.md)** - Utility functions

## 🚀 Getting Help

  - **GitHub Issues**: Report bugs or request features
  - **Discussions**: Ask questions and share ideas
  - **Examples**: Check the examples directory for working code
  - **User Guide**: Start here if you're new to PySetl

<!-- end list -->
