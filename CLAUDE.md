# CLAUDE.md

This file provides guidance to Claude Code (or other AI assistants) when working on this project.

## Project Overview

taterfs-rs is a Rust command-line utility. (TODO: Add specific description once defined)

## Build Commands

```bash
cargo build          # Debug build
cargo build --release # Release build
cargo run            # Run the application
cargo test           # Run tests
cargo clippy         # Run linter
cargo fmt            # Format code
```

## Project Structure

```
src/
  main.rs           # Application entry point
Cargo.toml          # Package manifest
```

## Coding Conventions

- Follow standard Rust idioms and the Rust API Guidelines
- Use `rustfmt` for formatting (default settings)
- Use `clippy` for linting - address all warnings
- Prefer `Result` and `?` operator for error handling
- Write tests for new functionality

## Dependencies

When adding dependencies, prefer well-maintained crates with minimal transitive dependencies.

## Related Documentation

- [ARCHITECTURE.md](./ARCHITECTURE.md) - System design and component overview
- [PLANS.md](./PLANS.md) - Roadmap and planned features
- [DECISIONS.md](./DECISIONS.md) - Architecture decision records
