# taterfs-rs

A storage model and CLI for archiving files in versioned repositories.  The storage defines no limits on file sizes, repository sizes, directory hierarchy depths or name lengths, etc.  The system emphasizes simplicity in its backend storage model, both to allow new backends to be created, and more importantly, to allow the filesystem structure to be easily deduced and recovered even in the absences of this software.

The CLI allows regions of the local filesystem to be sync'ed with portions of the repository file structure.  In typical usage, a single "sync" command uploads and downloads changes to and from the repository as needed (with special handling for conflicts).  The system does not expect an entire repository to be duplicated on any single filesystem.  Rather, it is expected that a user will typically treat a repository as a giant "everything" bucket, and only download and sync those portions that are relevant at the time.

## Status

This project is in early development.

## Prerequisites

- Rust (install via [rustup](https://rustup.rs/))

## Building

```bash
cargo build
```

## Running

```bash
cargo run
```

## Testing

```bash
cargo test
```

## Documentation

- [ARCHITECTURE.md](./ARCHITECTURE.md) - System design and components
- [PLANS.md](./PLANS.md) - Roadmap and planned features
- [DECISIONS.md](./DECISIONS.md) - Architecture decision records
- [HowToClaude.md](./HowToClaude.md) - Guide for working with Claude Code

## License

(TODO: Choose a license)
