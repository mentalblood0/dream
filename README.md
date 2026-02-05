# Dream

A tagged object indexing library with support for efficient tag-based searches, written in Rust.

## Overview

Dream provides an indexed storage system for objects with tags, enabling fast searches for objects that have certain tags and lack other tags. It uses [lawn](https://github.com/mentalblood0/lawn) as the underlying database for persistence.

## Key Features

- **Tag-based indexing**: Objects can be tagged and searched by tag combinations
- **Efficient searches**: Optimized query performance for common search patterns
- **Persistence**: All data is persisted using the lawn database
- **Transaction support**: Read and write transactions for safe concurrent access

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
dream = { path = "https://github.com/mentalblood0/dream" }
```

## Core Concepts

### Objects

Objects are the main entities stored in the index. They can be either raw byte data or pre-identified objects:

```rust
use dream::Object;

// Raw object - ID will be computed from content hash
let raw_object = Object::Raw(b"data".to_vec());

// Identified object with explicit ID
let identified_object = Object::Identified(id);
```

### Tags

Tags are used to categorize and filter objects. Any object can be used as a tag:

```rust
let tag = Object::Raw(b"category".to_vec());
```

### IDs

All objects and tags are uniquely identified by a 16-byte ID computed using xxh3-128 hashing:

```rust
let object = Object::Raw(b"content".to_vec());
let id = object.get_id(); // xxh3-128 hash of the content
```

## Architecture

### Internal Tables

The library maintains several internal tables:

- `tag_and_object<(Id, Id), ()>`: Maps tag IDs to object IDs
- `object_and_tag<(Id, Id), ()>`: Maps object IDs to tag IDs
- `id_to_source<Id, Vec<u8>>`: Stores raw data for objects
- `tag_to_objects_count<Id, u32>`: Caches tag occurrence counts
- `object_to_tags_count<Id, u32>`: Caches object tag counts

### Search Optimization

The search implementation optimizes for different scenarios:

- **0 present tags**: Scans all objects, filters by absent tags
- **1 present tag**: Efficient tag-based lookup with absent tag filtering
- **2+ present tags**: Uses cursor-based intersection algorithm for multi-tag intersection

## Testing

Run tests with:

```bash
cargo test
```

## Benchmarking

Run benchmarks with:

```bash
cargo bench
```

## License

This project is licensed under the MIT License.
