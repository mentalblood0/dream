# 🧶 dream

[![tests](https://github.com/mentalblood0/dream/actions/workflows/tests.yml/badge.svg)](https://github.com/mentalblood0/dream/actions/workflows/tests.yml)

A Rust library for tagged object indexing on top of [lawn](https://github.com/mentalblood0/lawn) database engine

Implements advanced, multicursor-based algorithm which is highly efficient in terms of both RAM and CPU usage

The library was developed as [lawn](https://github.com/mentalblood0/lawn)-compatible and optimized replacement for [sonic](https://github.com/valeriansaliou/sonic) with following enhancements:

- pluggable library instead of hardcoded service, so **no forced text preprocessing, any tags can be set to any object**
- data itself must be stored by user, dream only stores 16-byte identifiers, yet it means it might be using **16-byte hashes instead of 4-byte ones** because 4-byte hashes collide on `77000` objects, and 16-byte ones practically do not collide at all (expected collision is on `1.8 * 10^19` objects)
- multicursor-based intersection algorithm, which uses the fact that key-value pairs are stored by database engine in sorted order and **does not retreive all the objects for each individual tag in search query** while [sonic](https://github.com/valeriansaliou/sonic) does just this: retrieves all the objects tagged with first tag in query, puts them in set, then retrieves all the objects tagged with second tag in query, puts them in another set, intersects second set with first, throws away second set and proceeds to third tag etc.

## Internal tables

- `tag_and_object<(Id, Id), ()>`: maps tag identifier to object identifier
- `object_and_tag<(Id, Id), ()>`: maps object identifier to tag identifier
- `object<Id, ()>>`: lists objects for faster iteration when no present tags provided
