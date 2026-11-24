# Contributing

## Testing

Run the test suite:

```bash
cargo test
```

## Releases

Onwards is not published to crates.io. It stays a crate in this workspace and
is consumed as a path dependency, so there is no crate version or release PR to
prepare for it.

Every push to `main` builds the standalone image as
`ghcr.io/doublewordai/onwards:sha-<commit>`. The `main` and `latest` tags move
to the commit that is `main`'s head. A pull request from this repository that
runs backend CI builds the same image from its head commit and tags it with
that commit's SHA.

The public repository gets one versioned export for the whole workspace on a
weekday schedule. Its version comes from the conventional commit messages since
the previous export: a `feat` bumps the minor; a `!` title marker or a
`BREAKING CHANGE:` footer bumps the major, and every other commit bumps the
patch. `release:` commits are ignored.
