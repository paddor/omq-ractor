# Changelog

## 0.1.3

### Added

- **`data:` keyword for `OMQ::Ractor.new`** — pass an arbitrary
  Ractor-shareable object into the worker block, accessible as `omq.data`.
  This is the supported way to pass configuration under Ruby 4.0's strict
  Ractor isolation, which forbids closing over outer variables.

## 0.1.2

### Added

- **`SocketSet#socket_for(port)`** — maps a `Ractor::Port` back to its
  `SocketProxy` after `Ractor.select`. Replaces manual `port == a.to_port`
  comparisons.
- **`bundler/gem_tasks`** in Rakefile — enables `rake build` and `rake release`
  for the gem release workflow.
- **README badges** — CI, gem version, license, Ruby version.

### Fixed

- **README examples handle nil on close** — all loop examples now use
  `while msg = proxy.receive` instead of bare `loop do` to avoid passing
  nil to processing functions when the socket closes.
- **`Ractor.select` example** — documents the `[port, value]` return,
  shows nil check, explains topic stripping bypass, and demonstrates
  `socket_for` for port→proxy lookup.

## 0.1.1

Initial release.
