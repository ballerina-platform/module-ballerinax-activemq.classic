# Changelog

This file contains all the notable changes done to the Ballerina ActiveMQ Classic package through the releases.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Added an `examples/` directory with three runnable examples: sending to a queue, pull-receiving
  from a queue, and consuming via a `Listener`/`Service`.
- Extended typed payload data binding to `Listener`/`Service`'s `onMessage`: its parameter can now
  narrow `Message`'s `payload` field to a specific type (e.g.
  `record {|*classic:Message; string payload;|}`), the same way `MessageConsumer.receive()`
  already worked. A plain `classic:Message` parameter keeps its existing behavior unchanged. A
  payload that can't be converted to the requested type is routed to `onError` instead of being
  silently dropped.

### Changed
- Moved the test-only `TestProducer` native helper out of the released `activemq.classic-native` jar into a
  separate `testOnly`-scoped jar, so test utilities no longer ship in the production artifact.

### Removed
- Removed the unused, auto-generated `gradle/libs.versions.toml` stub.

### Fixed
- Fixed `PrefetchPolicy` being completely non-functional: configuring it at all, even with every
  field set, always crashed with a `NullPointerException` due to 3 of its 4 fields being looked up
  under the wrong native key names.
- Fixed `clientID` silently being a no-op: a case-mismatch in the native lookup key meant an
  explicitly configured `clientID` was always discarded in favor of a random UUID.
