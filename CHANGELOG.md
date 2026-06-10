# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project roughly adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Fixed

- A zero-parameter method whose entire body is a bare-identifier return
  (or a lone bare-identifier statement) was short-circuited as a trivial
  field getter even when the identifier named a module-level global, so
  `return SOME_GLOBAL;` from a method silently returned Null. The fast
  path now only applies when the identifier really is a field of the
  object.

## 0.3.0 - 2026-06-10

*stdlib tag 20260610, languagetests tag 20260610.*

### Added

- `--lint` option for detecting common antipatterns in Zuzu code.

### Changed

- Renamed dumped dot-member AST nodes to `MemberCallExpression` to reflect
  that `object.name` and `Class.name` are method calls, not member access.
- Bumped the crate version to 0.3.0.

### Fixed

- Rejected direct assignment, compound assignment, and increment/decrement
  updates targeting method or function calls, while still allowing assignment
  into collections returned from calls.
- Removed runtime support for writing object fields through dot syntax so
  `object.name` and `Class.name` cannot fall back to property access.

## 0.2.0 - 2026-06-08

*stdlib tag 20260608, languagetests tag 20260608.*

### Added

- Added the end-user README to packaged output.

### Changed

- Bumped the crate version to 0.2.0.
- Updated statement parsing so simple statements require semicolons unless
  they are final in a block or file.

### Fixed

- Added support for postfix `return if condition` and
  `return unless condition` statements.

## 0.1.1 - 2026-06-05

### Changed

- Bumped the crate version to 0.1.1.
- Updated the `docs/userguide` and `languagetests` submodules.

### Fixed

- Included source file paths in lex, parse, incomplete parse, and semantic
  diagnostics when source-aware parser entrypoints are used.
- Fixed imported module parse diagnostics to report the imported module path
  instead of only an anonymous line and column.
- Fixed function, method, lambda, and anonymous-function parameters so they
  are immutable like the Perl runtime's canonical behaviour.
- Fixed string and BinaryString indexing and slicing to support character and
  byte access consistently, including mutation.
- Added regression coverage for method-call statements using postfix `unless`
  with a membership condition.

## 0.1.0 - 2026-05-30

*First release.*
