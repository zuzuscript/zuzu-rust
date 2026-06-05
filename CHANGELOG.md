# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project roughly adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
