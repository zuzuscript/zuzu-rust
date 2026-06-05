# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project roughly adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Included source file paths in lex, parse, incomplete parse, and semantic
  diagnostics when source-aware parser entrypoints are used.
- Fixed imported module parse diagnostics to report the imported module path
  instead of only an anonymous line and column.
- Added regression coverage for method-call statements using postfix `unless`
  with a membership condition.

## 0.1.0 - 2026-05-30

*First release.*
