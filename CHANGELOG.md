# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project roughly adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- `switch` case values can now override the switch comparator with a
  comparison operator prefix, such as `case ~ /^Rob/` or `case eqi "Bob"`.
  O3 switch indexing now also optimizes literal-string `eq`/`eqi` dispatch
  tables and literal-integer numeric equality tables.
- Added shared conformance coverage for dynamic method calls whose method
  expression evaluates to either a method name or a Method value, including
  named arguments and `std/eval`.

### Fixed

- Bare wordlike named-argument keys such as `length: 42` now parse like
  the same unquoted keys in Dict and PairList literals.

## 0.4.0 - 2026-06-12

*stdlib tag 20260612, languagetests tag 20260612.*

### Changed

- Booleans render as `true`/`false` (in `say`, string interpolation, and
  inside rendered collections) instead of `1`/`0`, matching zuzu-perl.
  String coercion via operators already used `true`/`false`.

### Added

- std/net/url's `fill_template` is now a complete RFC 6570 URI
  Template implementation (all operators, `:N` prefix and `*` explode
  modifiers, list and associative values), implemented with the
  std-uritemplate crate and validated against the official URI
  Template test suite. Invalid templates throw; Dict keys expand in
  sorted order.
- New divisibility operators: `a ∣ b` (U+2223; ASCII alias `divides`,
  a new keyword) is a Boolean test that the left operand exactly
  divides the right; `a ∤ b` (U+2224, no ASCII alias) returns the
  Number `b mod a`, truthy exactly when the left operand does not
  divide the right. Both coerce operands to Number and sit at the
  comparison precedence tier.
- `for` loops (including postfix `for`) iterate over the characters of a
  String (each a 1-character String) and the bytes of a BinaryString
  (each a 1-byte BinaryString).
- Bitshift operators `<<`, `>>`, `«`, `»`. Numbers shift arithmetically
  (operands truncated to integers; negative shift counts throw).
  BinaryStrings shift as one whole bit string: bits carry across byte
  boundaries, length is preserved, vacated bits are zero. Shifts bind
  tighter than bitwise `&`/`|`/`^` and looser than additive operators;
  inside a set literal the closing `>>`/`»` still terminates the
  literal.
- Numeric literals: uppercase-E exponents (`1E3`, `2.5E-7`), hex
  (`0x1F`), binary (`0b1111`), and octal (`0o100`) with lowercase
  prefixes. Lowercase `1e3` and uppercase `0X`/`0B`/`0O` prefixes remain
  invalid in source, but String-to-Number coercion accepts either case
  for the exponent marker and radix prefixes.
- New `std/string/encode` module: `encode(String, encoding)` /
  `decode(BinaryString, encoding)` with UTF-8, UTF-16, UTF-32, and
  ISO-8859-1 codecs plus `ENCODING_UTF8`/`ENCODING_UTF16`/
  `ENCODING_UTF32`/`ENCODING_LATIN` constants. Encoding names match
  case-insensitively; UTF-16/UTF-32 encode big-endian without a BOM and
  decode honours a leading BOM.
- `std/string` exports `to_binary` and `to_string`.

### Fixed

- `std/math/bignum` now returns String values from `BigNum.to_dec` and
  `BigNum.to_String` consistently; use `BigNum.to_Number` for numeric
  conversion.
- A zero-parameter method whose entire body is a bare-identifier return
  (or a lone bare-identifier statement) was short-circuited as a trivial
  field getter even when the identifier named a module-level global, so
  `return SOME_GLOBAL;` from a method silently returned Null. The fast
  path now only applies when the identifier really is a field of the
  object.
- `Time` now restores missing `std/time` instance methods (`yy`, `day_of_week`,
  `day_of_year`, `month_last_day`, `hms`, `ymd`, `mdy`, `dmy`, `date`,
  `time`, `cdate`, `tzoffset`, `is_leap_year`, `week`, `week_year`,
  `julian_day`) and aligns weekday/month/day formatting with `zuzu-perl`,
  closing a longstanding bug report.
- Added runtime regression coverage for `Time` instance method availability in
  [runtime_basic.rs](tests/runtime_basic.rs).
- BigNum arithmetic now uses arbitrary-precision decimal math instead of `f64`,
  so `BigNum.bpow` can produce large integer results like `10 ** 1000`
  instead of `inf`. Added regression coverage for large-exponent `bpow` in
  [stdlib/tests/std/math/bignum.zzs](stdlib/tests/std/math/bignum.zzs).
- Time parser/runtime coverage now includes `day`, `day_of_week`, and
  `day_of_year` regressions in [stdlib/tests/std/time.zzs](stdlib/tests/std/time.zzs),
  and `BigNum.bpow` now uses integer arithmetic when both operands are integers.
- Updated runtime parity notes for `std/math/bignum` so cross-runtime test
  expectations (including `zuzu-js` BigNum method-calling behaviour and
  large-exponent checks) remain documented in lockstep.

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
