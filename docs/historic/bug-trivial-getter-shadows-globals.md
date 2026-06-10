# Bug: `return GLOBAL;` from a zero-parameter method yields Null

**Status: FIXED** — `maybe_return_trivial_field_getter` now bails out
(falling through to the normal call path) when the identifier is not a
field of the object, exactly as suggested below. Regression test:
`languagetests/lang/oop/method-returns-global.zzs` /
`runs_method_returns_global_ztest_script` in `tests/runtime_basic.rs`.
This document is kept for historical reference.

## Summary

On zuzu-rust only, a class method whose entire body is a bare-identifier
return (or a bare-identifier expression as the only statement) returns
**Null** when that identifier names a module-level `const`/`let` rather
than an object field. zuzu.pl and zuzu-js return the global's value, and
the same statement inside a plain function works on all three runtimes.

## Reproduction

```zzs
const G := "hello";

class Foo {
	method broken () { return G; }          // Null on zuzu-rust
	method also_broken () { return (G); }   // parens don't help: same AST
	method fine_expr () { return G _ ""; }  // "hello"
	method fine_cond () { return G if true; } // "hello"
	method fine_local () {
		let out := G;                       // the usual workaround
		return out;
	}
}

function fine_fn () { return G; }           // "hello"

let got := ( new Foo() ).broken();
say( got == null ? "NULL" : got );          // NULL  (expected "hello")
```

The failure is silent and surfaces far from the cause — for example, a
method returning a global Dict yields Null, and the caller later sees
`unsupported method 'exists' for Null` or similar.

## Affected / not affected

| Method body | Result |
|---|---|
| `return MODULE_GLOBAL;` | **Null** (bug) |
| `return (MODULE_GLOBAL);` | **Null** (bug — parens produce the same Identifier node) |
| `MODULE_GLOBAL;` as the only statement (implicit return) | **Null** (bug) |
| `return field_name;` | correct (this is the case the fast path is for) |
| `return param;` | correct (methods with parameters skip the fast path) |
| `return MODULE_GLOBAL _ "";` or any compound expression | correct |
| `return MODULE_GLOBAL if cond;` | correct (PostfixConditionalStatement) |
| `let out := MODULE_GLOBAL; return out;` | correct (two statements) |
| same `return MODULE_GLOBAL;` in a plain `function` | correct |

## Cause

`maybe_return_trivial_field_getter` in `src/runtime.rs` (~line 862) is a
fast path for trivial getters. When a zero-parameter method's body is a
single `return <Identifier>;` (or a single bare `<Identifier>;`
expression statement), it skips the normal call machinery and reads the
identifier straight out of the object's field map:

```rust
let value = object
    .borrow()
    .fields
    .get(field_name)
    .cloned()
    .unwrap_or(Value::Null);   // <-- not a field => silently Null
```

The fast path never checks that the identifier actually *is* a field, so
any other identifier in scope — module globals in practice — is
swallowed and replaced by `Value::Null` instead of being resolved
through the method's environment chain.

(`call_method_value` invokes this at ~line 5362 before falling back to
the full `call_function` path.)

## Suggested fix

Treat "identifier is not a field of this object" as "this is not a
trivial field getter" and fall through to the normal call path, which
resolves the identifier through the environment chain correctly:

```rust
let value = match object.borrow().fields.get(field_name) {
    Some(value) => value.clone(),
    None => return None,   // not a field getter; use the full call path
};
Some(Ok(if value.is_weak_value() {
    value.resolve_weak_value()
} else {
    value
}))
```

This keeps the optimisation for genuine getters (declared fields are
always present in `fields` once the object is constructed) and only
costs the fast path in the case it was wrong anyway.

A regression test should cover: bare `return GLOBAL;` from a method,
the implicit-return form, and the parenthesised form, for both `const`
and `let` module globals.

## Workaround (for code that must run on current zuzu-rust)

Copy the global into a local first:

```zzs
method terms () {
	let out := RDFA_INITIAL_TERMS;
	return out;
}
```

Used in `tobyink-dists/rdf-rdfa` (`rdf/parser/rdfa_core.zzm` and the
host modules), tagged with comments mentioning the
"zuzu-rust bare-return-global bug".
