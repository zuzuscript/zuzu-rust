use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::str::FromStr;

use bigdecimal::BigDecimal;
use num_bigint::BigInt;
use num_traits::{ToPrimitive, Zero};

use super::super::{
	FieldSpec, MethodValue, ObjectValue, Runtime, TraitValue, UserClassValue, Value,
};
use crate::error::{Result, ZuzuRustError};

pub(super) fn exports() -> HashMap<String, Value> {
	HashMap::from([(
		"BigNum".to_owned(),
		Value::builtin_class("BigNum".to_owned()),
	)])
}

pub(super) fn call_class_method(
	runtime: &Runtime,
	class_name: &str,
	name: &str,
	args: &[Value],
) -> Option<Result<Value>> {
	if class_name != "BigNum" {
		return None;
	}
	Some(match name {
		"from_dec" => make_from_dec(runtime, args),
		"from_hex" => make_from_hex(runtime, args),
		_ => return None,
	})
}

pub(super) fn call_object_method(
	_runtime: &Runtime,
	class_name: &str,
	builtin_value: &Value,
	name: &str,
	args: &[Value],
) -> Option<Result<Value>> {
	if class_name != "BigNum" {
		return None;
	}
	let Value::Dict(fields) = builtin_value else {
		return Some(Err(ZuzuRustError::runtime("BigNum internal state missing")));
	};
	let value = fields
		.get("value")
		.and_then(parse_decimal_value)
		.unwrap_or_else(BigDecimal::zero);
	let text = fields
		.get("text")
		.map(render_string)
		.unwrap_or_else(|| value.to_string());
	let is_int = matches!(fields.get("is_int"), Some(Value::Boolean(is_int)) if *is_int);

	Some(match name {
		"is_int" => Ok(Value::Boolean(is_int)),
		"bcmp" => compare_to(value, args),
		"beq" => compare_bool(value, args, |cmp| cmp == 0),
		"bne" => compare_bool(value, args, |cmp| cmp != 0),
		"blt" => compare_bool(value, args, |cmp| cmp < 0),
		"ble" => compare_bool(value, args, |cmp| cmp <= 0),
		"bgt" => compare_bool(value, args, |cmp| cmp > 0),
		"bge" => compare_bool(value, args, |cmp| cmp >= 0),
		"babs" => Ok(make_bignum(value.abs(), is_int)),
		"bneg" => Ok(make_bignum(-value, is_int)),
		"binv" => Ok(make_bignum_auto(BigDecimal::from_f64(1.0 / to_f64(&value)).unwrap_or_default())),
		"bsin" => Ok(make_bignum_auto(BigDecimal::from_f64(to_f64(&value).sin()).unwrap_or_default())),
		"bcos" => Ok(make_bignum_auto(BigDecimal::from_f64(to_f64(&value).cos()).unwrap_or_default())),
		"btan" => Ok(make_bignum_auto(BigDecimal::from_f64(to_f64(&value).tan()).unwrap_or_default())),
		"bsqrt" => Ok(make_bignum_auto(BigDecimal::from_f64(to_f64(&value).sqrt()).unwrap_or_default())),
		"bround" => Ok(make_bignum_auto(BigDecimal::from_f64(to_f64(&value).round()).unwrap_or_default())),
		"bfloor" => Ok(make_bignum_auto(BigDecimal::from_f64(to_f64(&value).floor()).unwrap_or_default())),
		"bceil" => Ok(make_bignum_auto(BigDecimal::from_f64(to_f64(&value).ceil()).unwrap_or_default())),
		"badd" => binary_num(value, args, |left, right| left + right, Some(false)),
		"bsub" => binary_num(value, args, |left, right| left - right, Some(false)),
		"bmul" => binary_num(value, args, |left, right| left * right, Some(false)),
		"bdiv" => binary_num(value, args, |left, right| left / right, None),
		"bmod" => binary_num(value, args, |left, right| left % right, None),
		"bpow" => Ok(pow_bignum(value, args)),
		"to_hex" => Ok(Value::String(to_hex(&value, &text))),
		"to_dec" => Ok(to_dec_value(&value, is_int, &text)),
		"to_String" => Ok(Value::String(trim_decimal(&text))),
		"to_Number" => Ok(Value::Number(to_f64(&value))),
		_ => return None,
	})
}

pub(super) fn has_builtin_object_method(class_name: &str, name: &str) -> bool {
	class_name == "BigNum"
		&& matches!(
			name,
			"is_int"
				| "bcmp"
				| "beq"
				| "bne"
				| "blt"
				| "ble"
				| "bgt"
				| "bge"
				| "babs"
				| "bneg"
				| "binv"
				| "bsin"
				| "bcos"
				| "btan"
				| "bsqrt"
				| "bround"
				| "bfloor"
				| "bceil"
				| "badd"
				| "bsub"
				| "bmul"
				| "bdiv"
				| "bmod"
				| "bpow"
				| "to_hex"
				| "to_dec"
				| "to_String"
				| "to_Number"
		)
}

fn class() -> Rc<UserClassValue> {
	Rc::new(UserClassValue {
		name: "BigNum".to_owned(),
		base: None,
		traits: Vec::<Rc<TraitValue>>::new(),
		fields: vec![
			FieldSpec {
				name: "value".to_owned(),
				declared_type: Some("Number".to_owned()),
				mutable: true,
				accessors: Vec::new(),
				default_value: None,
				is_weak_storage: false,
			},
			FieldSpec {
				name: "text".to_owned(),
				declared_type: Some("String".to_owned()),
				mutable: true,
				accessors: Vec::new(),
				default_value: None,
				is_weak_storage: false,
			},
			FieldSpec {
				name: "is_int".to_owned(),
				declared_type: Some("Boolean".to_owned()),
				mutable: true,
				accessors: Vec::new(),
				default_value: None,
				is_weak_storage: false,
			},
		],
		methods: HashMap::<String, Rc<MethodValue>>::new(),
		static_methods: HashMap::<String, Rc<MethodValue>>::new(),
		nested_classes: HashMap::new(),
		source_decl: None,
		closure_env: None,
	})
}

fn make_from_dec(runtime: &Runtime, args: &[Value]) -> Result<Value> {
	let text = args
		.first()
		.map(|value| runtime.render_value(value))
		.transpose()?
		.unwrap_or_else(|| "0".to_owned());
	let parsed = text
		.trim()
		.parse::<BigDecimal>()
		.unwrap_or_else(|_| BigDecimal::zero());
	let normalized = make_decimal_text(&parsed);
	Ok(make_bignum(parsed, !has_fractional_part(&normalized)))
}

fn make_from_hex(runtime: &Runtime, args: &[Value]) -> Result<Value> {
	let text = args
		.first()
		.map(|value| runtime.render_value(value))
		.transpose()?
		.unwrap_or_else(|| "0".to_owned());
	let text = text.trim();
	let is_negative = text.starts_with('-');
	let trimmed = text
		.trim_start_matches(&['+', '-'][..])
		.trim_start_matches("0x")
		.trim_start_matches("0X");
	let parsed = BigInt::parse_bytes(trimmed.as_bytes(), 16).unwrap_or_else(BigInt::zero);
	let parsed = if is_negative { -parsed } else { parsed };
	Ok(make_bignum(BigDecimal::from(parsed), true))
}

fn make_bignum(value: BigDecimal, is_int: bool) -> Value {
	let text = make_decimal_text(&value);
	let fields = HashMap::from([
		("value".to_owned(), Value::String(text.clone())),
		("text".to_owned(), Value::String(text.clone())),
		("is_int".to_owned(), Value::Boolean(is_int)),
	]);
	Value::Object(Rc::new(RefCell::new(ObjectValue {
		class: class(),
		fields: fields.clone(),
		weak_fields: std::collections::HashSet::new(),
		builtin_value: Some(Value::Dict(fields)),
	})))
}

fn binary_num(
	left: BigDecimal,
	args: &[Value],
	op: impl FnOnce(BigDecimal, BigDecimal) -> BigDecimal,
	is_int: Option<bool>,
) -> Result<Value> {
	let right = args
		.first()
		.map(coerce_other)
		.transpose()?
		.unwrap_or_else(BigDecimal::zero);
	let value = op(left, right);
	Ok(match is_int {
		Some(is_int) => make_bignum(value, is_int),
		None => make_bignum_auto(value),
	})
}

fn compare_to(left: BigDecimal, args: &[Value]) -> Result<Value> {
	let right = args
		.first()
		.map(coerce_other)
		.transpose()?
		.unwrap_or_else(BigDecimal::zero);
	Ok(Value::Number(if left < right {
		-1.0
	} else if left > right {
		1.0
	} else {
		0.0
	}))
}

fn compare_bool(left: BigDecimal, args: &[Value], test: impl FnOnce(i32) -> bool) -> Result<Value> {
	let right = args
		.first()
		.map(coerce_other)
		.transpose()?
		.unwrap_or_else(BigDecimal::zero);
	let cmp = if left < right {
		-1
	} else if left > right {
		1
	} else {
		0
	};
	Ok(Value::Boolean(test(cmp)))
}

fn coerce_other(value: &Value) -> Result<BigDecimal> {
	match value {
		Value::Object(object) if object.borrow().class.name == "BigNum" => Ok(object
			.borrow()
			.fields
			.get("value")
			.and_then(parse_decimal_value)
			.unwrap_or_else(BigDecimal::zero)),
		other => {
			let number = other.to_number()?;
			BigDecimal::from_f64(number).ok_or_else(|| {
				ZuzuRustError::runtime("cannot represent Number as BigNum")
			})
		}
	}
}

fn render_string(value: &Value) -> String {
	match value {
		Value::String(value) => value.clone(),
		Value::Number(value) => value.to_string(),
		other => other.render(),
	}
}

fn trim_decimal(text: &str) -> String {
	let text = text.trim();
	if text.contains('.') {
		text.trim_end_matches('0').trim_end_matches('.').to_owned()
	} else {
		text.to_owned()
	}
}

fn make_bignum_auto(value: BigDecimal) -> Value {
	let is_int = !has_fractional_part(&make_decimal_text(&value));
	make_bignum(value, is_int)
}

fn make_decimal_text(value: &BigDecimal) -> String {
	let text = value.to_string();
	if text.contains('E') || text.contains('e') {
		trim_decimal(&expand_scientific_notation(&text))
	} else {
		trim_decimal(&text)
	}
}

fn has_fractional_part(text: &str) -> bool {
	let text = text.trim().trim_start_matches(&['+', '-'][..]);
	let Some((_, fractional)) = text.split_once('.') else {
		return false;
	};
	!fractional.trim_end_matches('0').is_empty()
}

fn to_f64(value: &BigDecimal) -> f64 {
	value.to_f64().unwrap_or_else(|| {
		if value < &BigDecimal::zero() {
			f64::NEG_INFINITY
		} else {
			f64::INFINITY
		}
	})
}

fn to_hex(value: &BigDecimal, text: &str) -> String {
	let decimal = make_decimal_text(value);
	let integer = decimal.split_once('.').map_or(decimal.as_str(), |(integer, _)| integer);
	let parsed = BigInt::from_str(integer).or_else(|_| BigInt::from_str(text)).unwrap_or_else(|_| {
		BigInt::zero()
	});
	if parsed.is_zero() {
		"0x0".to_owned()
	} else if parsed < BigInt::zero() {
		format!("-0x{:x}", (-parsed).to_str_radix(16))
	} else {
		format!("0x{:x}", parsed.to_str_radix(16))
	}
}

fn to_dec_value(value: &BigDecimal, is_int: bool, text: &str) -> Value {
	if is_int {
		if let Some(number) = value.to_f64() {
			Value::Number(number)
		} else {
			Value::String(trim_decimal(text))
		}
	} else {
		Value::String(trim_decimal(text))
	}
}

fn parse_decimal_value(value: &Value) -> Option<BigDecimal> {
	match value {
		Value::String(value) => value.parse::<BigDecimal>().ok(),
		Value::Number(value) => BigDecimal::from_f64(*value),
		_ => None,
	}
}

fn pow_bignum(left: BigDecimal, args: &[Value]) -> Value {
	let right = args
		.first()
		.map(coerce_other)
		.transpose()
		.unwrap_or_else(|_| Ok(BigDecimal::zero()))
		.unwrap_or_else(BigDecimal::zero);
	if is_integer_text(&make_decimal_text(&right)) {
		if let Some(exp) = right.to_u64() {
			return make_bignum(integer_pow(left, exp), is_integer_text(&make_decimal_text(&left)));
		}
	}

	let value = to_f64(&left).powf(to_f64(&right));
	make_bignum_auto(BigDecimal::from_f64(value).unwrap_or_default())
}

fn is_integer_text(text: &str) -> bool {
	!has_fractional_part(text)
}

fn integer_pow(mut base: BigDecimal, mut exp: u64) -> BigDecimal {
	let mut result = BigDecimal::from(1_i64);
	while exp > 0 {
		if (exp & 1) == 1 {
			result *= base.clone();
		}
		exp >>= 1;
		if exp > 0 {
			base *= base.clone();
		}
	}
	result
}

fn expand_scientific_notation(text: &str) -> String {
	let mut parts = text.splitn(2, |c| c == 'e' || c == 'E');
	let coefficient = parts.next().unwrap_or_default();
	let exponent = parts
		.next()
		.and_then(|value| value.parse::<isize>().ok())
		.unwrap_or(0);
	let negative = coefficient.starts_with('-');
	let coefficient = coefficient.trim_start_matches(&['+', '-'][..]);
	let mut pieces = coefficient.splitn(2, '.');
	let integer_part = pieces.next().unwrap_or_default();
	let fractional_part = pieces.next().unwrap_or_default();
	let mut digits = format!("{}{}", integer_part, fractional_part);
	if digits.is_empty() {
		digits.push('0');
	}

	let point = integer_part.len() as isize;
	let shifted_point = point + exponent;
	let mut body = if shifted_point <= 0 {
		let mut rendered = String::from("0.");
		rendered.extend(std::iter::repeat('0').take((-shifted_point) as usize));
		rendered.push_str(&digits);
		rendered
	} else if shifted_point >= digits.len() as isize {
		let mut rendered = digits;
		rendered.extend(std::iter::repeat('0').take((shifted_point as usize) - rendered.len()));
		rendered
	} else {
		let mut rendered = String::new();
		rendered.push_str(&digits[..shifted_point as usize]);
		rendered.push('.');
		rendered.push_str(&digits[shifted_point as usize..]);
		rendered
	};
	if !body.contains('.') {
		let trimmed = body.trim_start_matches('0');
		body = if trimmed.is_empty() {
			"0".to_owned()
		} else {
			trimmed.to_owned()
		};
	}

	if negative {
		format!("-{}", body)
	} else {
		body
	}
}
