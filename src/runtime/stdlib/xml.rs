use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::rc::Rc;

use libxml::parser::{Parser, ParserOptions};
use libxml::tree::{Document as LibxmlDocument, Node as LibxmlNode, NodeType, SaveOptions};
use libxml::xpath::Context as XPathContext;

use super::super::{
    FieldSpec, MethodValue, ObjectValue, Runtime, TraitValue, UserClassValue, Value,
};
use super::io::{path_buf_from_value, resolve_fs_path};
use crate::error::{Result, ZuzuRustError};

#[derive(Clone, Copy, PartialEq, Eq)]
enum XmlKind {
    Document,
    Element,
    Text,
    Comment,
}

struct ParsedNode {
    kind: XmlKind,
    name: String,
    text: String,
    attrs: Vec<(String, String)>,
    parent: Option<usize>,
    children: Vec<usize>,
}

pub(super) fn exports() -> HashMap<String, Value> {
    let mut exports = HashMap::new();
    exports.insert("XML".to_owned(), Value::builtin_class("XML".to_owned()));
    exports.insert(
        "XMLDocument".to_owned(),
        Value::builtin_class("XMLDocument".to_owned()),
    );
    exports.insert(
        "XMLNode".to_owned(),
        Value::builtin_class("XMLNode".to_owned()),
    );
    exports.insert(
        "DOMNode".to_owned(),
        Value::builtin_class("DOMNode".to_owned()),
    );
    exports.insert(
        "DOMElement".to_owned(),
        Value::builtin_class("DOMElement".to_owned()),
    );
    exports.insert(
        "DOMAttr".to_owned(),
        Value::builtin_class("DOMAttr".to_owned()),
    );
    exports.insert(
        "DOMComment".to_owned(),
        Value::builtin_class("DOMComment".to_owned()),
    );
    exports.insert(
        "DOMText".to_owned(),
        Value::builtin_class("DOMText".to_owned()),
    );
    exports.insert(
        "DOMDocument".to_owned(),
        Value::builtin_class("DOMDocument".to_owned()),
    );
    exports
}

pub(super) fn call_class_method(
    runtime: &Runtime,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    if class_name != "XML" {
        return None;
    }
    let value = match name {
        "parse" => {
            let text = match args.first() {
                Some(value) => match runtime.render_value(value) {
                    Ok(text) => text,
                    Err(err) => return Some(Err(err)),
                },
                None => String::new(),
            };
            parse_xml_document(&text)
        }
        "load" => {
            if runtime.is_effectively_denied("fs") {
                return Some(Err(ZuzuRustError::thrown(
                    "XML.load is denied by runtime policy",
                )));
            }
            let Some(target) = args.first() else {
                return Some(Err(ZuzuRustError::thrown(
                    "TypeException: XML.load expects Path as first argument",
                )));
            };
            let path = match target {
                Value::Object(object) if object.borrow().class.name == "Path" => {
                    resolve_fs_path(runtime, &path_buf_from_value(target))
                }
                _ => {
                    return Some(Err(ZuzuRustError::thrown(
                        "TypeException: XML.load expects Path as first argument",
                    )))
                }
            };
            match fs::read_to_string(path) {
                Ok(text) => parse_xml_document(&text),
                Err(err) => Err(ZuzuRustError::thrown(format!("XML.load failed: {err}"))),
            }
        }
        "dump" => {
            if args.len() < 2 {
                return Some(Err(ZuzuRustError::runtime(
                    "XML.dump() expects a Path and an XML document/node",
                )));
            }
            let path = match &args[0] {
                Value::Object(object) if object.borrow().class.name == "Path" => {
                    resolve_fs_path(runtime, &path_buf_from_value(&args[0]))
                }
                _ => {
                    return Some(Err(ZuzuRustError::thrown(
                        "TypeException: XML.dump expects Path as first argument",
                    )))
                }
            };
            let text = serialize_value(&args[1]);
            match fs::write(path, text) {
                Ok(()) => Ok(args[0].clone()),
                Err(err) => Err(ZuzuRustError::thrown(format!("XML.dump failed: {err}"))),
            }
        }
        _ => return None,
    };
    Some(value)
}

pub(super) fn call_object_method(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    class_name: &str,
    _builtin_value: &Value,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    let value = match class_name {
        "XMLDocument" | "DOMDocument" => Some(call_document_method(runtime, object, name, args)),
        "DOMElement" | "DOMText" | "DOMComment" | "DOMAttr" | "XMLNode" | "DOMNode" => {
            Some(call_node_method(runtime, object, name, args))
        }
        _ => None,
    };
    value
}

pub(super) fn has_builtin_object_method(class_name: &str, name: &str) -> bool {
    match class_name {
        "XMLDocument" | "DOMDocument" => has_document_method(name),
        "DOMElement" | "DOMText" | "DOMComment" | "DOMAttr" | "XMLNode" | "DOMNode" => {
            has_node_method(name)
        }
        _ => false,
    }
}

fn has_document_method(name: &str) -> bool {
    matches!(
        name,
        "documentElement"
            | "nodeName"
            | "nodeType"
            | "nodeKind"
            | "childNodes"
            | "children"
            | "hasChildNodes"
            | "createElement"
            | "createComment"
            | "createTextNode"
            | "createCDATASection"
            | "findnodes"
            | "findvalue"
            | "getElementsByTagName"
            | "querySelectorAll"
            | "querySelector"
            | "visitEach"
            | "findFirst"
            | "toXML"
            | "to_String"
    )
}

fn has_node_method(name: &str) -> bool {
    matches!(
        name,
        "nodeName"
            | "tagName"
            | "localName"
            | "namespaceURI"
            | "nodeType"
            | "nodeKind"
            | "unique_id"
            | "uniqueKey"
            | "childNodes"
            | "children"
            | "hasChildNodes"
            | "attributes"
            | "nextSibling"
            | "previousSibling"
            | "parentNode"
            | "ownerDocument"
            | "textContent"
            | "data"
            | "nodeValue"
            | "id"
            | "setId"
            | "getAttribute"
            | "hasAttribute"
            | "setAttribute"
            | "removeAttribute"
            | "attributeNames"
            | "getElementsByTagName"
            | "querySelectorAll"
            | "querySelector"
            | "findnodes"
            | "findvalue"
            | "setTextContent"
            | "setData"
            | "appendChild"
            | "prependChild"
            | "insertBefore"
            | "replaceChild"
            | "removeChild"
            | "remove"
            | "firstChild"
            | "lastChild"
            | "cloneNode"
            | "isEqualNode"
            | "contains"
            | "isSameNode"
            | "visitEach"
            | "findFirst"
            | "toXML"
            | "to_String"
    )
}

fn call_document_method(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    match name {
        "documentElement" => Ok(document_element(object)),
        "nodeName" => Ok(Value::String("#document".to_owned())),
        "nodeType" => Ok(Value::String("9".to_owned())),
        "nodeKind" => Ok(Value::String("document".to_owned())),
        "childNodes" => Ok(Value::Array(xml_children(object))),
        "children" => Ok(Value::Array(
            xml_children(object)
                .into_iter()
                .filter(is_element_value)
                .collect(),
        )),
        "hasChildNodes" => Ok(Value::Number(
            (!xml_children(object).is_empty()) as i32 as f64,
        )),
        "createElement" => Ok(create_xml_node(
            xml_document(object),
            "DOMElement",
            "element",
            &args.first().map(render_string).unwrap_or_default(),
            "",
        )),
        "createComment" => Ok(create_xml_node(
            xml_document(object),
            "DOMComment",
            "comment",
            "#comment",
            &args.first().map(render_string).unwrap_or_default(),
        )),
        "createTextNode" | "createCDATASection" => Ok(create_xml_node(
            xml_document(object),
            "DOMText",
            "text",
            "#text",
            &args.first().map(render_string).unwrap_or_default(),
        )),
        "findnodes" => Ok(Value::Array(findnodes_from_value(
            &document_element(object),
            &args.first().map(render_string).unwrap_or_default(),
        ))),
        "findvalue" => Ok(Value::String(findvalue_from_value(
            &document_element(object),
            &args.first().map(render_string).unwrap_or_default(),
        ))),
        "getElementsByTagName" => Ok(Value::Array(find_descendants_by_tag_name(
            &document_element(object),
            &args.first().map(render_string).unwrap_or_default(),
        ))),
        "querySelectorAll" => Ok(Value::Array(query_selector_all(
            &document_element(object),
            &args.first().map(render_string).unwrap_or_default(),
        ))),
        "querySelector" => Ok(query_selector_all(
            &document_element(object),
            &args.first().map(render_string).unwrap_or_default(),
        )
        .into_iter()
        .next()
        .unwrap_or(Value::Null)),
        "visitEach" => {
            let callback = args.first().cloned().unwrap_or(Value::Null);
            for node in walk_nodes_including_self(&Value::Object(Rc::clone(object))) {
                let _ = runtime.call_value(callback.clone(), vec![node], Vec::new())?;
            }
            Ok(Value::Null)
        }
        "findFirst" => {
            let callback = args.first().cloned().unwrap_or(Value::Null);
            let root = document_element(object);
            for node in walk_nodes_including_self(&root) {
                if runtime
                    .call_value(callback.clone(), vec![node.clone()], Vec::new())?
                    .is_truthy()
                {
                    return Ok(node);
                }
            }
            Ok(Value::Null)
        }
        "toXML" | "to_String" => Ok(Value::String(serialize_value(&Value::Object(Rc::clone(
            object,
        ))))),
        _ => Err(ZuzuRustError::thrown(format!(
            "unsupported XMLDocument method '{}'",
            name
        ))),
    }
}

fn call_node_method(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    match name {
        "nodeName" | "tagName" => Ok(field_or_null(object, "__xml_name")),
        "localName" => Ok(field_or_null(object, "__xml_local_name")),
        "namespaceURI" => Ok(field_or_null(object, "__xml_namespace_uri")),
        "nodeType" => Ok(Value::String(render_string(&field_or_null(
            object,
            "__xml_node_type",
        )))),
        "nodeKind" => Ok(field_or_null(object, "__xml_kind")),
        "unique_id" | "uniqueKey" => Ok(field_or_null(object, "__xml_unique_id")),
        "childNodes" => Ok(Value::Array(xml_children(object))),
        "children" => Ok(Value::Array(
            xml_children(object)
                .into_iter()
                .filter(is_element_value)
                .collect(),
        )),
        "hasChildNodes" => Ok(Value::Number(
            (!xml_children(object).is_empty()) as i32 as f64,
        )),
        "attributes" => Ok(field_or_default(
            object,
            "__xml_attributes",
            Value::Array(Vec::new()),
        )),
        "nextSibling" => Ok(next_sibling(object)),
        "previousSibling" => Ok(previous_sibling(object)),
        "parentNode" => Ok(field_or_null(object, "__xml_parent")),
        "ownerDocument" => Ok(field_or_null(object, "__xml_document")),
        "textContent" | "data" => Ok(Value::String(text_content(&Value::Object(Rc::clone(
            object,
        ))))),
        "nodeValue" => Ok(node_value(&Value::Object(Rc::clone(object)))),
        "id" => Ok(Value::String(get_attribute(object, "id"))),
        "setId" => {
            set_attribute(
                object,
                "id",
                args.first().map(render_string).unwrap_or_default(),
            );
            Ok(Value::Null)
        }
        "getAttribute" => Ok(Value::String(get_attribute(
            object,
            &args.first().map(render_string).unwrap_or_default(),
        ))),
        "hasAttribute" => Ok(Value::Number(
            (!get_attribute(object, &args.first().map(render_string).unwrap_or_default())
                .is_empty()) as i32 as f64,
        )),
        "setAttribute" => {
            set_attribute(
                object,
                &args.first().map(render_string).unwrap_or_default(),
                args.get(1).map(render_string).unwrap_or_default(),
            );
            Ok(Value::Null)
        }
        "removeAttribute" => {
            remove_attribute(object, &args.first().map(render_string).unwrap_or_default());
            Ok(Value::Null)
        }
        "attributeNames" => Ok(Value::Array(
            attribute_names(object)
                .into_iter()
                .map(Value::String)
                .collect(),
        )),
        "getElementsByTagName" => Ok(Value::Array(find_descendants_by_tag_name(
            &Value::Object(Rc::clone(object)),
            &args.first().map(render_string).unwrap_or_default(),
        ))),
        "querySelectorAll" => Ok(Value::Array(query_selector_all(
            &Value::Object(Rc::clone(object)),
            &args.first().map(render_string).unwrap_or_default(),
        ))),
        "querySelector" => Ok(query_selector_all(
            &Value::Object(Rc::clone(object)),
            &args.first().map(render_string).unwrap_or_default(),
        )
        .into_iter()
        .next()
        .unwrap_or(Value::Null)),
        "findnodes" => Ok(Value::Array(findnodes_from_value(
            &Value::Object(Rc::clone(object)),
            &args.first().map(render_string).unwrap_or_default(),
        ))),
        "findvalue" => Ok(Value::String(findvalue_from_value(
            &Value::Object(Rc::clone(object)),
            &args.first().map(render_string).unwrap_or_default(),
        ))),
        "setTextContent" | "setData" => {
            set_text_content(object, args.first().map(render_string).unwrap_or_default());
            Ok(Value::Null)
        }
        "appendChild" => {
            if let Some(child) = args.first().and_then(as_object) {
                append_child(object, &child);
            }
            Ok(Value::Null)
        }
        "prependChild" => {
            if let Some(child) = args.first().and_then(as_object) {
                insert_child(object, &child, 0);
            }
            Ok(Value::Null)
        }
        "insertBefore" => {
            if let (Some(child), Some(before)) = (
                args.first().and_then(as_object),
                args.get(1).and_then(as_object),
            ) {
                let index = child_index(object, &before).unwrap_or(0);
                insert_child(object, &child, index);
            }
            Ok(Value::Null)
        }
        "replaceChild" => {
            if let (Some(new_child), Some(old_child)) = (
                args.first().and_then(as_object),
                args.get(1).and_then(as_object),
            ) {
                replace_child(object, &new_child, &old_child);
            }
            Ok(Value::Null)
        }
        "removeChild" => {
            if let Some(child) = args.first().and_then(as_object) {
                remove_child(object, &child);
            }
            Ok(Value::Null)
        }
        "remove" => {
            detach_node(object);
            Ok(Value::Null)
        }
        "firstChild" => Ok(xml_children(object)
            .into_iter()
            .next()
            .unwrap_or(Value::Null)),
        "lastChild" => Ok(xml_children(object)
            .into_iter()
            .last()
            .unwrap_or(Value::Null)),
        "cloneNode" => Ok(clone_node(
            &Value::Object(Rc::clone(object)),
            args.first().map(Value::is_truthy).unwrap_or(false),
        )),
        "isEqualNode" => Ok(Value::Number(
            (serialize_value(&Value::Object(Rc::clone(object)))
                == serialize_value(args.first().unwrap_or(&Value::Null))) as i32 as f64,
        )),
        "contains" => Ok(Value::Number(is_descendant_or_self(
            &Value::Object(Rc::clone(object)),
            args.first().unwrap_or(&Value::Null),
        ) as i32 as f64)),
        "isSameNode" => Ok(Value::Number(same_node(
            &Value::Object(Rc::clone(object)),
            args.first().unwrap_or(&Value::Null),
        ) as i32 as f64)),
        "visitEach" => {
            let callback = args.first().cloned().unwrap_or(Value::Null);
            for node in walk_descendants(&Value::Object(Rc::clone(object))) {
                let _ = runtime.call_value(callback.clone(), vec![node], Vec::new())?;
            }
            Ok(Value::Null)
        }
        "findFirst" => {
            let callback = args.first().cloned().unwrap_or(Value::Null);
            for node in walk_descendants(&Value::Object(Rc::clone(object))) {
                if runtime
                    .call_value(callback.clone(), vec![node.clone()], Vec::new())?
                    .is_truthy()
                {
                    return Ok(node);
                }
            }
            Ok(Value::Null)
        }
        "toXML" | "to_String" => Ok(Value::String(serialize_value(&Value::Object(Rc::clone(
            object,
        ))))),
        _ => Err(ZuzuRustError::thrown(format!(
            "unsupported XML node method '{}'",
            name
        ))),
    }
}

fn as_object(value: &Value) -> Option<Rc<RefCell<ObjectValue>>> {
    match value {
        Value::Object(object) => Some(Rc::clone(object)),
        _ => None,
    }
}

fn field_or_null(object: &Rc<RefCell<ObjectValue>>, name: &str) -> Value {
    object
        .borrow()
        .fields
        .get(name)
        .cloned()
        .unwrap_or(Value::Null)
}

fn field_or_default(object: &Rc<RefCell<ObjectValue>>, name: &str, default: Value) -> Value {
    object.borrow().fields.get(name).cloned().unwrap_or(default)
}

fn xml_document(object: &Rc<RefCell<ObjectValue>>) -> Value {
    field_or_null(object, "__xml_document")
}

fn document_element(object: &Rc<RefCell<ObjectValue>>) -> Value {
    field_or_null(object, "__document_element")
}

fn xml_children(object: &Rc<RefCell<ObjectValue>>) -> Vec<Value> {
    match object.borrow().fields.get("__xml_children") {
        Some(Value::Array(children)) => children.clone(),
        _ => Vec::new(),
    }
}

fn set_xml_children(object: &Rc<RefCell<ObjectValue>>, children: Vec<Value>) {
    object
        .borrow_mut()
        .fields
        .insert("__xml_children".to_owned(), Value::Array(children));
}

fn is_element_value(value: &Value) -> bool {
    matches!(
        value,
        Value::Object(object)
            if matches!(
                object.borrow().fields.get("__xml_kind"),
                Some(Value::String(kind)) if kind == "element"
            )
    )
}

fn same_node(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Object(left), Value::Object(right)) => Rc::ptr_eq(left, right),
        _ => false,
    }
}

fn child_index(
    parent: &Rc<RefCell<ObjectValue>>,
    child: &Rc<RefCell<ObjectValue>>,
) -> Option<usize> {
    xml_children(parent)
        .iter()
        .position(|value| matches!(value, Value::Object(item) if Rc::ptr_eq(item, child)))
}

fn detach_node(child: &Rc<RefCell<ObjectValue>>) {
    if let Some(parent) = as_object(&field_or_null(child, "__xml_parent")) {
        remove_child(&parent, child);
    }
}

fn append_child(parent: &Rc<RefCell<ObjectValue>>, child: &Rc<RefCell<ObjectValue>>) {
    let index = xml_children(parent).len();
    insert_child(parent, child, index);
}

fn insert_child(parent: &Rc<RefCell<ObjectValue>>, child: &Rc<RefCell<ObjectValue>>, index: usize) {
    detach_node(child);
    let mut children = xml_children(parent);
    let index = index.min(children.len());
    children.insert(index, Value::Object(Rc::clone(child)));
    set_xml_children(parent, children);
    child
        .borrow_mut()
        .fields
        .insert("__xml_parent".to_owned(), Value::Object(Rc::clone(parent)));
}

fn replace_child(
    parent: &Rc<RefCell<ObjectValue>>,
    new_child: &Rc<RefCell<ObjectValue>>,
    old_child: &Rc<RefCell<ObjectValue>>,
) {
    let mut children = xml_children(parent);
    if let Some(index) = child_index(parent, old_child) {
        detach_node(new_child);
        children[index] = Value::Object(Rc::clone(new_child));
        set_xml_children(parent, children);
        new_child
            .borrow_mut()
            .fields
            .insert("__xml_parent".to_owned(), Value::Object(Rc::clone(parent)));
        old_child
            .borrow_mut()
            .fields
            .insert("__xml_parent".to_owned(), Value::Null);
    }
}

fn remove_child(parent: &Rc<RefCell<ObjectValue>>, child: &Rc<RefCell<ObjectValue>>) {
    let children = xml_children(parent)
        .into_iter()
        .filter(|value| !matches!(value, Value::Object(item) if Rc::ptr_eq(item, child)))
        .collect::<Vec<_>>();
    set_xml_children(parent, children);
    child
        .borrow_mut()
        .fields
        .insert("__xml_parent".to_owned(), Value::Null);
}

fn previous_sibling(object: &Rc<RefCell<ObjectValue>>) -> Value {
    let Some(parent) = as_object(&field_or_null(object, "__xml_parent")) else {
        return Value::Null;
    };
    child_index(&parent, object)
        .and_then(|index| index.checked_sub(1))
        .and_then(|index| xml_children(&parent).get(index).cloned())
        .unwrap_or(Value::Null)
}

fn next_sibling(object: &Rc<RefCell<ObjectValue>>) -> Value {
    let Some(parent) = as_object(&field_or_null(object, "__xml_parent")) else {
        return Value::Null;
    };
    child_index(&parent, object)
        .and_then(|index| xml_children(&parent).get(index + 1).cloned())
        .unwrap_or(Value::Null)
}

fn get_attribute(object: &Rc<RefCell<ObjectValue>>, name: &str) -> String {
    match object.borrow().fields.get("__xml_attrs") {
        Some(Value::PairList(attrs)) => attrs
            .iter()
            .find(|(attr_name, _)| attr_name == name)
            .map(|(_, value)| render_string(value))
            .unwrap_or_default(),
        _ => String::new(),
    }
}

fn set_attribute(object: &Rc<RefCell<ObjectValue>>, name: &str, value: String) {
    let mut object_ref = object.borrow_mut();
    let attrs = match object_ref.fields.remove("__xml_attrs") {
        Some(Value::PairList(attrs)) => attrs,
        _ => Vec::new(),
    };
    let mut updated = false;
    let mut next = Vec::with_capacity(attrs.len() + 1);
    for (attr_name, attr_value) in attrs {
        if attr_name == name {
            if !updated {
                next.push((attr_name, Value::String(value.clone())));
                updated = true;
            }
        } else {
            next.push((attr_name, attr_value));
        }
    }
    if !updated {
        next.push((name.to_owned(), Value::String(value)));
    }
    object_ref
        .fields
        .insert("__xml_attrs".to_owned(), Value::PairList(next));
}

fn remove_attribute(object: &Rc<RefCell<ObjectValue>>, name: &str) {
    let mut object_ref = object.borrow_mut();
    let attrs = match object_ref.fields.remove("__xml_attrs") {
        Some(Value::PairList(attrs)) => attrs,
        _ => Vec::new(),
    };
    object_ref.fields.insert(
        "__xml_attrs".to_owned(),
        Value::PairList(
            attrs
                .into_iter()
                .filter(|(attr_name, _)| attr_name != name)
                .collect(),
        ),
    );
}

fn attribute_names(object: &Rc<RefCell<ObjectValue>>) -> Vec<String> {
    let mut names = match object.borrow().fields.get("__xml_attrs") {
        Some(Value::PairList(attrs)) => attrs.iter().map(|(name, _)| name.clone()).collect(),
        _ => Vec::new(),
    };
    names.sort();
    names.dedup();
    names
}

fn create_xml_node(document: Value, class_name: &str, kind: &str, name: &str, text: &str) -> Value {
    let unique_id = format!(
        "xml:{:p}",
        Rc::as_ptr(&Rc::new(RefCell::new(ObjectValue {
            class: class_named(class_name),
            fields: HashMap::new(),
            weak_fields: std::collections::HashSet::new(),
            builtin_value: Some(Value::Null),
        })))
    );
    let (local_name, namespace_uri) = if let Some((_, local)) = name.split_once(':') {
        (Value::String(local.to_owned()), Value::Null)
    } else {
        (Value::Null, Value::Null)
    };
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: class_named(class_name),
        fields: HashMap::from([
            ("__xml_name".to_owned(), Value::String(name.to_owned())),
            ("__xml_local_name".to_owned(), local_name),
            ("__xml_namespace_uri".to_owned(), namespace_uri),
            ("__xml_text".to_owned(), Value::String(text.to_owned())),
            ("__xml_kind".to_owned(), Value::String(kind.to_owned())),
            (
                "__xml_node_type".to_owned(),
                Value::Number(match kind {
                    "element" => 1.0,
                    "comment" => 8.0,
                    _ => 3.0,
                }),
            ),
            ("__xml_unique_id".to_owned(), Value::String(unique_id)),
            ("__xml_attrs".to_owned(), Value::PairList(Vec::new())),
            ("__xml_attributes".to_owned(), Value::Array(Vec::new())),
            ("__xml_children".to_owned(), Value::Array(Vec::new())),
            ("__xml_parent".to_owned(), Value::Null),
            ("__xml_document".to_owned(), document),
        ]),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Null),
    })))
}

fn walk_nodes_including_self(root: &Value) -> Vec<Value> {
    let mut nodes = vec![root.clone()];
    nodes.extend(walk_descendants(root));
    nodes
}

fn walk_descendants(root: &Value) -> Vec<Value> {
    let mut out = Vec::new();
    if let Some(object) = as_object(root) {
        for child in xml_children(&object) {
            out.push(child.clone());
            out.extend(walk_descendants(&child));
        }
    }
    out
}

fn clone_node(value: &Value, deep: bool) -> Value {
    let Some(object) = as_object(value) else {
        return Value::Null;
    };
    let object_ref = object.borrow();
    let clone = Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: Rc::clone(&object_ref.class),
        fields: object_ref.fields.clone(),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Null),
    })));
    drop(object_ref);
    if let Some(clone_object) = as_object(&clone) {
        clone_object
            .borrow_mut()
            .fields
            .insert("__xml_parent".to_owned(), Value::Null);
        if deep {
            let children = xml_children(&object)
                .into_iter()
                .map(|child| {
                    let cloned = clone_node(&child, true);
                    if let Some(cloned_object) = as_object(&cloned) {
                        cloned_object.borrow_mut().fields.insert(
                            "__xml_parent".to_owned(),
                            Value::Object(Rc::clone(&clone_object)),
                        );
                    }
                    cloned
                })
                .collect();
            set_xml_children(&clone_object, children);
        } else {
            set_xml_children(&clone_object, Vec::new());
        }
    }
    clone
}

fn is_descendant_or_self(root: &Value, candidate: &Value) -> bool {
    same_node(root, candidate)
        || walk_descendants(root)
            .iter()
            .any(|node| same_node(node, candidate))
}

fn find_descendants_by_tag_name(root: &Value, tag: &str) -> Vec<Value> {
    walk_descendants(root)
        .into_iter()
        .filter(|node| {
            matches!(
                node,
                Value::Object(object)
                    if matches!(
                        object.borrow().fields.get("__xml_name"),
                        Some(Value::String(name)) if name == tag
                    )
            )
        })
        .collect()
}

fn query_selector_all(root: &Value, selector: &str) -> Vec<Value> {
    if selector.starts_with('.') {
        Vec::new()
    } else {
        find_descendants_by_tag_name(root, selector)
    }
}

fn findnodes_from_value(root: &Value, expr: &str) -> Vec<Value> {
    let query = expr.trim();
    if query.is_empty() {
        return Vec::new();
    }
    let Some(projection) = LibxmlProjection::from_value(root) else {
        return Vec::new();
    };
    let Ok(mut context) = XPathContext::new(&projection.document) else {
        return Vec::new();
    };
    let found = match projection.context_node.as_ref() {
        Some(node) => context.findnodes(query, Some(node)),
        None => context.findnodes(query, None),
    };
    found
        .unwrap_or_default()
        .into_iter()
        .filter_map(|node| projection.values.get(&node.to_hashable()).cloned())
        .collect()
}

fn findvalue_from_value(root: &Value, expr: &str) -> String {
    let query = expr.trim();
    if query.is_empty() {
        return String::new();
    }
    let Some(projection) = LibxmlProjection::from_value(root) else {
        return String::new();
    };
    let Ok(mut context) = XPathContext::new(&projection.document) else {
        return String::new();
    };
    match projection.context_node.as_ref() {
        Some(node) => context.findvalue(query, Some(node)),
        None => context.findvalue(query, None),
    }
    .unwrap_or_default()
}

fn set_text_content(object: &Rc<RefCell<ObjectValue>>, value: String) {
    let kind = match object.borrow().fields.get("__xml_kind") {
        Some(Value::String(kind)) => kind.clone(),
        _ => String::new(),
    };
    if kind == "element" || kind == "document" {
        let document = xml_document(object);
        let text_node = create_xml_node(document, "DOMText", "text", "#text", &value);
        if let Some(text_object) = as_object(&text_node) {
            text_object
                .borrow_mut()
                .fields
                .insert("__xml_parent".to_owned(), Value::Object(Rc::clone(object)));
        }
        set_xml_children(object, vec![text_node]);
    } else {
        object
            .borrow_mut()
            .fields
            .insert("__xml_text".to_owned(), Value::String(value));
    }
}

fn parse_xml_document(text: &str) -> Result<Value> {
    let nodes = parse_nodes_with_libxml(text)?;
    build_document(nodes)
}

fn parse_nodes_with_libxml(text: &str) -> Result<Vec<ParsedNode>> {
    let options = ParserOptions {
        recover: false,
        no_net: true,
        ..ParserOptions::default()
    };
    let document = Parser::default()
        .parse_string_with_options(text.as_bytes(), options)
        .map_err(|err| ZuzuRustError::runtime(format!("XML.parse failed: {err}")))?;
    let mut nodes = vec![ParsedNode {
        kind: XmlKind::Document,
        name: "#document".to_owned(),
        text: String::new(),
        attrs: Vec::new(),
        parent: None,
        children: Vec::new(),
    }];

    for child in document.as_node().get_child_nodes() {
        append_parsed_libxml_node(&mut nodes, &child, 0);
    }
    Ok(nodes)
}

fn append_parsed_libxml_node(nodes: &mut Vec<ParsedNode>, node: &LibxmlNode, parent: usize) {
    let kind = match node.get_type() {
        Some(NodeType::ElementNode) => XmlKind::Element,
        Some(NodeType::TextNode) | Some(NodeType::CDataSectionNode) => XmlKind::Text,
        Some(NodeType::CommentNode) => XmlKind::Comment,
        _ => return,
    };
    let index = nodes.len();
    nodes[parent].children.push(index);
    nodes.push(ParsedNode {
        kind,
        name: match kind {
            XmlKind::Element => qualified_libxml_name(node),
            XmlKind::Text => "#text".to_owned(),
            XmlKind::Comment => "#comment".to_owned(),
            XmlKind::Document => "#document".to_owned(),
        },
        text: match kind {
            XmlKind::Text | XmlKind::Comment => node.get_content(),
            _ => String::new(),
        },
        attrs: libxml_attrs(node),
        parent: Some(parent),
        children: Vec::new(),
    });
    for child in node.get_child_nodes() {
        append_parsed_libxml_node(nodes, &child, index);
    }
}

fn qualified_libxml_name(node: &LibxmlNode) -> String {
    let name = node.get_name();
    if name.contains(':') {
        return name;
    }
    let Some(namespace) = node.get_namespace() else {
        return name;
    };
    let prefix = namespace.get_prefix();
    if prefix.is_empty() {
        name
    } else {
        format!("{prefix}:{name}")
    }
}

fn libxml_attrs(node: &LibxmlNode) -> Vec<(String, String)> {
    let mut attrs = node
        .get_attributes_ns()
        .into_iter()
        .map(|((name, namespace), value)| {
            let name = match namespace {
                Some(namespace) if !namespace.get_prefix().is_empty() && !name.contains(':') => {
                    format!("{}:{name}", namespace.get_prefix())
                }
                _ => name,
            };
            (name, value)
        })
        .collect::<Vec<_>>();
    for namespace in node.get_namespace_declarations() {
        let prefix = namespace.get_prefix();
        let name = if prefix.is_empty() {
            "xmlns".to_owned()
        } else {
            format!("xmlns:{prefix}")
        };
        attrs.push((name, namespace.get_href()));
    }
    attrs.sort_by(|left, right| left.0.cmp(&right.0));
    attrs.dedup_by(|left, right| left.0 == right.0);
    attrs
}

fn build_document(nodes: Vec<ParsedNode>) -> Result<Value> {
    let namespace_scopes = build_namespace_scopes(&nodes);
    let mut objects = Vec::with_capacity(nodes.len());
    for (index, node) in nodes.iter().enumerate() {
        let class_name = match node.kind {
            XmlKind::Document => "XMLDocument",
            XmlKind::Element => "DOMElement",
            XmlKind::Text => "DOMText",
            XmlKind::Comment => "DOMComment",
        };
        let (local_name, namespace_uri) =
            name_parts_with_scope(&node.name, &namespace_scopes[index]);
        let mut fields = HashMap::new();
        fields.insert("__xml_name".to_owned(), Value::String(node.name.clone()));
        fields.insert(
            "__xml_local_name".to_owned(),
            local_name.map(Value::String).unwrap_or(Value::Null),
        );
        fields.insert(
            "__xml_namespace_uri".to_owned(),
            namespace_uri.map(Value::String).unwrap_or(Value::Null),
        );
        fields.insert("__xml_text".to_owned(), Value::String(node.text.clone()));
        fields.insert(
            "__xml_kind".to_owned(),
            Value::String(
                match node.kind {
                    XmlKind::Document => "document",
                    XmlKind::Element => "element",
                    XmlKind::Text => "text",
                    XmlKind::Comment => "comment",
                }
                .to_owned(),
            ),
        );
        fields.insert(
            "__xml_node_type".to_owned(),
            Value::Number(match node.kind {
                XmlKind::Document => 9.0,
                XmlKind::Element => 1.0,
                XmlKind::Text => 3.0,
                XmlKind::Comment => 8.0,
            }),
        );
        fields.insert(
            "__xml_unique_id".to_owned(),
            Value::String(format!("xml:{index}")),
        );
        fields.insert(
            "__xml_attrs".to_owned(),
            Value::PairList(
                node.attrs
                    .iter()
                    .map(|(name, value)| (name.clone(), Value::String(value.clone())))
                    .collect(),
            ),
        );
        let object = Rc::new(RefCell::new(ObjectValue {
            class: class_named(class_name),
            fields,
            weak_fields: std::collections::HashSet::new(),
            builtin_value: Some(Value::Null),
        }));
        objects.push(object);
    }

    let document = Rc::clone(&objects[0]);
    for (index, node) in nodes.iter().enumerate() {
        let object = Rc::clone(&objects[index]);
        let mut fields = object.borrow_mut();
        let children = node
            .children
            .iter()
            .map(|child| Value::Object(Rc::clone(&objects[*child])))
            .collect::<Vec<_>>();
        fields
            .fields
            .insert("__xml_children".to_owned(), Value::Array(children));
        fields.fields.insert(
            "__xml_parent".to_owned(),
            node.parent
                .map(|parent| Value::Object(Rc::clone(&objects[parent])))
                .unwrap_or(Value::Null),
        );
        fields.fields.insert(
            "__xml_document".to_owned(),
            Value::Object(Rc::clone(&document)),
        );
        if let Some(parent) = node.parent {
            let siblings = &nodes[parent].children;
            let position = siblings
                .iter()
                .position(|child| *child == index)
                .unwrap_or(0);
            let prev = position
                .checked_sub(1)
                .and_then(|pos| siblings.get(pos))
                .map(|sibling| Value::Object(Rc::clone(&objects[*sibling])))
                .unwrap_or(Value::Null);
            let next = siblings
                .get(position + 1)
                .map(|sibling| Value::Object(Rc::clone(&objects[*sibling])))
                .unwrap_or(Value::Null);
            fields
                .fields
                .insert("__xml_previous_sibling".to_owned(), prev);
            fields.fields.insert("__xml_next_sibling".to_owned(), next);
        } else {
            fields
                .fields
                .insert("__xml_previous_sibling".to_owned(), Value::Null);
            fields
                .fields
                .insert("__xml_next_sibling".to_owned(), Value::Null);
        }
        let attrs = node
            .attrs
            .iter()
            .enumerate()
            .map(|(attr_index, (name, value))| {
                Value::Object(build_attr_object(
                    &document,
                    &object,
                    index,
                    attr_index,
                    name,
                    value,
                    &namespace_scopes[index],
                ))
            })
            .collect::<Vec<_>>();
        fields
            .fields
            .insert("__xml_attributes".to_owned(), Value::Array(attrs));
    }

    let root = nodes[0]
        .children
        .iter()
        .copied()
        .find(|child| nodes[*child].kind == XmlKind::Element)
        .map(|child| Value::Object(Rc::clone(&objects[child])))
        .unwrap_or(Value::Null);
    document
        .borrow_mut()
        .fields
        .insert("__document_element".to_owned(), root);
    Ok(Value::Object(document))
}

fn build_attr_object(
    document: &Rc<RefCell<ObjectValue>>,
    parent: &Rc<RefCell<ObjectValue>>,
    node_index: usize,
    attr_index: usize,
    name: &str,
    value: &str,
    namespace_scope: &HashMap<String, String>,
) -> Rc<RefCell<ObjectValue>> {
    let (local_name, namespace_uri) = name_parts_with_scope(name, namespace_scope);
    Rc::new(RefCell::new(ObjectValue {
        class: class_named("DOMAttr"),
        fields: HashMap::from([
            ("__xml_name".to_owned(), Value::String(name.to_owned())),
            (
                "__xml_local_name".to_owned(),
                local_name.map(Value::String).unwrap_or(Value::Null),
            ),
            (
                "__xml_namespace_uri".to_owned(),
                namespace_uri.map(Value::String).unwrap_or(Value::Null),
            ),
            ("__xml_text".to_owned(), Value::String(value.to_owned())),
            ("__xml_kind".to_owned(), Value::String("attr".to_owned())),
            ("__xml_node_type".to_owned(), Value::Number(2.0)),
            (
                "__xml_unique_id".to_owned(),
                Value::String(format!("xml:{node_index}:attr:{attr_index}")),
            ),
            ("__xml_children".to_owned(), Value::Array(Vec::new())),
            ("__xml_attributes".to_owned(), Value::Array(Vec::new())),
            ("__xml_parent".to_owned(), Value::Object(Rc::clone(parent))),
            (
                "__xml_document".to_owned(),
                Value::Object(Rc::clone(document)),
            ),
            ("__xml_previous_sibling".to_owned(), Value::Null),
            ("__xml_next_sibling".to_owned(), Value::Null),
        ]),
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Null),
    }))
}

fn build_namespace_scopes(nodes: &[ParsedNode]) -> Vec<HashMap<String, String>> {
    let mut scopes = Vec::with_capacity(nodes.len());
    for node in nodes {
        let mut scope: HashMap<String, String> = node
            .parent
            .and_then(|parent| scopes.get(parent).cloned())
            .unwrap_or_default();
        for (name, value) in &node.attrs {
            if name == "xmlns" {
                scope.insert(String::new(), value.clone());
            } else if let Some(prefix) = name.strip_prefix("xmlns:") {
                scope.insert(prefix.to_owned(), value.clone());
            }
        }
        scopes.push(scope);
    }
    scopes
}

fn name_parts_with_scope(
    name: &str,
    namespace_scope: &HashMap<String, String>,
) -> (Option<String>, Option<String>) {
    if let Some((prefix, local_name)) = name.split_once(':') {
        return (
            Some(local_name.to_owned()),
            namespace_scope.get(prefix).cloned(),
        );
    }
    (Some(name.to_owned()), namespace_scope.get("").cloned())
}

fn class_named(name: &str) -> Rc<UserClassValue> {
    Rc::new(UserClassValue {
        name: name.to_owned(),
        base: None,
        traits: Vec::<Rc<TraitValue>>::new(),
        fields: Vec::<FieldSpec>::new(),
        methods: HashMap::<String, Rc<MethodValue>>::new(),
        static_methods: HashMap::<String, Rc<MethodValue>>::new(),
        nested_classes: HashMap::new(),
        source_decl: None,
        closure_env: None,
    })
}

fn node_value(value: &Value) -> Value {
    match value {
        Value::Object(object) => {
            let object_ref = object.borrow();
            match object_ref.fields.get("__xml_kind") {
                Some(Value::String(kind)) if kind == "element" || kind == "document" => Value::Null,
                _ => object_ref
                    .fields
                    .get("__xml_text")
                    .cloned()
                    .unwrap_or(Value::Null),
            }
        }
        _ => Value::Null,
    }
}

fn text_content(value: &Value) -> String {
    match value {
        Value::Object(object) => {
            let object_ref = object.borrow();
            let kind = object_ref
                .fields
                .get("__xml_kind")
                .and_then(|value| match value {
                    Value::String(kind) => Some(kind.as_str()),
                    _ => None,
                })
                .unwrap_or("");
            match kind {
                "text" | "comment" | "attr" => object_ref
                    .fields
                    .get("__xml_text")
                    .map(render_string)
                    .unwrap_or_default(),
                _ => match object_ref.fields.get("__xml_children") {
                    Some(Value::Array(children)) => children
                        .iter()
                        .map(text_content)
                        .collect::<Vec<_>>()
                        .join(""),
                    _ => String::new(),
                },
            }
        }
        _ => String::new(),
    }
}

struct LibxmlProjection {
    document: LibxmlDocument,
    context_node: Option<LibxmlNode>,
    values: HashMap<usize, Value>,
}

impl LibxmlProjection {
    fn from_value(value: &Value) -> Option<Self> {
        let mut document = LibxmlDocument::new().ok()?;
        let mut values = HashMap::new();
        let context_node = match xml_kind(value).as_deref() {
            Some("document") => {
                let object = as_object(value)?;
                for child in xml_children(&object) {
                    if !is_element_value(&child) {
                        continue;
                    }
                    let node = build_libxml_node(&document, &child, &mut values)?;
                    document.set_root_element(&node);
                    break;
                }
                None
            }
            Some("element") => {
                let node = build_libxml_node(&document, value, &mut values)?;
                document.set_root_element(&node);
                Some(node)
            }
            Some("text") | Some("comment") | Some("attr") => {
                Some(build_libxml_node(&document, value, &mut values)?)
            }
            _ => return None,
        };
        Some(Self {
            document,
            context_node,
            values,
        })
    }
}

fn serialize_value(value: &Value) -> String {
    serialize_value_with_libxml(value).unwrap_or_default()
}

fn serialize_value_with_libxml(value: &Value) -> Option<String> {
    match value {
        Value::Object(object) => {
            let object_ref = object.borrow();
            let kind = object_ref
                .fields
                .get("__xml_kind")
                .map(render_string)
                .unwrap_or_default();
            drop(object_ref);
            let options = SaveOptions {
                no_declaration: true,
                no_empty_tags: true,
                ..SaveOptions::default()
            };
            if kind == "document" || kind == "element" {
                return LibxmlProjection::from_value(value)
                    .map(|projection| projection.document.to_string_with_options(options));
            }
            let document = LibxmlDocument::new().ok()?;
            let mut values = HashMap::new();
            let node = build_libxml_node(&document, value, &mut values)?;
            Some(document.node_to_string(&node))
        }
        _ => None,
    }
}

fn build_libxml_node(
    document: &LibxmlDocument,
    value: &Value,
    values: &mut HashMap<usize, Value>,
) -> Option<LibxmlNode> {
    let object = as_object(value)?;
    let kind = render_string(&field_or_null(&object, "__xml_kind"));
    let node = match kind.as_str() {
        "element" => {
            let name = render_string(&field_or_null(&object, "__xml_name"));
            let mut node = LibxmlNode::new(&name, None, document).ok()?;
            for (name, value) in xml_attrs(&object) {
                node.set_property(&name, &render_string(&value)).ok()?;
            }
            values.insert(node.to_hashable(), value.clone());
            map_libxml_attributes(&node, value, values);
            for child in xml_children(&object) {
                let mut child_node = build_libxml_node(document, &child, values)?;
                node.add_child(&mut child_node).ok()?;
            }
            node
        }
        "text" => {
            let text = render_string(&field_or_null(&object, "__xml_text"));
            let node = LibxmlNode::new_text(&text, document).ok()?;
            values.insert(node.to_hashable(), value.clone());
            node
        }
        "comment" => {
            let text = render_string(&field_or_null(&object, "__xml_text"));
            let node = LibxmlNode::new_comment(&text, document).ok()?;
            values.insert(node.to_hashable(), value.clone());
            node
        }
        "attr" => {
            let text = render_string(&field_or_null(&object, "__xml_text"));
            let node = LibxmlNode::new_text(&text, document).ok()?;
            values.insert(node.to_hashable(), value.clone());
            node
        }
        _ => return None,
    };
    Some(node)
}

fn map_libxml_attributes(node: &LibxmlNode, value: &Value, values: &mut HashMap<usize, Value>) {
    let Some(object) = as_object(value) else {
        return;
    };
    let attrs = match object.borrow().fields.get("__xml_attributes") {
        Some(Value::Array(attrs)) => attrs.clone(),
        _ => Vec::new(),
    };
    for attr in attrs {
        let Some(attr_object) = as_object(&attr) else {
            continue;
        };
        let name = render_string(&field_or_null(&attr_object, "__xml_name"));
        if let Some(attr_node) = node.get_attribute_node(&name) {
            values.insert(attr_node.to_hashable(), attr);
        }
    }
}

fn xml_kind(value: &Value) -> Option<String> {
    as_object(value).map(|object| render_string(&field_or_null(&object, "__xml_kind")))
}

fn xml_attrs(object: &Rc<RefCell<ObjectValue>>) -> Vec<(String, Value)> {
    match object.borrow().fields.get("__xml_attrs") {
        Some(Value::PairList(items)) => items.clone(),
        _ => Vec::new(),
    }
}

fn render_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Number(number) => {
            if number.fract() == 0.0 {
                format!("{}", *number as i64)
            } else {
                number.to_string()
            }
        }
        Value::Boolean(true) => "true".to_owned(),
        Value::Boolean(false) => "false".to_owned(),
        Value::Null => String::new(),
        _ => value.render(),
    }
}
