use std::cell::RefCell;
use std::collections::HashMap;
use std::ffi::{c_char, c_int, c_void, CStr, CString};
use std::path::Path;
use std::rc::Rc;
use std::time::{SystemTime, UNIX_EPOCH};

use super::super::{
    ClassBase, FieldSpec, FunctionValue, MethodValue, ObjectValue, Runtime, TraitValue,
    UserClassValue, Value,
};
use crate::error::{Result, ZuzuRustError};

const WIDGET_CLASSES: &[&str] = &[
    "Widget",
    "Window",
    "VBox",
    "HBox",
    "Frame",
    "Label",
    "Text",
    "RichText",
    "Image",
    "Input",
    "DatePicker",
    "Checkbox",
    "Radio",
    "RadioGroup",
    "Select",
    "Menu",
    "MenuItem",
    "Button",
    "Separator",
    "Slider",
    "Progress",
    "Tabs",
    "Tab",
    "ListView",
    "TreeView",
];

const ALL_CLASSES: &[&str] = &[
    "Widget",
    "Window",
    "VBox",
    "HBox",
    "Frame",
    "Label",
    "Text",
    "RichText",
    "Image",
    "Input",
    "DatePicker",
    "Checkbox",
    "Radio",
    "RadioGroup",
    "Select",
    "Menu",
    "MenuItem",
    "Button",
    "Separator",
    "Slider",
    "Progress",
    "Tabs",
    "Tab",
    "ListView",
    "TreeView",
    "Event",
    "ListenerToken",
];

#[derive(Clone, Copy)]
enum GuiBackend {
    Gtk4,
}

impl GuiBackend {
    fn current() -> Self {
        Self::Gtk4
    }

    fn name(self) -> &'static str {
        match self {
            Self::Gtk4 => "GTK4",
        }
    }
}

pub(super) fn exports() -> HashMap<String, Value> {
    let mut exports = HashMap::new();
    for class_name in ALL_CLASSES {
        exports.insert(
            (*class_name).to_owned(),
            Value::builtin_class((*class_name).to_owned()),
        );
    }
    for function_name in [
        "native_file_open",
        "native_file_save",
        "native_directory_open",
        "native_directory_save",
        "native_alert",
        "native_confirm",
        "native_prompt",
        "native_colour_picker",
    ] {
        exports.insert(
            function_name.to_owned(),
            Value::native_function(function_name.to_owned()),
        );
    }
    exports.insert(
        "meta".to_owned(),
        Value::Dict(HashMap::from([
            (
                "backend".to_owned(),
                Value::String(GuiBackend::current().name().to_owned()),
            ),
            ("font_size_pixels".to_owned(), Value::Number(16.0)),
            ("font_name".to_owned(), Value::String("Sans".to_owned())),
            ("font_point_size".to_owned(), Value::Number(10.0)),
        ])),
    );
    exports
}

pub(super) fn call(runtime: &Runtime, name: &str, args: &[Value]) -> Option<Result<Value>> {
    match name {
        "native_file_open"
        | "native_file_save"
        | "native_directory_open"
        | "native_directory_save" => {
            if args.len() != 1 {
                return Some(Err(ZuzuRustError::runtime(format!(
                    "Wrong number of arguments for function '{name}'"
                ))));
            }
            Some(gtk_backend::native_file_dialog(runtime, name, &args[0]))
        }
        "native_colour_picker" => {
            if args.len() != 1 {
                return Some(Err(ZuzuRustError::runtime(format!(
                    "Wrong number of arguments for function '{name}'"
                ))));
            }
            Some(gtk_backend::native_colour_dialog(runtime, &args[0]))
        }
        "native_alert" => {
            if args.len() != 2 {
                return Some(Err(ZuzuRustError::runtime(format!(
                    "Wrong number of arguments for function '{name}'"
                ))));
            }
            Some(Ok(Value::Boolean(false)))
        }
        "native_confirm" | "native_prompt" => {
            if args.len() != 2 {
                return Some(Err(ZuzuRustError::runtime(format!(
                    "Wrong number of arguments for function '{name}'"
                ))));
            }
            Some(Ok(Value::Null))
        }
        _ => None,
    }
}

pub(in crate::runtime) fn preview_widget(runtime: &Runtime, root: &Value) -> Result<*mut c_void> {
    gtk_backend::preview_widget(runtime, root)
}

pub(super) fn is_gui_class(class_name: &str) -> bool {
    ALL_CLASSES.contains(&class_name)
}

pub(super) fn construct_object(
    runtime: &Runtime,
    class_name: &str,
    args: Vec<Value>,
    named_args: Vec<(String, Value)>,
) -> Result<Value> {
    match class_name {
        "Event" => construct_event(runtime, args, named_args),
        "ListenerToken" => Ok(native_object("ListenerToken", None, HashMap::new())),
        name if WIDGET_CLASSES.contains(&name) => construct_widget(runtime, name, args, named_args),
        _ => Err(ZuzuRustError::runtime(format!(
            "cannot construct GUI class '{class_name}'"
        ))),
    }
}

pub(super) fn has_builtin_object_method(class_name: &str, name: &str) -> bool {
    if class_name == "Event" {
        return matches!(
            name,
            "name"
                | "target"
                | "current_target"
                | "phase"
                | "timestamp"
                | "data"
                | "cancelled"
                | "propagation_stopped"
                | "default_prevented"
                | "window"
                | "stop_propagation"
                | "prevent_default"
        );
    }
    if class_name == "ListenerToken" {
        return false;
    }
    WIDGET_CLASSES.contains(&class_name) && is_widget_method(name)
}

pub(super) fn call_object_method(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Option<Result<Value>> {
    let result = match class_name {
        "Event" => call_event_method(object, name, args),
        "ListenerToken" => return None,
        class_name if WIDGET_CLASSES.contains(&class_name) => {
            call_widget_method(runtime, object, class_name, name, args)
        }
        _ => return None,
    };
    Some(result)
}

fn construct_widget(
    runtime: &Runtime,
    class_name: &str,
    positional: Vec<Value>,
    named: Vec<(String, Value)>,
) -> Result<Value> {
    let mut fields = default_widget_fields(runtime, class_name, &named)?;
    apply_widget_specific_fields(runtime, class_name, &mut fields, &named)?;

    let object = Rc::new(RefCell::new(ObjectValue {
        class: class_named(class_name),
        fields,
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Dict(HashMap::from([(
            "__gui_object".to_owned(),
            Value::Boolean(true),
        )]))),
    }));
    let value = Value::Object(Rc::clone(&object));

    let mut children = positional;
    if let Some(value) = named_value(&named, "children") {
        if let Value::Array(items) = runtime.deref_value(&value)? {
            children.extend(items);
        }
    }
    for child in children {
        if is_widget_value(&child) {
            adopt_child(&value, child)?;
        }
    }
    if class_name == "RadioGroup" {
        sync_radio_group_children(&value);
    }
    if class_name == "Tabs" {
        sync_tabs_children(&value);
    }

    if class_name == "Window" {
        let content = named_value(&named, "content")
            .filter(|value| !matches!(value, Value::Null))
            .or_else(|| first_non_menu_child(&value));
        if let Some(content) = content {
            if !is_widget_value(&content) || is_menu_kind_value(&content) {
                return Err(gui_error(
                    "GUI_PROP_TYPE",
                    "content property expects a non-menu Widget or null",
                ));
            }
            set_field(&value, "content", content.clone());
            adopt_child(&value, content)?;
        }
    }

    Ok(value)
}

fn construct_event(
    runtime: &Runtime,
    args: Vec<Value>,
    named: Vec<(String, Value)>,
) -> Result<Value> {
    let name = named_value(&named, "name")
        .or_else(|| args.first().cloned())
        .map(|value| runtime.render_value(&value))
        .transpose()?
        .unwrap_or_default();
    let target = named_value(&named, "target").unwrap_or(Value::Null);
    let data = named_value(&named, "data")
        .or_else(|| args.get(1).cloned())
        .unwrap_or(Value::Null);
    Ok(make_event(name, target, data))
}

fn default_widget_fields(
    runtime: &Runtime,
    class_name: &str,
    named: &[(String, Value)],
) -> Result<HashMap<String, Value>> {
    let enabled = if let Some(disabled) = named_value(named, "disabled") {
        !runtime.value_is_truthy(&disabled)?
    } else {
        bool_prop(runtime, named, "enabled", true)?
    };
    Ok(HashMap::from([
        ("id".to_owned(), optional_string_prop(runtime, named, "id")?),
        ("parent".to_owned(), Value::Null),
        ("children".to_owned(), Value::Array(Vec::new())),
        (
            "visible".to_owned(),
            Value::Boolean(bool_prop(runtime, named, "visible", true)?),
        ),
        ("enabled".to_owned(), Value::Boolean(enabled)),
        (
            "width".to_owned(),
            optional_number_prop(runtime, named, "width")?,
        ),
        (
            "height".to_owned(),
            optional_number_prop(runtime, named, "height")?,
        ),
        (
            "minwidth".to_owned(),
            optional_number_prop(runtime, named, "minwidth")?,
        ),
        (
            "minheight".to_owned(),
            optional_number_prop(runtime, named, "minheight")?,
        ),
        (
            "maxwidth".to_owned(),
            optional_number_prop(runtime, named, "maxwidth")?,
        ),
        (
            "maxheight".to_owned(),
            optional_number_prop(runtime, named, "maxheight")?,
        ),
        ("classes".to_owned(), array_prop(runtime, named, "classes")?),
        ("style".to_owned(), dict_prop(runtime, named, "style")?),
        ("meta".to_owned(), dict_prop(runtime, named, "meta")?),
        ("listeners".to_owned(), Value::Dict(HashMap::new())),
        ("listener_seq".to_owned(), Value::Number(0.0)),
        (
            "widget_type".to_owned(),
            Value::String(class_name.to_owned()),
        ),
    ]))
}

fn apply_widget_specific_fields(
    runtime: &Runtime,
    class_name: &str,
    fields: &mut HashMap<String, Value>,
    named: &[(String, Value)],
) -> Result<()> {
    match class_name {
        "Window" => {
            fields.insert(
                "title".to_owned(),
                string_prop(runtime, named, "title", "")?,
            );
            fields.insert(
                "width".to_owned(),
                number_prop(runtime, named, "width", 800.0)?,
            );
            fields.insert(
                "height".to_owned(),
                number_prop(runtime, named, "height", 600.0)?,
            );
            fields.insert(
                "resizable".to_owned(),
                Value::Boolean(bool_prop(runtime, named, "resizable", true)?),
            );
            fields.insert(
                "modal".to_owned(),
                Value::Boolean(bool_prop(runtime, named, "modal", false)?),
            );
            fields.insert("shown".to_owned(), Value::Boolean(false));
            fields.insert("closed".to_owned(), Value::Boolean(false));
            fields.insert("close_result".to_owned(), Value::Null);
            fields.insert("content".to_owned(), Value::Null);
        }
        "VBox" => {
            fields.insert(
                "align".to_owned(),
                string_prop(runtime, named, "align", "top")?,
            );
            fields.insert("gap".to_owned(), number_prop(runtime, named, "gap", 0.0)?);
            fields.insert(
                "padding".to_owned(),
                named_value(named, "padding").unwrap_or(Value::Number(0.0)),
            );
        }
        "HBox" => {
            fields.insert(
                "align".to_owned(),
                string_prop(runtime, named, "align", "left")?,
            );
            fields.insert("gap".to_owned(), number_prop(runtime, named, "gap", 0.0)?);
            fields.insert(
                "padding".to_owned(),
                named_value(named, "padding").unwrap_or(Value::Number(0.0)),
            );
        }
        "Frame" => {
            fields.insert(
                "label".to_owned(),
                string_prop(runtime, named, "label", "")?,
            );
            fields.insert(
                "collapsible".to_owned(),
                Value::Boolean(bool_prop(runtime, named, "collapsible", false)?),
            );
            fields.insert(
                "collapsed".to_owned(),
                Value::Boolean(bool_prop(runtime, named, "collapsed", false)?),
            );
        }
        "Label" => {
            fields.insert("text".to_owned(), string_prop(runtime, named, "text", "")?);
            fields.insert(
                "for".to_owned(),
                optional_string_prop(runtime, named, "for")?,
            );
        }
        "Text" => {
            text_like_fields(runtime, fields, named, false, false)?;
            fields.insert(
                "wrap".to_owned(),
                Value::Boolean(bool_prop(runtime, named, "wrap", true)?),
            );
        }
        "RichText" => {
            text_like_fields(runtime, fields, named, true, true)?;
        }
        "Image" => {
            fields.insert("src".to_owned(), string_prop(runtime, named, "src", "")?);
            fields.insert("alt".to_owned(), string_prop(runtime, named, "alt", "")?);
            fields.insert(
                "fit".to_owned(),
                string_prop(runtime, named, "fit", "none")?,
            );
        }
        "Input" => {
            fields.insert(
                "value".to_owned(),
                string_prop(runtime, named, "value", "")?,
            );
            fields.insert(
                "placeholder".to_owned(),
                string_prop(runtime, named, "placeholder", "")?,
            );
            for (key, default) in [
                ("multiline", false),
                ("readonly", false),
                ("password", false),
                ("required", false),
            ] {
                fields.insert(
                    key.to_owned(),
                    Value::Boolean(bool_prop(runtime, named, key, default)?),
                );
            }
        }
        "DatePicker" => {
            fields.insert(
                "value".to_owned(),
                optional_string_prop(runtime, named, "value")?,
            );
            fields.insert(
                "min".to_owned(),
                optional_string_prop(runtime, named, "min")?,
            );
            fields.insert(
                "max".to_owned(),
                optional_string_prop(runtime, named, "max")?,
            );
            fields.insert(
                "first_day_of_week".to_owned(),
                number_prop(runtime, named, "first_day_of_week", 0.0)?,
            );
        }
        "Checkbox" => {
            fields.insert(
                "label".to_owned(),
                string_prop(runtime, named, "label", "")?,
            );
            fields.insert(
                "checked".to_owned(),
                Value::Boolean(bool_prop(runtime, named, "checked", false)?),
            );
            fields.insert(
                "indeterminate".to_owned(),
                Value::Boolean(bool_prop(runtime, named, "indeterminate", false)?),
            );
        }
        "Radio" => {
            fields.insert(
                "label".to_owned(),
                string_prop(runtime, named, "label", "")?,
            );
            fields.insert(
                "value".to_owned(),
                string_prop(runtime, named, "value", "")?,
            );
            fields.insert(
                "group".to_owned(),
                named_value(named, "group").unwrap_or(Value::Null),
            );
            fields.insert(
                "checked".to_owned(),
                Value::Boolean(bool_prop(runtime, named, "checked", false)?),
            );
        }
        "RadioGroup" => {
            fields.insert("name".to_owned(), string_prop(runtime, named, "name", "")?);
            fields.insert(
                "value".to_owned(),
                named_value(named, "value").unwrap_or(Value::Null),
            );
        }
        "Select" => {
            fields.insert(
                "value".to_owned(),
                named_value(named, "value").unwrap_or(Value::Null),
            );
            fields.insert("options".to_owned(), array_prop(runtime, named, "options")?);
            fields.insert(
                "multiple".to_owned(),
                Value::Boolean(bool_prop(runtime, named, "multiple", false)?),
            );
        }
        "Menu" | "MenuItem" | "Button" => {
            fields.insert("text".to_owned(), string_prop(runtime, named, "text", "")?);
            if class_name == "Button" {
                fields.insert(
                    "variant".to_owned(),
                    string_prop(runtime, named, "variant", "default")?,
                );
            }
        }
        "Separator" => {
            fields.insert(
                "orientation".to_owned(),
                string_prop(runtime, named, "orientation", "horizontal")?,
            );
        }
        "Slider" => {
            fields.insert(
                "value".to_owned(),
                number_prop(runtime, named, "value", 0.0)?,
            );
            fields.insert("min".to_owned(), number_prop(runtime, named, "min", 0.0)?);
            fields.insert("max".to_owned(), number_prop(runtime, named, "max", 100.0)?);
            fields.insert("step".to_owned(), number_prop(runtime, named, "step", 1.0)?);
            fields.insert(
                "orientation".to_owned(),
                string_prop(runtime, named, "orientation", "horizontal")?,
            );
            fields.insert(
                "readonly".to_owned(),
                Value::Boolean(bool_prop(runtime, named, "readonly", false)?),
            );
        }
        "Progress" => {
            fields.insert(
                "value".to_owned(),
                number_prop(runtime, named, "value", 0.0)?,
            );
            fields.insert("min".to_owned(), number_prop(runtime, named, "min", 0.0)?);
            fields.insert("max".to_owned(), number_prop(runtime, named, "max", 100.0)?);
            fields.insert(
                "indeterminate".to_owned(),
                Value::Boolean(bool_prop(runtime, named, "indeterminate", false)?),
            );
            fields.insert(
                "show_text".to_owned(),
                Value::Boolean(bool_prop(runtime, named, "show_text", false)?),
            );
        }
        "Tabs" => {
            fields.insert(
                "selected".to_owned(),
                named_value(named, "selected")
                    .or_else(|| named_value(named, "value"))
                    .unwrap_or(Value::Null),
            );
            fields.insert(
                "placement".to_owned(),
                string_prop(runtime, named, "placement", "top")?,
            );
        }
        "Tab" => {
            fields.insert(
                "title".to_owned(),
                string_prop(runtime, named, "title", "")?,
            );
            fields.insert(
                "value".to_owned(),
                string_prop(runtime, named, "value", "")?,
            );
            fields.insert(
                "selected".to_owned(),
                Value::Boolean(bool_prop(runtime, named, "selected", false)?),
            );
            fields.insert(
                "closable".to_owned(),
                Value::Boolean(bool_prop(runtime, named, "closable", false)?),
            );
            fields.insert(
                "icon".to_owned(),
                optional_string_prop(runtime, named, "icon")?,
            );
        }
        "ListView" => {
            fields.insert("items".to_owned(), array_prop(runtime, named, "items")?);
            fields.insert(
                "selected_index".to_owned(),
                optional_number_prop(runtime, named, "selected_index")?,
            );
            fields.insert(
                "multiple".to_owned(),
                Value::Boolean(bool_prop(runtime, named, "multiple", false)?),
            );
        }
        "TreeView" => {
            fields.insert("items".to_owned(), array_prop(runtime, named, "items")?);
            fields.insert(
                "selected_path".to_owned(),
                array_prop(runtime, named, "selected_path")?,
            );
            fields.insert(
                "multiple".to_owned(),
                Value::Boolean(bool_prop(runtime, named, "multiple", false)?),
            );
            fields.insert(
                "expanded_path_keys".to_owned(),
                Value::Dict(initial_expanded_tree_paths(&array_field_map(
                    fields, "items",
                ))),
            );
        }
        _ => {}
    }
    Ok(())
}

fn text_like_fields(
    runtime: &Runtime,
    fields: &mut HashMap<String, Value>,
    named: &[(String, Value)],
    default_multiline: bool,
    default_readonly: bool,
) -> Result<()> {
    fields.insert(
        "value".to_owned(),
        string_prop(runtime, named, "value", "")?,
    );
    fields.insert(
        "multiline".to_owned(),
        Value::Boolean(bool_prop(runtime, named, "multiline", default_multiline)?),
    );
    fields.insert(
        "readonly".to_owned(),
        Value::Boolean(bool_prop(runtime, named, "readonly", default_readonly)?),
    );
    Ok(())
}

fn call_event_method(
    object: &Rc<RefCell<ObjectValue>>,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    require_arity(name, args, 0)?;
    let value = Value::Object(Rc::clone(object));
    match name {
        "name"
        | "target"
        | "current_target"
        | "phase"
        | "timestamp"
        | "data"
        | "cancelled"
        | "propagation_stopped"
        | "default_prevented" => Ok(field(&value, name)),
        "window" => Ok(owner_window(&field(&value, "target")).unwrap_or(Value::Null)),
        "stop_propagation" => {
            set_field(&value, "propagation_stopped", Value::Boolean(true));
            set_field(&value, "cancelled", Value::Boolean(true));
            Ok(value)
        }
        "prevent_default" => {
            set_field(&value, "default_prevented", Value::Boolean(true));
            set_field(&value, "cancelled", Value::Boolean(true));
            Ok(value)
        }
        _ => Err(unsupported_method(name, "Event")),
    }
}

fn call_widget_method(
    runtime: &Runtime,
    object: &Rc<RefCell<ObjectValue>>,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    let value = Value::Object(Rc::clone(object));
    match name {
        "id" => getter(&value, name, args, "id"),
        "set_id" => set_string(runtime, &value, name, args, "id"),
        "parent" => getter(&value, name, args, "parent"),
        "children" => clone_array_getter(&value, name, args, "children"),
        "add_child" => {
            require_arity(name, args, 1)?;
            adopt_child(&value, args[0].clone())?;
            Ok(value)
        }
        "remove_child" => {
            require_arity(name, args, 1)?;
            remove_child(&value, &args[0])?;
            Ok(value)
        }
        "enabled" => bool_getter(&value, name, args, "enabled"),
        "set_enabled" => set_bool(runtime, &value, name, args, "enabled"),
        "visible" => bool_getter(&value, name, args, "visible"),
        "set_visible" => set_bool(runtime, &value, name, args, "visible"),
        "width" | "height" | "minwidth" | "minheight" | "maxwidth" | "maxheight" => {
            number_accessor(runtime, &value, name, args, name)
        }
        "classes" => clone_array_getter(&value, name, args, "classes"),
        "add_class" => add_class(runtime, &value, name, args),
        "remove_class" => remove_class(runtime, &value, name, args),
        "style" => map_accessor(runtime, &value, name, args, "style"),
        "meta" => map_accessor(runtime, &value, name, args, "meta"),
        "on" => add_listener(runtime, &value, name, args, false),
        "once" => add_listener(runtime, &value, name, args, true),
        "off" => remove_listener(&value, name, args),
        "emit" => emit(runtime, &value, name, args),
        "find_by_id" => {
            require_arity(name, args, 1)?;
            let id = runtime.render_value(&args[0])?;
            Ok(find_by_id(&value, &id).unwrap_or(Value::Null))
        }
        event if is_event_alias(event) => {
            if args.is_empty() {
                emit(runtime, &value, event, &[])
            } else {
                add_listener(runtime, &value, event, &[args[0].clone()], false)
            }
        }
        "show" if class_name == "Window" => show_window(runtime, &value, name, args),
        "call" if class_name == "Window" => {
            require_arity(name, args, 0)?;
            if !field(&value, "closed").is_truthy() {
                if !field(&value, "shown").is_truthy() {
                    show_window(runtime, &value, "show", &[])?;
                }
                gtk_backend::run_window(runtime, &value)?;
                if !field(&value, "closed").is_truthy() {
                    set_field(&value, "closed", Value::Boolean(true));
                    set_field(&value, "close_result", Value::Null);
                    let _ = dispatch_named_event(runtime, &value, "closed", Value::Null)?;
                }
            }
            Ok(field(&value, "close_result"))
        }
        "close" if class_name == "Window" => close_window(runtime, &value, name, args),
        "content" if class_name == "Window" => getter(&value, name, args, "content"),
        "set_content" if class_name == "Window" => set_content(&value, name, args),
        "menus" if class_name == "Window" => {
            require_arity(name, args, 0)?;
            Ok(Value::Array(menu_widgets(&value)))
        }
        "title" if class_name == "Window" => text_accessor(runtime, &value, name, args, "title"),
        "set_title" if class_name == "Window" => set_string(runtime, &value, name, args, "title"),
        "align" | "gap" | "padding" if matches!(class_name, "VBox" | "HBox") => {
            getter(&value, name, args, name)
        }
        "text" if matches!(class_name, "Label" | "Menu" | "MenuItem" | "Button") => {
            text_accessor(runtime, &value, name, args, "text")
        }
        "set_text" if matches!(class_name, "Label" | "Menu" | "MenuItem" | "Button") => {
            set_string(runtime, &value, name, args, "text")
        }
        "for_id" if class_name == "Label" => getter(&value, name, args, "for"),
        "set_for_id" if class_name == "Label" => set_string(runtime, &value, name, args, "for"),
        "value" if matches!(class_name, "Input" | "Text" | "RichText") => {
            text_accessor(runtime, &value, name, args, "value")
        }
        "set_value" if matches!(class_name, "Input" | "Text" | "RichText") => {
            set_string(runtime, &value, name, args, "value")
        }
        "placeholder" if class_name == "Input" => {
            text_accessor(runtime, &value, name, args, "placeholder")
        }
        "set_placeholder" if class_name == "Input" => {
            set_string(runtime, &value, name, args, "placeholder")
        }
        "items" if class_name == "Menu" => clone_array_getter(&value, name, args, "children"),
        "disabled" if class_name == "MenuItem" => {
            if args.is_empty() {
                Ok(Value::Boolean(!field(&value, "enabled").is_truthy()))
            } else {
                require_arity(name, args, 1)?;
                set_field(
                    &value,
                    "enabled",
                    Value::Boolean(!runtime.value_is_truthy(&args[0])?),
                );
                Ok(value)
            }
        }
        "variant" if class_name == "Button" => getter(&value, name, args, "variant"),
        "label" if matches!(class_name, "Frame" | "Checkbox" | "Radio") => {
            text_accessor(runtime, &value, name, args, "label")
        }
        "collapsible" if class_name == "Frame" => {
            bool_accessor(runtime, &value, name, args, "collapsible")
        }
        "collapsed" if class_name == "Frame" => {
            bool_accessor(runtime, &value, name, args, "collapsed")
        }
        "wrap" if class_name == "Text" => bool_accessor(runtime, &value, name, args, "wrap"),
        "readonly" if matches!(class_name, "Input" | "Text" | "RichText" | "Slider") => {
            bool_accessor(runtime, &value, name, args, "readonly")
        }
        "multiline" if matches!(class_name, "Input" | "Text" | "RichText") => {
            bool_accessor(runtime, &value, name, args, "multiline")
        }
        "password" | "required" if class_name == "Input" => {
            bool_accessor(runtime, &value, name, args, name)
        }
        "select_all" if class_name == "Input" => {
            require_arity(name, args, 0)?;
            Ok(value)
        }
        "src" | "alt" | "fit" if class_name == "Image" => {
            text_accessor(runtime, &value, name, args, name)
        }
        "value" if class_name == "Tab" => text_accessor(runtime, &value, name, args, "value"),
        "set_value" if class_name == "Tab" => set_string(runtime, &value, name, args, "value"),
        "checked" if matches!(class_name, "Checkbox" | "Radio") => {
            checked_accessor(runtime, &value, class_name, name, args)
        }
        "indeterminate" if class_name == "Checkbox" => {
            bool_accessor(runtime, &value, name, args, "indeterminate")
        }
        "name" if class_name == "RadioGroup" => text_accessor(runtime, &value, name, args, "name"),
        "options" if class_name == "RadioGroup" => {
            require_arity(name, args, 0)?;
            Ok(Value::Array(radio_children(&value)))
        }
        "value" if matches!(class_name, "Radio" | "RadioGroup" | "Select" | "DatePicker") => {
            value_accessor(runtime, &value, class_name, name, args)
        }
        "set_value" if class_name == "DatePicker" => {
            value_accessor(runtime, &value, class_name, name, args)
        }
        "min" | "max" if class_name == "DatePicker" => {
            text_accessor(runtime, &value, name, args, name)
        }
        "first_day_of_week" if class_name == "DatePicker" => {
            number_accessor(runtime, &value, name, args, "first_day_of_week")
        }
        "group" if class_name == "Radio" => getter(&value, name, args, "group"),
        "options" if class_name == "Select" => clone_array_getter(&value, name, args, "options"),
        "add_option" if class_name == "Select" => add_option(&value, name, args),
        "clear_options" if class_name == "Select" => {
            require_arity(name, args, 0)?;
            set_field(&value, "options", Value::Array(Vec::new()));
            Ok(value)
        }
        "multiple" if class_name == "Select" => {
            bool_accessor(runtime, &value, name, args, "multiple")
        }
        "orientation" if matches!(class_name, "Separator" | "Slider") => {
            getter(&value, name, args, "orientation")
        }
        "value" if matches!(class_name, "Slider" | "Progress") => {
            number_accessor(runtime, &value, name, args, "value")
        }
        "min" | "max" if matches!(class_name, "Slider" | "Progress") => {
            number_accessor(runtime, &value, name, args, name)
        }
        "step" if class_name == "Slider" => number_accessor(runtime, &value, name, args, "step"),
        "indeterminate" if class_name == "Progress" => {
            bool_accessor(runtime, &value, name, args, "indeterminate")
        }
        "show_text" if class_name == "Progress" => {
            bool_accessor(runtime, &value, name, args, "show_text")
        }
        "placement" if class_name == "Tabs" => getter(&value, name, args, "placement"),
        "selected" | "value" if class_name == "Tabs" => {
            tabs_selected_accessor(runtime, &value, name, args)
        }
        "tabs" if class_name == "Tabs" => {
            require_arity(name, args, 0)?;
            Ok(Value::Array(tab_children(&value)))
        }
        "selected_tab" if class_name == "Tabs" => {
            require_arity(name, args, 0)?;
            Ok(selected_tab(&value).unwrap_or(Value::Null))
        }
        "title" if class_name == "Tab" => text_accessor(runtime, &value, name, args, "title"),
        "set_title" if class_name == "Tab" => set_string(runtime, &value, name, args, "title"),
        "icon" if class_name == "Tab" => text_accessor(runtime, &value, name, args, "icon"),
        "set_icon" if class_name == "Tab" => set_string(runtime, &value, name, args, "icon"),
        "closable" if class_name == "Tab" => bool_accessor(runtime, &value, name, args, "closable"),
        "selected" if class_name == "Tab" => tab_selected_accessor(runtime, &value, name, args),
        "items" if matches!(class_name, "ListView" | "TreeView") => {
            clone_array_getter(&value, name, args, "items")
        }
        "selected_index" if class_name == "ListView" => {
            selected_index_accessor(runtime, &value, name, args)
        }
        "selected_item" if class_name == "ListView" => {
            require_arity(name, args, 0)?;
            Ok(list_selected_item(&value))
        }
        "add_item" if matches!(class_name, "ListView" | "TreeView") => add_item(&value, name, args),
        "clear_items" if matches!(class_name, "ListView" | "TreeView") => {
            clear_items(&value, class_name, name, args)
        }
        "activate_index" if class_name == "ListView" => activate_index(runtime, &value, name, args),
        "multiple" if matches!(class_name, "ListView" | "TreeView") => {
            bool_accessor(runtime, &value, name, args, "multiple")
        }
        "selected_path" if class_name == "TreeView" => {
            selected_path_accessor(runtime, &value, name, args)
        }
        "selected_item" if class_name == "TreeView" => {
            require_arity(name, args, 0)?;
            Ok(tree_selected_item(&value))
        }
        "activate_path" if class_name == "TreeView" => {
            tree_path_event(runtime, &value, name, args, "activate")
        }
        "expand_path" if class_name == "TreeView" => {
            tree_expand_collapse(runtime, &value, name, args, true)
        }
        "collapse_path" if class_name == "TreeView" => {
            tree_expand_collapse(runtime, &value, name, args, false)
        }
        "is_expanded" if class_name == "TreeView" => is_tree_expanded(&value, name, args),
        _ => Err(unsupported_method(name, class_name)),
    }
}

fn is_widget_method(name: &str) -> bool {
    matches!(
        name,
        "id" | "set_id"
            | "parent"
            | "children"
            | "add_child"
            | "remove_child"
            | "enabled"
            | "set_enabled"
            | "visible"
            | "set_visible"
            | "width"
            | "height"
            | "minwidth"
            | "minheight"
            | "maxwidth"
            | "maxheight"
            | "classes"
            | "add_class"
            | "remove_class"
            | "style"
            | "meta"
            | "on"
            | "once"
            | "off"
            | "emit"
            | "find_by_id"
            | "show"
            | "call"
            | "close"
            | "content"
            | "set_content"
            | "menus"
            | "title"
            | "set_title"
            | "text"
            | "set_text"
            | "value"
            | "set_value"
            | "placeholder"
            | "set_placeholder"
            | "select_all"
            | "label"
            | "collapsible"
            | "collapsed"
            | "wrap"
            | "readonly"
            | "multiline"
            | "password"
            | "required"
            | "src"
            | "alt"
            | "fit"
            | "checked"
            | "indeterminate"
            | "name"
            | "options"
            | "add_option"
            | "clear_options"
            | "multiple"
            | "group"
            | "min"
            | "max"
            | "first_day_of_week"
            | "orientation"
            | "step"
            | "show_text"
            | "placement"
            | "selected"
            | "tabs"
            | "selected_tab"
            | "icon"
            | "set_icon"
            | "closable"
            | "selected_index"
            | "selected_item"
            | "add_item"
            | "clear_items"
            | "activate_index"
            | "selected_path"
            | "activate_path"
            | "expand_path"
            | "collapse_path"
            | "is_expanded"
            | "items"
            | "disabled"
            | "variant"
    ) || is_event_alias(name)
}

fn is_event_alias(name: &str) -> bool {
    matches!(
        name,
        "activate"
            | "blur"
            | "change"
            | "click"
            | "close_request"
            | "closed"
            | "collapse"
            | "enter"
            | "expand"
            | "focus"
            | "input"
            | "open"
            | "resize"
            | "select"
            | "submit"
    )
}

fn show_window(runtime: &Runtime, window: &Value, name: &str, args: &[Value]) -> Result<Value> {
    require_arity(name, args, 0)?;
    gtk_backend::show_window(runtime, window)?;
    set_field(window, "shown", Value::Boolean(true));
    let _ = dispatch_named_event(runtime, window, "open", Value::Null)?;
    Ok(window.clone())
}

fn close_window(runtime: &Runtime, window: &Value, name: &str, args: &[Value]) -> Result<Value> {
    if args.len() > 1 {
        return Err(ZuzuRustError::runtime(format!(
            "{name}() expects at most 1 argument"
        )));
    }
    if field(window, "closed").is_truthy() {
        return Ok(window.clone());
    }
    let result = args.first().cloned().unwrap_or(Value::Null);
    let event = dispatch_named_event(runtime, window, "close_request", result.clone())?;
    if event_cancelled(&event) {
        return Ok(window.clone());
    }
    set_field(window, "closed", Value::Boolean(true));
    set_field(window, "close_result", result.clone());
    gtk_backend::destroy_window(window);
    let _ = dispatch_named_event(runtime, window, "closed", result)?;
    Ok(window.clone())
}

fn set_content(window: &Value, name: &str, args: &[Value]) -> Result<Value> {
    require_arity(name, args, 1)?;
    let child = args[0].clone();
    if !matches!(child, Value::Null) && (!is_widget_value(&child) || is_menu_kind_value(&child)) {
        return Err(gui_error(
            "GUI_PROP_TYPE",
            "set_content expects a non-menu Widget or null",
        ));
    }

    let old_children = children_values(window);
    let mut kept = Vec::new();
    for old in old_children {
        if is_menu_kind_value(&old) {
            kept.push(old);
        } else {
            set_weak_field(&old, "parent", Value::Null);
        }
    }
    set_field(window, "children", Value::Array(kept));
    set_field(window, "content", child.clone());
    if !matches!(child, Value::Null) {
        adopt_child(window, child)?;
    }
    Ok(window.clone())
}

fn add_listener(
    runtime: &Runtime,
    widget: &Value,
    name: &str,
    args: &[Value],
    once: bool,
) -> Result<Value> {
    require_arity(
        name,
        args,
        if name == "on" || name == "once" { 2 } else { 1 },
    )?;
    let (event_name, handler) = if name == "on" || name == "once" {
        (runtime.render_value(&args[0])?, args[1].clone())
    } else {
        (name.to_owned(), args[0].clone())
    };
    if !matches!(handler, Value::Function(_)) {
        return Err(gui_error(
            "GUI_EVENT_HANDLER",
            &format!("{name} expects a Function handler"),
        ));
    }
    let id = field(widget, "listener_seq").to_number()? + 1.0;
    set_field(widget, "listener_seq", Value::Number(id));

    let listener = Value::Dict(HashMap::from([
        ("id".to_owned(), Value::Number(id)),
        ("handler".to_owned(), handler),
        ("once".to_owned(), Value::Boolean(once)),
        ("capture".to_owned(), Value::Boolean(false)),
    ]));
    let mut listeners = match field(widget, "listeners") {
        Value::Dict(map) => map,
        _ => HashMap::new(),
    };
    match listeners.get_mut(&event_name) {
        Some(Value::Array(items)) => items.push(listener),
        _ => {
            listeners.insert(event_name.clone(), Value::Array(vec![listener]));
        }
    }
    set_field(widget, "listeners", Value::Dict(listeners));
    Ok(listener_token(widget.clone(), event_name, id))
}

fn remove_listener(widget: &Value, name: &str, args: &[Value]) -> Result<Value> {
    require_arity(name, args, 1)?;
    let Value::Object(token) = &args[0] else {
        return Ok(Value::Boolean(false));
    };
    if token.borrow().class.name != "ListenerToken" {
        return Ok(Value::Boolean(false));
    }
    let token_widget = field(&args[0], "widget");
    if !same_object(widget, &token_widget) {
        return Ok(Value::Boolean(false));
    }
    let event_name = match field(&args[0], "event") {
        Value::String(value) => value,
        _ => String::new(),
    };
    let id = field(&args[0], "id").to_number()?;
    let mut listeners = match field(widget, "listeners") {
        Value::Dict(map) => map,
        _ => HashMap::new(),
    };
    let before = listeners
        .get(&event_name)
        .and_then(|value| match value {
            Value::Array(items) => Some(items.len()),
            _ => None,
        })
        .unwrap_or(0);
    if let Some(Value::Array(items)) = listeners.get_mut(&event_name) {
        items.retain(|listener| listener_id(listener) != Some(id));
    }
    let after = listeners
        .get(&event_name)
        .and_then(|value| match value {
            Value::Array(items) => Some(items.len()),
            _ => None,
        })
        .unwrap_or(0);
    set_field(widget, "listeners", Value::Dict(listeners));
    Ok(Value::Boolean(after != before))
}

fn emit(runtime: &Runtime, widget: &Value, name: &str, args: &[Value]) -> Result<Value> {
    if name == "emit" && args.is_empty() {
        return Err(ZuzuRustError::runtime("emit() expects an event name"));
    }
    let (event_name, payload) = if name == "emit" {
        (
            runtime.render_value(&args[0])?,
            args.get(1).cloned().unwrap_or(Value::Null),
        )
    } else {
        (
            name.to_owned(),
            args.first().cloned().unwrap_or(Value::Null),
        )
    };
    let event = if matches!(&payload, Value::Object(object) if object.borrow().class.name == "Event")
    {
        payload
    } else {
        make_event(event_name.clone(), widget.clone(), payload)
    };
    if matches!(field(&event, "name"), Value::Null) {
        set_field(&event, "name", Value::String(event_name));
    }
    if matches!(field(&event, "target"), Value::Null) {
        set_weak_field(&event, "target", widget.clone());
    }
    dispatch_event(runtime, widget, event)
}

fn dispatch_named_event(
    runtime: &Runtime,
    widget: &Value,
    name: &str,
    data: Value,
) -> Result<Value> {
    dispatch_event(
        runtime,
        widget,
        make_event(name.to_owned(), widget.clone(), data),
    )
}

fn dispatch_event(runtime: &Runtime, source: &Value, event: Value) -> Result<Value> {
    let name = match field(&event, "name") {
        Value::String(name) => name,
        other => runtime.render_value(&other)?,
    };
    let mut path = Vec::new();
    let mut current = source.clone();
    loop {
        path.push(current.clone());
        let parent = field(&current, "parent");
        if !is_widget_value(&parent) {
            break;
        }
        current = parent;
    }

    for widget in path.iter().rev().skip(1) {
        dispatch_at(runtime, widget, &event, &name, "capture")?;
        if field(&event, "propagation_stopped").is_truthy() {
            return Ok(event);
        }
    }
    dispatch_at(runtime, source, &event, &name, "target")?;
    for widget in path.iter().skip(1) {
        if field(&event, "propagation_stopped").is_truthy() {
            return Ok(event);
        }
        dispatch_at(runtime, widget, &event, &name, "bubble")?;
    }
    Ok(event)
}

fn dispatch_at(
    runtime: &Runtime,
    widget: &Value,
    event: &Value,
    name: &str,
    phase: &str,
) -> Result<()> {
    let listeners = match field(widget, "listeners") {
        Value::Dict(map) => map,
        _ => HashMap::new(),
    };
    let current = match listeners.get(name) {
        Some(Value::Array(items)) => items.clone(),
        _ => return Ok(()),
    };
    set_weak_field(event, "current_target", widget.clone());
    set_field(event, "phase", Value::String(phase.to_owned()));

    let mut remaining = Vec::new();
    for listener in current {
        if phase == "capture" && !dict_bool(&listener, "capture") {
            remaining.push(listener);
            continue;
        }
        call_handler(runtime, &listener, event.clone())?;
        if !dict_bool(&listener, "once") {
            remaining.push(listener);
        }
        if field(event, "propagation_stopped").is_truthy() {
            break;
        }
    }

    let mut listeners = match field(widget, "listeners") {
        Value::Dict(map) => map,
        _ => HashMap::new(),
    };
    listeners.insert(name.to_owned(), Value::Array(remaining));
    set_field(widget, "listeners", Value::Dict(listeners));
    Ok(())
}

fn call_handler(runtime: &Runtime, listener: &Value, event: Value) -> Result<()> {
    let handler = dict_field(listener, "handler");
    let args = match &handler {
        Value::Function(function) if handler_wants_event(function) => vec![event],
        Value::Function(_) => Vec::new(),
        _ => {
            return Err(gui_error(
                "GUI_EVENT_HANDLER",
                "listener is not a Function handler",
            ))
        }
    };
    let _ = runtime.call_value(handler, args, Vec::new())?;
    Ok(())
}

fn handler_wants_event(function: &FunctionValue) -> bool {
    !function.params.is_empty()
}

fn make_event(name: String, target: Value, data: Value) -> Value {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs_f64())
        .unwrap_or(0.0);
    let event = native_object(
        "Event",
        None,
        HashMap::from([
            ("name".to_owned(), Value::String(name)),
            ("target".to_owned(), Value::Null),
            ("current_target".to_owned(), Value::Null),
            ("timestamp".to_owned(), Value::Number(timestamp)),
            ("data".to_owned(), data),
            ("cancelled".to_owned(), Value::Boolean(false)),
            ("propagation_stopped".to_owned(), Value::Boolean(false)),
            ("default_prevented".to_owned(), Value::Boolean(false)),
            ("phase".to_owned(), Value::String(String::new())),
        ]),
    );
    set_weak_field(&event, "target", target);
    event
}

fn listener_token(widget: Value, event: String, id: f64) -> Value {
    let token = native_object(
        "ListenerToken",
        None,
        HashMap::from([
            ("widget".to_owned(), Value::Null),
            ("event".to_owned(), Value::String(event)),
            ("id".to_owned(), Value::Number(id)),
        ]),
    );
    set_weak_field(&token, "widget", widget);
    token
}

fn native_object(class_name: &str, parent: Option<&str>, fields: HashMap<String, Value>) -> Value {
    Value::Object(Rc::new(RefCell::new(ObjectValue {
        class: class_named_with_parent(class_name, parent),
        fields,
        weak_fields: std::collections::HashSet::new(),
        builtin_value: Some(Value::Dict(HashMap::from([(
            "__gui_object".to_owned(),
            Value::Boolean(true),
        )]))),
    })))
}

fn class_named(name: &str) -> Rc<UserClassValue> {
    let parent = if name == "Widget" || !WIDGET_CLASSES.contains(&name) {
        None
    } else {
        Some("Widget")
    };
    class_named_with_parent(name, parent)
}

fn class_named_with_parent(name: &str, parent: Option<&str>) -> Rc<UserClassValue> {
    Rc::new(UserClassValue {
        name: name.to_owned(),
        base: parent.map(|parent| ClassBase::Builtin(parent.to_owned())),
        traits: Vec::<Rc<TraitValue>>::new(),
        fields: Vec::<FieldSpec>::new(),
        methods: HashMap::<String, Rc<MethodValue>>::new(),
        static_methods: HashMap::<String, Rc<MethodValue>>::new(),
        nested_classes: HashMap::new(),
        source_decl: None,
        closure_env: None,
    })
}

fn adopt_child(parent: &Value, child: Value) -> Result<()> {
    if !is_widget_value(&child) {
        return Err(gui_error("GUI_PROP_TYPE", "child must be a Widget"));
    }
    let old_parent = field(&child, "parent");
    if is_widget_value(&old_parent) && !same_object(parent, &old_parent) {
        remove_child(&old_parent, &child)?;
    }

    let mut children = children_values(parent);
    if !children.iter().any(|item| same_object(item, &child)) {
        children.push(child.clone());
    }
    set_field(parent, "children", Value::Array(children));
    set_weak_field(&child, "parent", parent.clone());
    Ok(())
}

fn remove_child(parent: &Value, child: &Value) -> Result<()> {
    if !is_widget_value(child) {
        return Ok(());
    }
    let children = children_values(parent)
        .into_iter()
        .filter(|item| !same_object(item, child))
        .collect::<Vec<_>>();
    set_field(parent, "children", Value::Array(children));
    if same_object(&field(child, "parent"), parent) {
        set_weak_field(child, "parent", Value::Null);
    }
    Ok(())
}

fn first_non_menu_child(widget: &Value) -> Option<Value> {
    children_values(widget)
        .into_iter()
        .find(|child| !is_menu_kind_value(child))
}

fn menu_widgets(widget: &Value) -> Vec<Value> {
    children_values(widget)
        .into_iter()
        .filter(is_menu_kind_value)
        .collect()
}

fn radio_children(widget: &Value) -> Vec<Value> {
    children_values(widget)
        .into_iter()
        .filter(|child| matches!(class_name(child).as_deref(), Some("Radio")))
        .collect()
}

fn tab_children(widget: &Value) -> Vec<Value> {
    children_values(widget)
        .into_iter()
        .filter(|child| matches!(class_name(child).as_deref(), Some("Tab")))
        .collect()
}

fn parent_radio_group(widget: &Value) -> Option<Value> {
    let parent = field(widget, "parent");
    matches!(class_name(&parent).as_deref(), Some("RadioGroup")).then_some(parent)
}

fn parent_tabs(widget: &Value) -> Option<Value> {
    let parent = field(widget, "parent");
    matches!(class_name(&parent).as_deref(), Some("Tabs")).then_some(parent)
}

fn sync_radio_group_children(group: &Value) {
    let selected = field(group, "value");
    for radio in radio_children(group) {
        let checked =
            !matches!(selected, Value::Null) && values_equal(&field(&radio, "value"), &selected);
        set_field(&radio, "checked", Value::Boolean(checked));
    }
}

fn sync_tabs_children(tabs: &Value) {
    let mut selected = field(tabs, "selected");
    if matches!(selected, Value::Null) {
        for tab in tab_children(tabs) {
            if field(&tab, "selected").is_truthy() {
                selected = field(&tab, "value");
                set_field(tabs, "selected", selected.clone());
                break;
            }
        }
    }
    for tab in tab_children(tabs) {
        let checked =
            !matches!(selected, Value::Null) && values_equal(&field(&tab, "value"), &selected);
        set_field(&tab, "selected", Value::Boolean(checked));
    }
}

fn selected_tab(tabs: &Value) -> Option<Value> {
    tab_children(tabs)
        .into_iter()
        .find(|tab| field(tab, "selected").is_truthy())
}

fn find_by_id(widget: &Value, id: &str) -> Option<Value> {
    if matches!(field(widget, "id"), Value::String(value) if value == id) {
        return Some(widget.clone());
    }
    for child in children_values(widget) {
        if let Some(found) = find_by_id(&child, id) {
            return Some(found);
        }
    }
    None
}

fn owner_window(widget: &Value) -> Option<Value> {
    if !is_widget_value(widget) {
        return None;
    }
    let mut current = widget.clone();
    loop {
        if matches!(class_name(&current).as_deref(), Some("Window")) {
            return Some(current);
        }
        let parent = field(&current, "parent");
        if !is_widget_value(&parent) {
            return None;
        }
        current = parent;
    }
}

fn is_widget_value(value: &Value) -> bool {
    let Value::Object(object) = value else {
        return false;
    };
    class_matches_name(&object.borrow().class, "Widget")
}

fn is_menu_kind_value(value: &Value) -> bool {
    matches!(class_name(value).as_deref(), Some("Menu" | "MenuItem"))
}

fn class_matches_name(class: &Rc<UserClassValue>, name: &str) -> bool {
    if class.name == name {
        return true;
    }
    match &class.base {
        Some(ClassBase::Builtin(base)) => base == name,
        Some(ClassBase::User(base)) => class_matches_name(base, name),
        None => false,
    }
}

fn class_name(value: &Value) -> Option<String> {
    match value {
        Value::Object(object) => Some(object.borrow().class.name.clone()),
        _ => None,
    }
}

fn same_object(left: &Value, right: &Value) -> bool {
    matches!((left, right), (Value::Object(a), Value::Object(b)) if Rc::ptr_eq(a, b))
}

fn values_equal(left: &Value, right: &Value) -> bool {
    if let Value::Shared(value) = left {
        return values_equal(&value.borrow(), right);
    }
    if let Value::Shared(value) = right {
        return values_equal(left, &value.borrow());
    }
    match (left, right) {
        (Value::Null, Value::Null) => true,
        (Value::Boolean(a), Value::Boolean(b)) => a == b,
        (Value::Number(a), Value::Number(b)) => (*a - *b).abs() < f64::EPSILON,
        (Value::String(a), Value::String(b)) => a == b,
        _ => same_object(left, right),
    }
}

fn children_values(widget: &Value) -> Vec<Value> {
    match field(widget, "children") {
        Value::Array(items) => items,
        _ => Vec::new(),
    }
}

fn field(value: &Value, name: &str) -> Value {
    match value {
        Value::Object(object) => object
            .borrow()
            .fields
            .get(name)
            .cloned()
            .map(|value| value.resolve_weak_value())
            .unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn set_field(value: &Value, name: &str, new_value: Value) {
    if let Value::Object(object) = value {
        object
            .borrow_mut()
            .fields
            .insert(name.to_owned(), new_value);
    }
    gtk_backend::sync_property(value, name);
}

fn set_weak_field(value: &Value, name: &str, new_value: Value) {
    if let Value::Object(object) = value {
        let stored = new_value.stored_with_weak_policy(true);
        let mut object = object.borrow_mut();
        object.weak_fields.insert(name.to_owned());
        object.fields.insert(name.to_owned(), stored);
    }
    gtk_backend::sync_property(value, name);
}

fn dict_field(value: &Value, name: &str) -> Value {
    match value {
        Value::Dict(map) => map.get(name).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn dict_bool(value: &Value, name: &str) -> bool {
    dict_field(value, name).is_truthy()
}

fn dialog_prop(props: &Value, name: &str) -> Option<Value> {
    match unshared_value(props) {
        Value::Dict(map) => map.get(name).cloned(),
        Value::PairList(pairs) => pairs
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| value.clone()),
        _ => None,
    }
}

fn dialog_string_prop(
    runtime: &Runtime,
    props: &Value,
    name: &str,
    default: &str,
) -> Result<String> {
    match dialog_prop(props, name) {
        Some(Value::Null) | None => Ok(default.to_owned()),
        Some(value) => runtime.render_value(&value),
    }
}

fn dialog_bool_prop(runtime: &Runtime, props: &Value, name: &str, default: bool) -> Result<bool> {
    match dialog_prop(props, name) {
        Some(value) => runtime.value_is_truthy(&value),
        None => Ok(default),
    }
}

fn listener_id(value: &Value) -> Option<f64> {
    match dict_field(value, "id") {
        Value::Number(id) => Some(id),
        _ => None,
    }
}

fn event_cancelled(event: &Value) -> bool {
    field(event, "cancelled").is_truthy() || field(event, "default_prevented").is_truthy()
}

fn named_value(named: &[(String, Value)], name: &str) -> Option<Value> {
    named
        .iter()
        .find(|(key, _)| key == name)
        .map(|(_, value)| value.clone())
}

fn array_field_map(fields: &HashMap<String, Value>, name: &str) -> Vec<Value> {
    match fields.get(name) {
        Some(Value::Array(items)) => items.clone(),
        _ => Vec::new(),
    }
}

fn string_prop(
    runtime: &Runtime,
    named: &[(String, Value)],
    name: &str,
    default: &str,
) -> Result<Value> {
    Ok(Value::String(match named_value(named, name) {
        Some(value) if !matches!(value, Value::Null) => runtime.render_value(&value)?,
        _ => default.to_owned(),
    }))
}

fn optional_string_prop(runtime: &Runtime, named: &[(String, Value)], name: &str) -> Result<Value> {
    match named_value(named, name) {
        Some(value) if !matches!(value, Value::Null) => {
            Ok(Value::String(runtime.render_value(&value)?))
        }
        _ => Ok(Value::Null),
    }
}

fn bool_prop(
    runtime: &Runtime,
    named: &[(String, Value)],
    name: &str,
    default: bool,
) -> Result<bool> {
    match named_value(named, name) {
        Some(value) => runtime.value_is_truthy(&value),
        None => Ok(default),
    }
}

fn number_prop(
    runtime: &Runtime,
    named: &[(String, Value)],
    name: &str,
    default: f64,
) -> Result<Value> {
    match named_value(named, name) {
        Some(value) => Ok(Value::Number(runtime.value_to_number(&value)?)),
        None => Ok(Value::Number(default)),
    }
}

fn optional_number_prop(runtime: &Runtime, named: &[(String, Value)], name: &str) -> Result<Value> {
    match named_value(named, name) {
        Some(Value::Null) | None => Ok(Value::Null),
        Some(value) => Ok(Value::Number(runtime.value_to_number(&value)?)),
    }
}

fn array_prop(runtime: &Runtime, named: &[(String, Value)], name: &str) -> Result<Value> {
    match named_value(named, name) {
        Some(value) => match runtime.deref_value(&value)? {
            Value::Array(items) => Ok(Value::Array(items)),
            _ => Ok(Value::Array(Vec::new())),
        },
        _ => Ok(Value::Array(Vec::new())),
    }
}

fn dict_prop(runtime: &Runtime, named: &[(String, Value)], name: &str) -> Result<Value> {
    match named_value(named, name) {
        Some(value) => match runtime.deref_value(&value)? {
            Value::Dict(map) => Ok(Value::Dict(map)),
            _ => Ok(Value::Dict(HashMap::new())),
        },
        _ => Ok(Value::Dict(HashMap::new())),
    }
}

fn getter(value: &Value, name: &str, args: &[Value], field_name: &str) -> Result<Value> {
    require_arity(name, args, 0)?;
    Ok(field(value, field_name))
}

fn bool_getter(value: &Value, name: &str, args: &[Value], field_name: &str) -> Result<Value> {
    require_arity(name, args, 0)?;
    Ok(Value::Boolean(field(value, field_name).is_truthy()))
}

fn clone_array_getter(
    value: &Value,
    name: &str,
    args: &[Value],
    field_name: &str,
) -> Result<Value> {
    require_arity(name, args, 0)?;
    Ok(Value::Array(match field(value, field_name) {
        Value::Array(items) => items,
        _ => Vec::new(),
    }))
}

fn set_string(
    runtime: &Runtime,
    value: &Value,
    name: &str,
    args: &[Value],
    field_name: &str,
) -> Result<Value> {
    require_arity(name, args, 1)?;
    let text = if matches!(args[0], Value::Null) {
        String::new()
    } else {
        runtime.render_value(&args[0])?
    };
    set_field(value, field_name, Value::String(text));
    Ok(value.clone())
}

fn set_bool(
    runtime: &Runtime,
    value: &Value,
    name: &str,
    args: &[Value],
    field_name: &str,
) -> Result<Value> {
    require_arity(name, args, 1)?;
    set_field(
        value,
        field_name,
        Value::Boolean(runtime.value_is_truthy(&args[0])?),
    );
    Ok(value.clone())
}

fn text_accessor(
    runtime: &Runtime,
    value: &Value,
    name: &str,
    args: &[Value],
    field_name: &str,
) -> Result<Value> {
    if args.is_empty() {
        Ok(field(value, field_name))
    } else {
        set_string(runtime, value, name, args, field_name)
    }
}

fn bool_accessor(
    runtime: &Runtime,
    value: &Value,
    name: &str,
    args: &[Value],
    field_name: &str,
) -> Result<Value> {
    if args.is_empty() {
        Ok(Value::Boolean(field(value, field_name).is_truthy()))
    } else {
        set_bool(runtime, value, name, args, field_name)
    }
}

fn checked_accessor(
    runtime: &Runtime,
    value: &Value,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Boolean(field(value, "checked").is_truthy()));
    }
    set_bool(runtime, value, name, args, "checked")?;
    if class_name == "Radio" && field(value, "checked").is_truthy() {
        if let Some(group) = parent_radio_group(value) {
            set_field(&group, "value", field(value, "value"));
            sync_radio_group_children(&group);
        }
    }
    Ok(value.clone())
}

fn value_accessor(
    runtime: &Runtime,
    value: &Value,
    class_name: &str,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    if args.is_empty() {
        return Ok(field(value, "value"));
    }
    require_arity(name, args, 1)?;
    let next = if matches!(args[0], Value::Null) {
        Value::Null
    } else if matches!(class_name, "Radio" | "RadioGroup" | "Select" | "DatePicker") {
        args[0].clone()
    } else {
        Value::String(runtime.render_value(&args[0])?)
    };
    set_field(value, "value", next);
    if class_name == "RadioGroup" {
        sync_radio_group_children(value);
    }
    Ok(value.clone())
}

fn tabs_selected_accessor(
    _runtime: &Runtime,
    value: &Value,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    if args.is_empty() {
        return Ok(field(value, "selected"));
    }
    require_arity(name, args, 1)?;
    set_field(value, "selected", args[0].clone());
    sync_tabs_children(value);
    Ok(value.clone())
}

fn tab_selected_accessor(
    runtime: &Runtime,
    value: &Value,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Boolean(field(value, "selected").is_truthy()));
    }
    set_bool(runtime, value, name, args, "selected")?;
    if field(value, "selected").is_truthy() {
        if let Some(tabs) = parent_tabs(value) {
            set_field(&tabs, "selected", field(value, "value"));
            sync_tabs_children(&tabs);
        }
    }
    Ok(value.clone())
}

fn selected_index_accessor(
    runtime: &Runtime,
    value: &Value,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    if args.is_empty() {
        return Ok(field(value, "selected_index"));
    }
    require_arity(name, args, 1)?;
    let next = if matches!(args[0], Value::Null) {
        Value::Null
    } else {
        Value::Number(runtime.value_to_number(&args[0])?)
    };
    set_field(value, "selected_index", next);
    Ok(value.clone())
}

fn list_selected_item(value: &Value) -> Value {
    let Value::Number(index) = field(value, "selected_index") else {
        return Value::Null;
    };
    match field(value, "items") {
        Value::Array(items) => items.get(index as usize).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn add_item(value: &Value, name: &str, args: &[Value]) -> Result<Value> {
    require_arity(name, args, 1)?;
    let mut items = match field(value, "items") {
        Value::Array(items) => items,
        _ => Vec::new(),
    };
    items.push(args[0].clone());
    set_field(value, "items", Value::Array(items));
    if matches!(class_name(value).as_deref(), Some("TreeView")) {
        let all_items = match field(value, "items") {
            Value::Array(items) => items,
            _ => Vec::new(),
        };
        set_field(
            value,
            "expanded_path_keys",
            Value::Dict(initial_expanded_tree_paths(&all_items)),
        );
    }
    Ok(value.clone())
}

fn clear_items(value: &Value, class_name: &str, name: &str, args: &[Value]) -> Result<Value> {
    require_arity(name, args, 0)?;
    set_field(value, "items", Value::Array(Vec::new()));
    if class_name == "ListView" {
        set_field(value, "selected_index", Value::Null);
    } else {
        set_field(value, "selected_path", Value::Array(Vec::new()));
        set_field(value, "expanded_path_keys", Value::Dict(HashMap::new()));
    }
    Ok(value.clone())
}

fn activate_index(runtime: &Runtime, value: &Value, name: &str, args: &[Value]) -> Result<Value> {
    require_arity(name, args, 1)?;
    set_field(
        value,
        "selected_index",
        Value::Number(runtime.value_to_number(&args[0])?),
    );
    let _ = dispatch_named_event(runtime, value, "activate", Value::Null)?;
    Ok(value.clone())
}

fn selected_path_accessor(
    _runtime: &Runtime,
    value: &Value,
    name: &str,
    args: &[Value],
) -> Result<Value> {
    if args.is_empty() {
        return clone_array_getter(value, name, args, "selected_path");
    }
    require_arity(name, args, 1)?;
    set_field(
        value,
        "selected_path",
        Value::Array(path_indexes(&args[0])?),
    );
    Ok(value.clone())
}

fn tree_selected_item(value: &Value) -> Value {
    let path = match field(value, "selected_path") {
        Value::Array(path) => path,
        _ => Vec::new(),
    };
    let items = match field(value, "items") {
        Value::Array(items) => items,
        _ => Vec::new(),
    };
    tree_item_at_path(&items, &path_indexes_from_values(&path)).unwrap_or(Value::Null)
}

fn tree_path_event(
    runtime: &Runtime,
    value: &Value,
    name: &str,
    args: &[Value],
    event: &str,
) -> Result<Value> {
    require_arity(name, args, 1)?;
    set_field(
        value,
        "selected_path",
        Value::Array(path_indexes(&args[0])?),
    );
    let _ = dispatch_named_event(runtime, value, event, Value::Null)?;
    Ok(value.clone())
}

fn tree_expand_collapse(
    runtime: &Runtime,
    value: &Value,
    name: &str,
    args: &[Value],
    expand: bool,
) -> Result<Value> {
    require_arity(name, args, 1)?;
    let path = path_indexes(&args[0])?;
    let key = path_key_values(&path);
    let mut expanded = match field(value, "expanded_path_keys") {
        Value::Dict(map) => map,
        _ => HashMap::new(),
    };
    if expand {
        expanded.insert(key, Value::Boolean(true));
    } else {
        expanded.remove(&key);
    }
    set_field(value, "expanded_path_keys", Value::Dict(expanded));
    let _ = dispatch_named_event(
        runtime,
        value,
        if expand { "expand" } else { "collapse" },
        Value::Null,
    )?;
    Ok(value.clone())
}

fn is_tree_expanded(value: &Value, name: &str, args: &[Value]) -> Result<Value> {
    require_arity(name, args, 1)?;
    let key = path_key_values(&path_indexes(&args[0])?);
    let expanded = match field(value, "expanded_path_keys") {
        Value::Dict(map) => map.contains_key(&key),
        _ => false,
    };
    Ok(Value::Boolean(expanded))
}

fn number_accessor(
    runtime: &Runtime,
    value: &Value,
    name: &str,
    args: &[Value],
    field_name: &str,
) -> Result<Value> {
    if args.is_empty() {
        Ok(field(value, field_name))
    } else {
        require_arity(name, args, 1)?;
        let next = if matches!(args[0], Value::Null) {
            Value::Null
        } else {
            Value::Number(runtime.value_to_number(&args[0])?)
        };
        set_field(value, field_name, next);
        Ok(value.clone())
    }
}

fn add_option(value: &Value, name: &str, args: &[Value]) -> Result<Value> {
    require_arity(name, args, 1)?;
    let mut options = match field(value, "options") {
        Value::Array(items) => items,
        _ => Vec::new(),
    };
    options.push(args[0].clone());
    set_field(value, "options", Value::Array(options));
    Ok(value.clone())
}

fn path_indexes(path: &Value) -> Result<Vec<Value>> {
    match path {
        Value::Array(items) => Ok(items
            .iter()
            .map(|item| Value::Number(index_number(item).unwrap_or(0.0)))
            .collect()),
        _ => Err(gui_error("GUI_PROP_TYPE", "selected_path expects an Array")),
    }
}

fn path_indexes_from_values(path: &[Value]) -> Vec<usize> {
    path.iter()
        .map(|item| index_number(item).unwrap_or(0.0).max(0.0) as usize)
        .collect()
}

fn index_number(value: &Value) -> Option<f64> {
    match value {
        Value::Number(value) => Some(*value),
        Value::String(value) => value.parse::<f64>().ok(),
        Value::Shared(shared) => index_number(&shared.borrow()),
        _ => None,
    }
}

fn path_key_values(path: &[Value]) -> String {
    path_indexes_from_values(path)
        .into_iter()
        .map(|index| index.to_string())
        .collect::<Vec<_>>()
        .join("/")
}

fn initial_expanded_tree_paths(items: &[Value]) -> HashMap<String, Value> {
    let mut expanded = HashMap::new();
    mark_tree_expanded(items, Vec::new(), &mut expanded);
    expanded
}

fn mark_tree_expanded(items: &[Value], prefix: Vec<usize>, expanded: &mut HashMap<String, Value>) {
    for (index, item) in items.iter().enumerate() {
        let mut path = prefix.clone();
        path.push(index);
        let children = tree_item_children(item);
        if children.is_empty() {
            continue;
        }
        expanded.insert(path_key_usize(&path), Value::Boolean(true));
        mark_tree_expanded(&children, path, expanded);
    }
}

fn path_key_usize(path: &[usize]) -> String {
    path.iter()
        .map(|index| index.to_string())
        .collect::<Vec<_>>()
        .join("/")
}

fn tree_item_children(item: &Value) -> Vec<Value> {
    match unshared_value(item) {
        Value::Dict(map) => match map.get("children") {
            Some(Value::Array(children)) => children.clone(),
            Some(Value::Shared(shared)) => match &*shared.borrow() {
                Value::Array(children) => children.clone(),
                _ => Vec::new(),
            },
            _ => Vec::new(),
        },
        Value::PairList(pairs) => pairs
            .iter()
            .find(|(key, _)| key == "children")
            .and_then(|(_, value)| match unshared_value(value) {
                Value::Array(children) => Some(children),
                _ => None,
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

fn tree_item_at_path(items: &[Value], path: &[usize]) -> Option<Value> {
    let mut current_items = items.to_vec();
    let mut current = None;
    for index in path {
        let item = current_items.get(*index)?.clone();
        current_items = tree_item_children(&item);
        current = Some(item);
    }
    current
}

fn unshared_value(value: &Value) -> Value {
    let mut current = value.clone();
    for _ in 0..32 {
        match current {
            Value::Shared(shared) => current = shared.borrow().clone(),
            other => return other,
        }
    }
    current
}

fn add_class(runtime: &Runtime, value: &Value, name: &str, args: &[Value]) -> Result<Value> {
    require_arity(name, args, 1)?;
    let class_name = runtime.render_value(&args[0])?;
    let mut classes = match field(value, "classes") {
        Value::Array(items) => items,
        _ => Vec::new(),
    };
    if !classes
        .iter()
        .any(|item| matches!(item, Value::String(existing) if existing == &class_name))
    {
        classes.push(Value::String(class_name));
    }
    set_field(value, "classes", Value::Array(classes));
    Ok(value.clone())
}

fn remove_class(runtime: &Runtime, value: &Value, name: &str, args: &[Value]) -> Result<Value> {
    require_arity(name, args, 1)?;
    let class_name = runtime.render_value(&args[0])?;
    let classes = match field(value, "classes") {
        Value::Array(items) => items
            .into_iter()
            .filter(|item| !matches!(item, Value::String(existing) if existing == &class_name))
            .collect(),
        _ => Vec::new(),
    };
    set_field(value, "classes", Value::Array(classes));
    Ok(value.clone())
}

fn map_accessor(
    runtime: &Runtime,
    value: &Value,
    name: &str,
    args: &[Value],
    field_name: &str,
) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(ZuzuRustError::runtime(format!(
            "{name}() expects one or two arguments"
        )));
    }
    let key = runtime.render_value(&args[0])?;
    let mut map = match field(value, field_name) {
        Value::Dict(map) => map,
        _ => HashMap::new(),
    };
    if args.len() == 2 {
        map.insert(key, args[1].clone());
        set_field(value, field_name, Value::Dict(map));
        Ok(value.clone())
    } else {
        Ok(map.get(&key).cloned().unwrap_or(Value::Null))
    }
}

fn require_arity(name: &str, args: &[Value], expected: usize) -> Result<()> {
    if args.len() != expected {
        return Err(ZuzuRustError::runtime(format!(
            "{name}() expects {expected} argument{}",
            if expected == 1 { "" } else { "s" }
        )));
    }
    Ok(())
}

fn unsupported_method(name: &str, class_name: &str) -> ZuzuRustError {
    ZuzuRustError::thrown(format!("unsupported method '{name}' for {class_name}"))
}

fn gui_error(code: &str, message: &str) -> ZuzuRustError {
    ZuzuRustError::thrown(format!("{code}: {message}"))
}

mod gtk_backend {
    use super::*;
    use libloading::Library;

    const GTK_ORIENTATION_HORIZONTAL: c_int = 0;
    const GTK_ORIENTATION_VERTICAL: c_int = 1;
    const GTK_FILE_CHOOSER_ACTION_OPEN: c_int = 0;
    const GTK_FILE_CHOOSER_ACTION_SAVE: c_int = 1;
    const GTK_FILE_CHOOSER_ACTION_SELECT_FOLDER: c_int = 2;
    const GTK_RESPONSE_ACCEPT: c_int = -3;
    const GTK_RESPONSE_OK: c_int = -5;

    #[repr(C)]
    #[derive(Clone, Copy)]
    struct GdkRgba {
        red: f32,
        green: f32,
        blue: f32,
        alpha: f32,
    }

    type GtkInitCheck = unsafe extern "C" fn() -> c_int;
    type GtkWindowNew = unsafe extern "C" fn() -> *mut c_void;
    type GtkWindowSetTitle = unsafe extern "C" fn(*mut c_void, *const c_char);
    type GtkWindowSetDefaultSize = unsafe extern "C" fn(*mut c_void, c_int, c_int);
    type GtkWindowSetChild = unsafe extern "C" fn(*mut c_void, *mut c_void);
    type GtkWindowPresent = unsafe extern "C" fn(*mut c_void);
    type GtkWindowDestroy = unsafe extern "C" fn(*mut c_void);
    type GtkBoxNew = unsafe extern "C" fn(c_int, c_int) -> *mut c_void;
    type GtkBoxAppend = unsafe extern "C" fn(*mut c_void, *mut c_void);
    type GtkFrameNew = unsafe extern "C" fn(*const c_char) -> *mut c_void;
    type GtkFrameSetChild = unsafe extern "C" fn(*mut c_void, *mut c_void);
    type GtkLabelNew = unsafe extern "C" fn(*const c_char) -> *mut c_void;
    type GtkLabelSetXalign = unsafe extern "C" fn(*mut c_void, f32);
    type GtkLabelSetWrap = unsafe extern "C" fn(*mut c_void, c_int);
    type GtkLabelSetText = unsafe extern "C" fn(*mut c_void, *const c_char);
    type GtkLabelSetMarkup = unsafe extern "C" fn(*mut c_void, *const c_char);
    type GtkButtonNewWithLabel = unsafe extern "C" fn(*const c_char) -> *mut c_void;
    type GtkButtonSetLabel = unsafe extern "C" fn(*mut c_void, *const c_char);
    type GtkEntryNew = unsafe extern "C" fn() -> *mut c_void;
    type GtkEditableSetText = unsafe extern "C" fn(*mut c_void, *const c_char);
    type GtkEditableGetText = unsafe extern "C" fn(*mut c_void) -> *const c_char;
    type GtkCheckButtonNewWithLabel = unsafe extern "C" fn(*const c_char) -> *mut c_void;
    type GtkCheckButtonSetGroup = unsafe extern "C" fn(*mut c_void, *mut c_void);
    type GtkCheckButtonSetActive = unsafe extern "C" fn(*mut c_void, c_int);
    type GtkCheckButtonGetActive = unsafe extern "C" fn(*mut c_void) -> c_int;
    type GtkDropDownNewFromStrings = unsafe extern "C" fn(*const *const c_char) -> *mut c_void;
    type GtkDropDownGetSelected = unsafe extern "C" fn(*mut c_void) -> u32;
    type GtkDropDownSetSelected = unsafe extern "C" fn(*mut c_void, u32);
    type GtkPictureNewForFilename = unsafe extern "C" fn(*const c_char) -> *mut c_void;
    type GtkCalendarNew = unsafe extern "C" fn() -> *mut c_void;
    type GtkCalendarGetDate = unsafe extern "C" fn(*mut c_void) -> *mut c_void;
    type GtkCalendarSelectDay = unsafe extern "C" fn(*mut c_void, *mut c_void);
    type GtkMenuButtonNew = unsafe extern "C" fn() -> *mut c_void;
    type GtkMenuButtonSetLabel = unsafe extern "C" fn(*mut c_void, *const c_char);
    type GtkMenuButtonSetPopover = unsafe extern "C" fn(*mut c_void, *mut c_void);
    type GtkMenuButtonPopdown = unsafe extern "C" fn(*mut c_void);
    type GtkPopoverNew = unsafe extern "C" fn() -> *mut c_void;
    type GtkPopoverSetChild = unsafe extern "C" fn(*mut c_void, *mut c_void);
    type GtkSeparatorNew = unsafe extern "C" fn(c_int) -> *mut c_void;
    type GtkScaleNewWithRange = unsafe extern "C" fn(c_int, f64, f64, f64) -> *mut c_void;
    type GtkRangeSetValue = unsafe extern "C" fn(*mut c_void, f64);
    type GtkRangeGetValue = unsafe extern "C" fn(*mut c_void) -> f64;
    type GtkProgressBarNew = unsafe extern "C" fn() -> *mut c_void;
    type GtkProgressBarSetFraction = unsafe extern "C" fn(*mut c_void, f64);
    type GtkProgressBarSetShowText = unsafe extern "C" fn(*mut c_void, c_int);
    type GtkProgressBarPulse = unsafe extern "C" fn(*mut c_void);
    type GtkColorChooserDialogNew = unsafe extern "C" fn(*const c_char, *mut c_void) -> *mut c_void;
    type GtkColorChooserGetRgba = unsafe extern "C" fn(*mut c_void, *mut GdkRgba);
    type GtkColorChooserSetRgba = unsafe extern "C" fn(*mut c_void, *const GdkRgba);
    type GtkNotebookNew = unsafe extern "C" fn() -> *mut c_void;
    type GtkNotebookAppendPage =
        unsafe extern "C" fn(*mut c_void, *mut c_void, *mut c_void) -> c_int;
    type GtkListBoxNew = unsafe extern "C" fn() -> *mut c_void;
    type GtkListBoxAppend = unsafe extern "C" fn(*mut c_void, *mut c_void);
    type GtkListBoxRemove = unsafe extern "C" fn(*mut c_void, *mut c_void);
    type GtkListBoxGetSelectedRow = unsafe extern "C" fn(*mut c_void) -> *mut c_void;
    type GtkListBoxRowGetIndex = unsafe extern "C" fn(*mut c_void) -> c_int;
    type GtkStringListNew = unsafe extern "C" fn(*const *const c_char) -> *mut c_void;
    type GtkStringListAppend = unsafe extern "C" fn(*mut c_void, *const c_char);
    type GtkStringObjectGetString = unsafe extern "C" fn(*mut c_void) -> *const c_char;
    type GtkTreeListModelCreateModelFunc =
        unsafe extern "C" fn(*mut c_void, *mut c_void) -> *mut c_void;
    type GDestroyNotify = Option<unsafe extern "C" fn(*mut c_void)>;
    type GtkTreeListModelNew = unsafe extern "C" fn(
        *mut c_void,
        c_int,
        c_int,
        GtkTreeListModelCreateModelFunc,
        *mut c_void,
        GDestroyNotify,
    ) -> *mut c_void;
    type GtkTreeListModelGetRow = unsafe extern "C" fn(*mut c_void, u32) -> *mut c_void;
    type GtkTreeListRowGetItem = unsafe extern "C" fn(*mut c_void) -> *mut c_void;
    type GtkTreeListRowSetExpanded = unsafe extern "C" fn(*mut c_void, c_int);
    type GtkTreeListRowGetExpanded = unsafe extern "C" fn(*mut c_void) -> c_int;
    type GtkSignalListItemFactoryNew = unsafe extern "C" fn() -> *mut c_void;
    type GtkListItemSetChild = unsafe extern "C" fn(*mut c_void, *mut c_void);
    type GtkListItemGetChild = unsafe extern "C" fn(*mut c_void) -> *mut c_void;
    type GtkListItemGetItem = unsafe extern "C" fn(*mut c_void) -> *mut c_void;
    type GtkListViewNew = unsafe extern "C" fn(*mut c_void, *mut c_void) -> *mut c_void;
    type GtkSingleSelectionNew = unsafe extern "C" fn(*mut c_void) -> *mut c_void;
    type GtkSingleSelectionSetModel = unsafe extern "C" fn(*mut c_void, *mut c_void);
    type GtkSingleSelectionGetSelectedItem = unsafe extern "C" fn(*mut c_void) -> *mut c_void;
    type GtkTreeExpanderNew = unsafe extern "C" fn() -> *mut c_void;
    type GtkTreeExpanderGetChild = unsafe extern "C" fn(*mut c_void) -> *mut c_void;
    type GtkTreeExpanderSetChild = unsafe extern "C" fn(*mut c_void, *mut c_void);
    type GtkTreeExpanderSetListRow = unsafe extern "C" fn(*mut c_void, *mut c_void);
    type GtkTreeExpanderSetIndentForIcon = unsafe extern "C" fn(*mut c_void, c_int);
    type GtkFileChooserNativeNew = unsafe extern "C" fn(
        *const c_char,
        *mut c_void,
        c_int,
        *const c_char,
        *const c_char,
    ) -> *mut c_void;
    type GtkNativeDialogShow = unsafe extern "C" fn(*mut c_void);
    type GtkNativeDialogDestroy = unsafe extern "C" fn(*mut c_void);
    type GtkFileChooserSetSelectMultiple = unsafe extern "C" fn(*mut c_void, c_int);
    type GtkFileChooserSetCurrentFolder =
        unsafe extern "C" fn(*mut c_void, *mut c_void, *mut *mut c_void) -> c_int;
    type GtkFileChooserSetCurrentName = unsafe extern "C" fn(*mut c_void, *const c_char);
    type GtkFileChooserGetFile = unsafe extern "C" fn(*mut c_void) -> *mut c_void;
    type GtkFileChooserGetFiles = unsafe extern "C" fn(*mut c_void) -> *mut c_void;
    type GtkWidgetSetSensitive = unsafe extern "C" fn(*mut c_void, c_int);
    type GtkWidgetSetVisible = unsafe extern "C" fn(*mut c_void, c_int);
    type GtkWidgetSetSizeRequest = unsafe extern "C" fn(*mut c_void, c_int, c_int);
    type GtkWidgetGetFirstChild = unsafe extern "C" fn(*mut c_void) -> *mut c_void;
    type GtkWidgetSetMarginTop = unsafe extern "C" fn(*mut c_void, c_int);
    type GtkWidgetSetMarginBottom = unsafe extern "C" fn(*mut c_void, c_int);
    type GtkWidgetSetMarginStart = unsafe extern "C" fn(*mut c_void, c_int);
    type GtkWidgetSetMarginEnd = unsafe extern "C" fn(*mut c_void, c_int);
    type GtkWidgetAddController = unsafe extern "C" fn(*mut c_void, *mut c_void);
    type GtkWidgetGrabFocus = unsafe extern "C" fn(*mut c_void) -> c_int;
    type GtkGestureClickNew = unsafe extern "C" fn() -> *mut c_void;
    type GMainContextIteration = unsafe extern "C" fn(*mut c_void, c_int) -> c_int;
    type GDateTimeNewLocal =
        unsafe extern "C" fn(c_int, c_int, c_int, c_int, c_int, f64) -> *mut c_void;
    type GDateTimeGetYear = unsafe extern "C" fn(*mut c_void) -> c_int;
    type GDateTimeGetMonth = unsafe extern "C" fn(*mut c_void) -> c_int;
    type GDateTimeGetDayOfMonth = unsafe extern "C" fn(*mut c_void) -> c_int;
    type GDateTimeUnref = unsafe extern "C" fn(*mut c_void);
    type GFileNewForPath = unsafe extern "C" fn(*const c_char) -> *mut c_void;
    type GFileGetPath = unsafe extern "C" fn(*mut c_void) -> *mut c_char;
    type GListModelGetNItems = unsafe extern "C" fn(*mut c_void) -> u32;
    type GListModelGetItem = unsafe extern "C" fn(*mut c_void, u32) -> *mut c_void;
    type GObjectUnref = unsafe extern "C" fn(*mut c_void);
    type GErrorFree = unsafe extern "C" fn(*mut c_void);
    type GFree = unsafe extern "C" fn(*mut c_void);
    type GdkRgbaParse = unsafe extern "C" fn(*mut GdkRgba, *const c_char) -> c_int;
    type GSignalConnectData = unsafe extern "C" fn(
        *mut c_void,
        *const c_char,
        *mut c_void,
        *mut c_void,
        *mut c_void,
        c_int,
    ) -> u64;

    struct GtkApi {
        gtk_init_check: GtkInitCheck,
        gtk_window_new: GtkWindowNew,
        gtk_window_set_title: GtkWindowSetTitle,
        gtk_window_set_default_size: GtkWindowSetDefaultSize,
        gtk_window_set_child: GtkWindowSetChild,
        gtk_window_present: GtkWindowPresent,
        gtk_window_destroy: GtkWindowDestroy,
        gtk_box_new: GtkBoxNew,
        gtk_box_append: GtkBoxAppend,
        gtk_frame_new: GtkFrameNew,
        gtk_frame_set_child: GtkFrameSetChild,
        gtk_label_new: GtkLabelNew,
        gtk_label_set_xalign: GtkLabelSetXalign,
        gtk_label_set_wrap: GtkLabelSetWrap,
        gtk_label_set_text: GtkLabelSetText,
        gtk_label_set_markup: GtkLabelSetMarkup,
        gtk_button_new_with_label: GtkButtonNewWithLabel,
        gtk_button_set_label: GtkButtonSetLabel,
        gtk_entry_new: GtkEntryNew,
        gtk_editable_set_text: GtkEditableSetText,
        gtk_editable_get_text: GtkEditableGetText,
        gtk_check_button_new_with_label: GtkCheckButtonNewWithLabel,
        gtk_check_button_set_group: GtkCheckButtonSetGroup,
        gtk_check_button_set_active: GtkCheckButtonSetActive,
        gtk_check_button_get_active: GtkCheckButtonGetActive,
        gtk_drop_down_new_from_strings: GtkDropDownNewFromStrings,
        gtk_drop_down_get_selected: GtkDropDownGetSelected,
        gtk_drop_down_set_selected: GtkDropDownSetSelected,
        gtk_picture_new_for_filename: GtkPictureNewForFilename,
        gtk_calendar_new: GtkCalendarNew,
        gtk_calendar_get_date: GtkCalendarGetDate,
        gtk_calendar_select_day: GtkCalendarSelectDay,
        gtk_menu_button_new: GtkMenuButtonNew,
        gtk_menu_button_set_label: GtkMenuButtonSetLabel,
        gtk_menu_button_set_popover: GtkMenuButtonSetPopover,
        gtk_menu_button_popdown: GtkMenuButtonPopdown,
        gtk_popover_new: GtkPopoverNew,
        gtk_popover_set_child: GtkPopoverSetChild,
        gtk_separator_new: GtkSeparatorNew,
        gtk_scale_new_with_range: GtkScaleNewWithRange,
        gtk_range_set_value: GtkRangeSetValue,
        gtk_range_get_value: GtkRangeGetValue,
        gtk_progress_bar_new: GtkProgressBarNew,
        gtk_progress_bar_set_fraction: GtkProgressBarSetFraction,
        gtk_progress_bar_set_show_text: GtkProgressBarSetShowText,
        gtk_progress_bar_pulse: GtkProgressBarPulse,
        gtk_color_chooser_dialog_new: GtkColorChooserDialogNew,
        gtk_color_chooser_get_rgba: GtkColorChooserGetRgba,
        gtk_color_chooser_set_rgba: GtkColorChooserSetRgba,
        gtk_notebook_new: GtkNotebookNew,
        gtk_notebook_append_page: GtkNotebookAppendPage,
        gtk_list_box_new: GtkListBoxNew,
        gtk_list_box_append: GtkListBoxAppend,
        gtk_list_box_remove: GtkListBoxRemove,
        gtk_list_box_get_selected_row: GtkListBoxGetSelectedRow,
        gtk_list_box_row_get_index: GtkListBoxRowGetIndex,
        gtk_string_list_new: GtkStringListNew,
        gtk_string_list_append: GtkStringListAppend,
        gtk_string_object_get_string: GtkStringObjectGetString,
        gtk_tree_list_model_new: GtkTreeListModelNew,
        gtk_tree_list_model_get_row: GtkTreeListModelGetRow,
        gtk_tree_list_row_get_item: GtkTreeListRowGetItem,
        gtk_tree_list_row_set_expanded: GtkTreeListRowSetExpanded,
        gtk_tree_list_row_get_expanded: GtkTreeListRowGetExpanded,
        gtk_signal_list_item_factory_new: GtkSignalListItemFactoryNew,
        gtk_list_item_set_child: GtkListItemSetChild,
        gtk_list_item_get_child: GtkListItemGetChild,
        gtk_list_item_get_item: GtkListItemGetItem,
        gtk_list_view_new: GtkListViewNew,
        gtk_single_selection_new: GtkSingleSelectionNew,
        gtk_single_selection_set_model: GtkSingleSelectionSetModel,
        gtk_single_selection_get_selected_item: GtkSingleSelectionGetSelectedItem,
        gtk_tree_expander_new: Option<GtkTreeExpanderNew>,
        gtk_tree_expander_get_child: GtkTreeExpanderGetChild,
        gtk_tree_expander_set_child: Option<GtkTreeExpanderSetChild>,
        gtk_tree_expander_set_list_row: GtkTreeExpanderSetListRow,
        gtk_tree_expander_set_indent_for_icon: Option<GtkTreeExpanderSetIndentForIcon>,
        gtk_file_chooser_native_new: GtkFileChooserNativeNew,
        gtk_native_dialog_show: GtkNativeDialogShow,
        gtk_native_dialog_destroy: GtkNativeDialogDestroy,
        gtk_file_chooser_set_select_multiple: GtkFileChooserSetSelectMultiple,
        gtk_file_chooser_set_current_folder: GtkFileChooserSetCurrentFolder,
        gtk_file_chooser_set_current_name: GtkFileChooserSetCurrentName,
        gtk_file_chooser_get_file: GtkFileChooserGetFile,
        gtk_file_chooser_get_files: GtkFileChooserGetFiles,
        gtk_widget_set_sensitive: GtkWidgetSetSensitive,
        gtk_widget_set_visible: GtkWidgetSetVisible,
        gtk_widget_set_size_request: GtkWidgetSetSizeRequest,
        gtk_widget_get_first_child: GtkWidgetGetFirstChild,
        gtk_widget_set_margin_top: GtkWidgetSetMarginTop,
        gtk_widget_set_margin_bottom: GtkWidgetSetMarginBottom,
        gtk_widget_set_margin_start: GtkWidgetSetMarginStart,
        gtk_widget_set_margin_end: GtkWidgetSetMarginEnd,
        gtk_widget_add_controller: GtkWidgetAddController,
        gtk_widget_grab_focus: GtkWidgetGrabFocus,
        gtk_gesture_click_new: GtkGestureClickNew,
        g_main_context_iteration: GMainContextIteration,
        g_date_time_new_local: GDateTimeNewLocal,
        g_date_time_get_year: GDateTimeGetYear,
        g_date_time_get_month: GDateTimeGetMonth,
        g_date_time_get_day_of_month: GDateTimeGetDayOfMonth,
        g_date_time_unref: GDateTimeUnref,
        g_file_new_for_path: GFileNewForPath,
        g_file_get_path: GFileGetPath,
        g_list_model_get_n_items: GListModelGetNItems,
        g_list_model_get_item: GListModelGetItem,
        g_object_unref: GObjectUnref,
        g_error_free: GErrorFree,
        g_free: GFree,
        gdk_rgba_parse: GdkRgbaParse,
        g_signal_connect_data: GSignalConnectData,
    }

    thread_local! {
        static GTK_API: RefCell<Option<&'static GtkApi>> = const { RefCell::new(None) };
        static WINDOWS: RefCell<HashMap<usize, *mut c_void>> = RefCell::new(HashMap::new());
        static WIDGETS: RefCell<HashMap<usize, *mut c_void>> = RefCell::new(HashMap::new());
        static TREE_LIST_MODELS: RefCell<HashMap<usize, *mut c_void>> = RefCell::new(HashMap::new());
        static TREE_SELECTIONS: RefCell<HashMap<usize, *mut c_void>> = RefCell::new(HashMap::new());
        static TREE_ROW_EXPANDED_HANDLERS: RefCell<HashMap<usize, bool>> = RefCell::new(HashMap::new());
        static DATE_PICKER_ENTRIES: RefCell<HashMap<usize, *mut c_void>> = RefCell::new(HashMap::new());
        static DATE_PICKER_CALENDARS: RefCell<HashMap<usize, *mut c_void>> = RefCell::new(HashMap::new());
        static DATE_PICKER_BUTTONS: RefCell<HashMap<usize, *mut c_void>> = RefCell::new(HashMap::new());
        static RUN_STATES: RefCell<HashMap<usize, *mut bool>> = RefCell::new(HashMap::new());
        static SYNCING: RefCell<bool> = const { RefCell::new(false) };
    }

    struct TreeListModelData {
        widget: Value,
    }

    struct TreeFactoryData {
        api: *const GtkApi,
        runtime: *const Runtime,
        widget: Value,
    }

    pub(super) fn show_window(runtime: &Runtime, window: &Value) -> Result<()> {
        let api = api()?;
        unsafe {
            if (api.gtk_init_check)() == 0 {
                return Err(gui_error(
                    "GUI_BACKEND",
                    "GTK4 could not initialize; check DISPLAY or WAYLAND_DISPLAY",
                ));
            }
            let native = build_window(api, runtime, window)?;
            WINDOWS.with(|windows| {
                windows.borrow_mut().insert(object_key(window), native);
            });
            (api.gtk_window_present)(native);
        }
        Ok(())
    }

    pub(super) fn preview_widget(runtime: &Runtime, root: &Value) -> Result<*mut c_void> {
        let api = api()?;
        unsafe {
            if (api.gtk_init_check)() == 0 {
                return Err(gui_error(
                    "GUI_BACKEND",
                    "GTK4 could not initialize; check DISPLAY or WAYLAND_DISPLAY",
                ));
            }
            if matches!(class_name(root).as_deref(), Some("Window")) {
                build_window_preview(api, runtime, root)
            } else {
                build_widget(api, runtime, root)
            }
        }
    }

    pub(super) fn native_file_dialog(
        runtime: &Runtime,
        name: &str,
        props: &Value,
    ) -> Result<Value> {
        let api = api()?;
        unsafe {
            if (api.gtk_init_check)() == 0 {
                return Err(gui_error(
                    "GUI_BACKEND",
                    "GTK4 could not initialize; check DISPLAY or WAYLAND_DISPLAY",
                ));
            }
            run_native_file_dialog(api, runtime, name, props)
        }
    }

    pub(super) fn native_colour_dialog(runtime: &Runtime, props: &Value) -> Result<Value> {
        let api = api()?;
        unsafe {
            if (api.gtk_init_check)() == 0 {
                return Err(gui_error(
                    "GUI_BACKEND",
                    "GTK4 could not initialize; check DISPLAY or WAYLAND_DISPLAY",
                ));
            }
            run_native_colour_dialog(api, runtime, props)
        }
    }

    struct DialogResponse {
        done: bool,
        response: c_int,
    }

    struct DialogParent {
        native: *mut c_void,
        temporary: bool,
    }

    unsafe fn run_native_file_dialog(
        api: &GtkApi,
        runtime: &Runtime,
        name: &str,
        props: &Value,
    ) -> Result<Value> {
        let (title_default, action, accept_label) = match name {
            "native_file_open" => ("Open File", GTK_FILE_CHOOSER_ACTION_OPEN, "Open"),
            "native_file_save" => ("Save File", GTK_FILE_CHOOSER_ACTION_SAVE, "Save"),
            "native_directory_open" => (
                "Open Directory",
                GTK_FILE_CHOOSER_ACTION_SELECT_FOLDER,
                "Open",
            ),
            "native_directory_save" => (
                "Save Directory",
                GTK_FILE_CHOOSER_ACTION_SELECT_FOLDER,
                "Save",
            ),
            _ => return Ok(Value::Null),
        };
        let title = dialog_string_prop(runtime, props, "title", title_default)?;
        let parent = transient_dialog_parent(api);
        let dialog = (api.gtk_file_chooser_native_new)(
            c_string(&title).as_ptr(),
            parent.native,
            action,
            c_string(accept_label).as_ptr(),
            c_string("Cancel").as_ptr(),
        );
        if dialog.is_null() {
            destroy_dialog_parent(api, parent);
            return Ok(Value::Null);
        }

        let multiple =
            name == "native_file_open" && dialog_bool_prop(runtime, props, "multiple", false)?;
        if multiple {
            (api.gtk_file_chooser_set_select_multiple)(dialog, 1);
        }
        configure_file_dialog_initial_path(api, runtime, dialog, props, name)?;

        let response = Box::into_raw(Box::new(DialogResponse {
            done: false,
            response: 0,
        }));
        let signal = c_string("response");
        let callback: unsafe extern "C" fn(*mut c_void, c_int, *mut c_void) = dialog_response;
        (api.g_signal_connect_data)(
            dialog,
            signal.as_ptr(),
            callback as *mut c_void,
            response.cast(),
            std::ptr::null_mut(),
            0,
        );
        (api.gtk_native_dialog_show)(dialog);
        while !(*response).done {
            (api.g_main_context_iteration)(std::ptr::null_mut(), 1);
        }
        let accepted = (*response).response == GTK_RESPONSE_ACCEPT;
        drop(Box::from_raw(response));

        let result = if accepted {
            if multiple {
                native_file_dialog_paths(api, dialog)
            } else {
                native_file_dialog_path(api, dialog)
                    .map(Value::String)
                    .unwrap_or(Value::Null)
            }
        } else {
            Value::Null
        };
        (api.gtk_native_dialog_destroy)(dialog);
        destroy_dialog_parent(api, parent);
        Ok(result)
    }

    unsafe fn transient_dialog_parent(api: &GtkApi) -> DialogParent {
        let existing = WINDOWS.with(|windows| windows.borrow().values().copied().next());
        if let Some(native) = existing {
            return DialogParent {
                native,
                temporary: false,
            };
        }

        let native = (api.gtk_window_new)();
        if !native.is_null() {
            (api.gtk_window_set_title)(native, c_string("Zuzu").as_ptr());
            (api.gtk_window_set_default_size)(native, 1, 1);
        }
        DialogParent {
            native,
            temporary: true,
        }
    }

    unsafe fn destroy_dialog_parent(api: &GtkApi, parent: DialogParent) {
        if parent.temporary && !parent.native.is_null() {
            (api.gtk_window_destroy)(parent.native);
        }
    }

    unsafe extern "C" fn dialog_response(
        _dialog: *mut c_void,
        response_id: c_int,
        data: *mut c_void,
    ) {
        if !data.is_null() {
            let response = data.cast::<DialogResponse>();
            (*response).response = response_id;
            (*response).done = true;
        }
    }

    unsafe fn configure_file_dialog_initial_path(
        api: &GtkApi,
        runtime: &Runtime,
        dialog: *mut c_void,
        props: &Value,
        name: &str,
    ) -> Result<()> {
        let directory = dialog_string_prop(runtime, props, "directory", "")?;
        if !directory.is_empty() {
            set_dialog_folder(api, dialog, &directory);
        }

        let value = dialog_string_prop(runtime, props, "value", "")?;
        if value.is_empty() {
            return Ok(());
        }

        let path = Path::new(&value);
        if name == "native_file_save" {
            if let Some(parent) = path.parent().and_then(Path::to_str) {
                if !parent.is_empty() {
                    set_dialog_folder(api, dialog, parent);
                }
            }
            if let Some(file_name) = path.file_name().and_then(|part| part.to_str()) {
                (api.gtk_file_chooser_set_current_name)(dialog, c_string(file_name).as_ptr());
            }
        } else if path.is_dir() || name == "native_directory_open" {
            set_dialog_folder(api, dialog, &value);
        } else if let Some(parent) = path.parent().and_then(Path::to_str) {
            if !parent.is_empty() {
                set_dialog_folder(api, dialog, parent);
            }
        }

        Ok(())
    }

    unsafe fn set_dialog_folder(api: &GtkApi, dialog: *mut c_void, path: &str) {
        let file = (api.g_file_new_for_path)(c_string(path).as_ptr());
        if file.is_null() {
            return;
        }
        let mut error: *mut c_void = std::ptr::null_mut();
        let _ = (api.gtk_file_chooser_set_current_folder)(dialog, file, &mut error);
        if !error.is_null() {
            (api.g_error_free)(error);
        }
        (api.g_object_unref)(file);
    }

    unsafe fn native_file_dialog_path(api: &GtkApi, dialog: *mut c_void) -> Option<String> {
        let file = (api.gtk_file_chooser_get_file)(dialog);
        if file.is_null() {
            return None;
        }
        let path = g_file_path(api, file);
        (api.g_object_unref)(file);
        path
    }

    unsafe fn native_file_dialog_paths(api: &GtkApi, dialog: *mut c_void) -> Value {
        let files = (api.gtk_file_chooser_get_files)(dialog);
        if files.is_null() {
            return Value::Null;
        }
        let len = (api.g_list_model_get_n_items)(files);
        let mut out = Vec::new();
        for index in 0..len {
            let file = (api.g_list_model_get_item)(files, index);
            if file.is_null() {
                continue;
            }
            if let Some(path) = g_file_path(api, file) {
                out.push(Value::String(path));
            }
            (api.g_object_unref)(file);
        }
        (api.g_object_unref)(files);
        if out.is_empty() {
            Value::Null
        } else {
            Value::Array(out)
        }
    }

    unsafe fn g_file_path(api: &GtkApi, file: *mut c_void) -> Option<String> {
        let path = (api.g_file_get_path)(file);
        if path.is_null() {
            return None;
        }
        let text = c_text(path.cast());
        (api.g_free)(path.cast());
        Some(text)
    }

    unsafe fn run_native_colour_dialog(
        api: &GtkApi,
        runtime: &Runtime,
        props: &Value,
    ) -> Result<Value> {
        let title = dialog_string_prop(runtime, props, "title", "Choose Colour")?;
        let default_colour = dialog_colour_prop(api, runtime, props)?;
        let parent = transient_dialog_parent(api);
        let dialog = (api.gtk_color_chooser_dialog_new)(c_string(&title).as_ptr(), parent.native);
        if dialog.is_null() {
            destroy_dialog_parent(api, parent);
            return Ok(Value::Null);
        }

        (api.gtk_color_chooser_set_rgba)(dialog, &default_colour);
        let response = Box::into_raw(Box::new(DialogResponse {
            done: false,
            response: 0,
        }));
        let signal = c_string("response");
        let callback: unsafe extern "C" fn(*mut c_void, c_int, *mut c_void) = dialog_response;
        (api.g_signal_connect_data)(
            dialog,
            signal.as_ptr(),
            callback as *mut c_void,
            response.cast(),
            std::ptr::null_mut(),
            0,
        );
        (api.gtk_window_present)(dialog);
        while !(*response).done {
            (api.g_main_context_iteration)(std::ptr::null_mut(), 1);
        }
        let accepted =
            (*response).response == GTK_RESPONSE_OK || (*response).response == GTK_RESPONSE_ACCEPT;
        drop(Box::from_raw(response));

        let result = if accepted {
            let mut colour = default_colour;
            (api.gtk_color_chooser_get_rgba)(dialog, &mut colour);
            Value::String(format_gdk_rgba(colour))
        } else {
            Value::String(format_gdk_rgba(default_colour))
        };
        (api.gtk_window_destroy)(dialog);
        destroy_dialog_parent(api, parent);
        Ok(result)
    }

    unsafe fn dialog_colour_prop(
        api: &GtkApi,
        runtime: &Runtime,
        props: &Value,
    ) -> Result<GdkRgba> {
        let raw = dialog_string_prop(runtime, props, "value", "#000000")?;
        Ok(parse_gdk_rgba(api, &raw).unwrap_or(GdkRgba {
            red: 0.0,
            green: 0.0,
            blue: 0.0,
            alpha: 1.0,
        }))
    }

    unsafe fn parse_gdk_rgba(api: &GtkApi, text: &str) -> Option<GdkRgba> {
        let mut colour = GdkRgba {
            red: 0.0,
            green: 0.0,
            blue: 0.0,
            alpha: 1.0,
        };
        ((api.gdk_rgba_parse)(&mut colour, c_string(text).as_ptr()) != 0).then_some(colour)
    }

    fn format_gdk_rgba(colour: GdkRgba) -> String {
        fn channel(value: f32) -> u8 {
            (value.clamp(0.0, 1.0) * 255.0).round() as u8
        }
        format!(
            "#{:02x}{:02x}{:02x}",
            channel(colour.red),
            channel(colour.green),
            channel(colour.blue)
        )
    }

    pub(super) fn run_window(runtime: &Runtime, window: &Value) -> Result<()> {
        let api = api()?;
        let mut native = WINDOWS.with(|windows| windows.borrow().get(&object_key(window)).copied());
        if native.is_none() {
            show_window(runtime, window)?;
            native = WINDOWS.with(|windows| windows.borrow().get(&object_key(window)).copied());
        }
        let state = Box::into_raw(Box::new(false));
        let signal = c_string("close-request");
        unsafe {
            if let Some(native) = native {
                RUN_STATES.with(|states| {
                    states.borrow_mut().insert(object_key(window), state);
                });
                let callback: unsafe extern "C" fn(*mut c_void, *mut c_void) -> c_int =
                    close_request;
                (api.g_signal_connect_data)(
                    native,
                    signal.as_ptr(),
                    callback as *mut c_void,
                    state.cast(),
                    std::ptr::null_mut(),
                    0,
                );
                while !*state {
                    (api.g_main_context_iteration)(std::ptr::null_mut(), 1);
                }
            }
            RUN_STATES.with(|states| {
                states.borrow_mut().remove(&object_key(window));
            });
            drop(Box::from_raw(state));
        }
        Ok(())
    }

    pub(super) fn destroy_window(window: &Value) {
        if let Ok(api) = api() {
            RUN_STATES.with(|states| {
                if let Some(state) = states.borrow().get(&object_key(window)).copied() {
                    unsafe {
                        *state = true;
                    }
                }
            });
            let native = WINDOWS.with(|windows| windows.borrow_mut().remove(&object_key(window)));
            if let Some(native) = native {
                unsafe {
                    (api.gtk_window_destroy)(native);
                }
            }
        }
    }

    pub(super) fn sync_property(widget: &Value, property: &str) {
        if SYNCING.with(|syncing| *syncing.borrow()) {
            return;
        }
        let Some(native) =
            WIDGETS.with(|widgets| widgets.borrow().get(&object_key(widget)).copied())
        else {
            return;
        };
        let Ok(api) = api() else {
            return;
        };
        let class = class_name(widget).unwrap_or_default();
        unsafe {
            match (class.as_str(), property) {
                ("Label", "text") => (api.gtk_label_set_text)(
                    native,
                    c_string(&string_field(widget, "text")).as_ptr(),
                ),
                ("Text", "value") => (api.gtk_label_set_text)(
                    native,
                    c_string(&string_field(widget, "value")).as_ptr(),
                ),
                ("RichText", "value") => (api.gtk_label_set_markup)(
                    native,
                    c_string(&string_field(widget, "value")).as_ptr(),
                ),
                ("Button", "text") => (api.gtk_button_set_label)(
                    native,
                    c_string(&string_field(widget, "text")).as_ptr(),
                ),
                ("Input", "value") => (api.gtk_editable_set_text)(
                    native,
                    c_string(&string_field(widget, "value")).as_ptr(),
                ),
                ("DatePicker", "value") => {
                    DATE_PICKER_ENTRIES.with(|entries| {
                        if let Some(entry) = entries.borrow().get(&object_key(widget)).copied() {
                            (api.gtk_editable_set_text)(
                                entry,
                                c_string(&string_field(widget, "value")).as_ptr(),
                            );
                        }
                    });
                    DATE_PICKER_CALENDARS.with(|calendars| {
                        if let Some(calendar) = calendars.borrow().get(&object_key(widget)).copied()
                        {
                            select_calendar_date(api, calendar, &string_field(widget, "value"));
                        }
                    });
                }
                ("Checkbox" | "Radio", "checked") => (api.gtk_check_button_set_active)(
                    native,
                    bool_field(widget, "checked", false) as c_int,
                ),
                ("Slider", "value") => {
                    (api.gtk_range_set_value)(native, number_field(widget, "value", 0.0));
                }
                ("Progress", "value" | "min" | "max") => apply_progress(api, widget, native),
                ("Progress", "show_text") => (api.gtk_progress_bar_set_show_text)(
                    native,
                    bool_field(widget, "show_text", false) as c_int,
                ),
                ("Progress", "indeterminate") => {
                    if bool_field(widget, "indeterminate", false) {
                        (api.gtk_progress_bar_pulse)(native);
                    }
                }
                ("Select", "value") => {
                    if let Some(index) = selected_option_index(widget) {
                        (api.gtk_drop_down_set_selected)(native, index as u32);
                    }
                }
                ("ListView", "items") => rebuild_list_view(api, native, widget, false),
                ("TreeView", "items") => rebuild_tree_view_model(api, widget),
                ("TreeView", "expanded_path_keys") => apply_tree_view_expansion(api, widget),
                (_, "width" | "height" | "minwidth" | "minheight" | "maxwidth" | "maxheight") => {
                    apply_size_request(api, widget, native)
                }
                ("VBox" | "HBox", "padding") => apply_padding(api, widget, native),
                _ => {}
            }
        }
    }

    unsafe extern "C" fn close_request(_window: *mut c_void, data: *mut c_void) -> c_int {
        if !data.is_null() {
            *data.cast::<bool>() = true;
        }
        0
    }

    struct SignalHandler {
        runtime: *const Runtime,
        widget: Value,
        event: String,
        kind: SignalKind,
    }

    #[derive(Clone, Copy)]
    enum SignalKind {
        Plain,
        EntryChanged,
        CheckToggled,
        RadioToggled,
        SelectChanged,
        SliderChanged,
        CalendarSelected,
        CalendarAccepted,
        ListSelected,
        TreeSelected,
        TreeListSelected,
    }

    fn connect_widget_event(
        api: &GtkApi,
        native: *mut c_void,
        signal: &str,
        runtime: &Runtime,
        widget: &Value,
        event: &str,
        kind: SignalKind,
    ) {
        let signal = c_string(signal);
        let handler = Box::into_raw(Box::new(SignalHandler {
            runtime,
            widget: widget.clone(),
            event: event.to_owned(),
            kind,
        }));
        unsafe {
            let callback: unsafe extern "C" fn(*mut c_void, *mut c_void) = widget_signal;
            (api.g_signal_connect_data)(
                native,
                signal.as_ptr(),
                callback as *mut c_void,
                handler.cast(),
                std::ptr::null_mut(),
                0,
            );
        }
    }

    fn connect_widget_notify(
        api: &GtkApi,
        native: *mut c_void,
        signal: &str,
        runtime: &Runtime,
        widget: &Value,
        event: &str,
        kind: SignalKind,
    ) {
        let signal = c_string(signal);
        let handler = Box::into_raw(Box::new(SignalHandler {
            runtime,
            widget: widget.clone(),
            event: event.to_owned(),
            kind,
        }));
        unsafe {
            let callback: unsafe extern "C" fn(*mut c_void, *mut c_void, *mut c_void) =
                widget_signal_notify;
            (api.g_signal_connect_data)(
                native,
                signal.as_ptr(),
                callback as *mut c_void,
                handler.cast(),
                std::ptr::null_mut(),
                0,
            );
        }
    }

    fn connect_listbox_event(
        api: &GtkApi,
        native: *mut c_void,
        signal: &str,
        runtime: &Runtime,
        widget: &Value,
        event: &str,
        kind: SignalKind,
    ) {
        let signal = c_string(signal);
        let handler = Box::into_raw(Box::new(SignalHandler {
            runtime,
            widget: widget.clone(),
            event: event.to_owned(),
            kind,
        }));
        unsafe {
            let callback: unsafe extern "C" fn(*mut c_void, *mut c_void, *mut c_void) =
                listbox_signal;
            (api.g_signal_connect_data)(
                native,
                signal.as_ptr(),
                callback as *mut c_void,
                handler.cast(),
                std::ptr::null_mut(),
                0,
            );
        }
    }

    unsafe extern "C" fn widget_signal(_widget: *mut c_void, data: *mut c_void) {
        if data.is_null() {
            return;
        }
        let handler = &*data.cast::<SignalHandler>();
        let runtime = &*handler.runtime;
        update_from_native(runtime, handler, _widget);
        let _ = dispatch_named_event(runtime, &handler.widget, &handler.event, Value::Null);
    }

    unsafe extern "C" fn widget_signal_notify(
        widget: *mut c_void,
        _pspec: *mut c_void,
        data: *mut c_void,
    ) {
        widget_signal(widget, data);
    }

    unsafe extern "C" fn listbox_signal(widget: *mut c_void, _row: *mut c_void, data: *mut c_void) {
        widget_signal(widget, data);
    }

    unsafe extern "C" fn listview_activate_signal(
        _widget: *mut c_void,
        position: u32,
        data: *mut c_void,
    ) {
        if data.is_null() {
            return;
        }
        let handler = &*data.cast::<SignalHandler>();
        let runtime = &*handler.runtime;
        let Ok(api) = api() else {
            return;
        };
        if let Some(path) = tree_list_position_path(api, &handler.widget, position) {
            SYNCING.with(|syncing| {
                *syncing.borrow_mut() = true;
            });
            set_field(&handler.widget, "selected_path", Value::Array(path));
            SYNCING.with(|syncing| {
                *syncing.borrow_mut() = false;
            });
        }
        let _ = dispatch_named_event(runtime, &handler.widget, &handler.event, Value::Null);
    }

    unsafe extern "C" fn gesture_pressed(
        gesture: *mut c_void,
        n_press: c_int,
        _x: f64,
        _y: f64,
        data: *mut c_void,
    ) {
        if !data.is_null() {
            let handler = &*data.cast::<SignalHandler>();
            if matches!(handler.kind, SignalKind::CalendarAccepted) && n_press < 2 {
                return;
            }
        }
        widget_signal(gesture, data);
    }

    fn update_from_native(runtime: &Runtime, handler: &SignalHandler, native: *mut c_void) {
        let Ok(api) = api() else {
            return;
        };
        SYNCING.with(|syncing| {
            *syncing.borrow_mut() = true;
        });
        unsafe {
            match handler.kind {
                SignalKind::Plain => {}
                SignalKind::EntryChanged => {
                    let text = (api.gtk_editable_get_text)(native);
                    let text = c_text(text);
                    set_field(&handler.widget, "value", Value::String(text.clone()));
                    if matches!(class_name(&handler.widget).as_deref(), Some("DatePicker")) {
                        DATE_PICKER_CALENDARS.with(|calendars| {
                            if let Some(calendar) = calendars
                                .borrow()
                                .get(&object_key(&handler.widget))
                                .copied()
                            {
                                select_calendar_date(api, calendar, &text);
                            }
                        });
                    }
                }
                SignalKind::CheckToggled => {
                    let checked = (api.gtk_check_button_get_active)(native) != 0;
                    set_field(&handler.widget, "checked", Value::Boolean(checked));
                }
                SignalKind::RadioToggled => {
                    let checked = (api.gtk_check_button_get_active)(native) != 0;
                    set_field(&handler.widget, "checked", Value::Boolean(checked));
                    if checked {
                        if let Some(group) = parent_radio_group(&handler.widget) {
                            set_field(&group, "value", field(&handler.widget, "value"));
                            sync_radio_group_children(&group);
                            SYNCING.with(|syncing| {
                                *syncing.borrow_mut() = false;
                            });
                            for radio in radio_children(&group) {
                                sync_property(&radio, "checked");
                            }
                            SYNCING.with(|syncing| {
                                *syncing.borrow_mut() = true;
                            });
                            let _ = dispatch_named_event(runtime, &group, "change", Value::Null);
                        }
                    }
                }
                SignalKind::SelectChanged => {
                    let index = (api.gtk_drop_down_get_selected)(native) as usize;
                    let selected = option_value_at(&handler.widget, index);
                    set_field(&handler.widget, "value", selected);
                }
                SignalKind::SliderChanged => {
                    let value = (api.gtk_range_get_value)(native);
                    set_field(&handler.widget, "value", Value::Number(value));
                }
                SignalKind::CalendarSelected => {
                    if let Some(text) = calendar_date_string(api, native) {
                        set_field(&handler.widget, "value", Value::String(text.clone()));
                        DATE_PICKER_ENTRIES.with(|entries| {
                            if let Some(entry) =
                                entries.borrow().get(&object_key(&handler.widget)).copied()
                            {
                                (api.gtk_editable_set_text)(entry, c_string(&text).as_ptr());
                            }
                        });
                    }
                }
                SignalKind::CalendarAccepted => {
                    DATE_PICKER_CALENDARS.with(|calendars| {
                        if let Some(calendar) = calendars
                            .borrow()
                            .get(&object_key(&handler.widget))
                            .copied()
                        {
                            if let Some(text) = calendar_date_string(api, calendar) {
                                set_field(&handler.widget, "value", Value::String(text.clone()));
                                DATE_PICKER_ENTRIES.with(|entries| {
                                    if let Some(entry) =
                                        entries.borrow().get(&object_key(&handler.widget)).copied()
                                    {
                                        (api.gtk_editable_set_text)(
                                            entry,
                                            c_string(&text).as_ptr(),
                                        );
                                        (api.gtk_widget_grab_focus)(entry);
                                    }
                                });
                            }
                        }
                    });
                    DATE_PICKER_BUTTONS.with(|buttons| {
                        if let Some(button) =
                            buttons.borrow().get(&object_key(&handler.widget)).copied()
                        {
                            (api.gtk_menu_button_popdown)(button);
                        }
                    });
                }
                SignalKind::ListSelected => {
                    let row = (api.gtk_list_box_get_selected_row)(native);
                    if !row.is_null() {
                        set_field(
                            &handler.widget,
                            "selected_index",
                            Value::Number((api.gtk_list_box_row_get_index)(row) as f64),
                        );
                    }
                }
                SignalKind::TreeSelected => {
                    let row = (api.gtk_list_box_get_selected_row)(native);
                    if !row.is_null() {
                        let index = (api.gtk_list_box_row_get_index)(row) as usize;
                        if let Some(path) = flattened_tree_paths(&handler.widget).get(index) {
                            set_field(&handler.widget, "selected_path", Value::Array(path.clone()));
                        }
                    }
                }
                SignalKind::TreeListSelected => {
                    let row = (api.gtk_single_selection_get_selected_item)(native);
                    if let Some(path) = tree_list_row_path(api, row) {
                        set_field(&handler.widget, "selected_path", Value::Array(path));
                    }
                }
            }
        }
        SYNCING.with(|syncing| {
            *syncing.borrow_mut() = false;
        });
    }

    fn build_window(api: &GtkApi, runtime: &Runtime, window: &Value) -> Result<*mut c_void> {
        unsafe {
            let native = (api.gtk_window_new)();
            if native.is_null() {
                return Err(gui_error("GUI_BACKEND", "GTK4 could not create a window"));
            }
            let title = c_string(&string_field(window, "title"));
            (api.gtk_window_set_title)(native, title.as_ptr());
            (api.gtk_window_set_default_size)(
                native,
                number_field(window, "width", 800.0) as c_int,
                number_field(window, "height", 600.0) as c_int,
            );
            if let Some(content) = first_non_menu_child(window) {
                let child = build_widget(api, runtime, &content)?;
                (api.gtk_window_set_child)(native, child);
            }
            Ok(native)
        }
    }

    unsafe fn build_window_preview(
        api: &GtkApi,
        runtime: &Runtime,
        window: &Value,
    ) -> Result<*mut c_void> {
        let native = (api.gtk_box_new)(GTK_ORIENTATION_VERTICAL, 10);
        let title = build_label(api, &string_field(window, "title"));
        (api.gtk_box_append)(native, title);
        if let Some(content) = first_non_menu_child(window) {
            let child = build_widget(api, runtime, &content)?;
            (api.gtk_box_append)(native, child);
        }
        Ok(native)
    }

    fn build_widget(api: &GtkApi, runtime: &Runtime, widget: &Value) -> Result<*mut c_void> {
        let class = class_name(widget).unwrap_or_else(|| "Widget".to_owned());
        let native = unsafe {
            match class.as_str() {
                "VBox" => build_box(api, runtime, widget, GTK_ORIENTATION_VERTICAL)?,
                "HBox" | "RadioGroup" => {
                    build_box(api, runtime, widget, GTK_ORIENTATION_HORIZONTAL)?
                }
                "Frame" => build_frame(api, runtime, widget)?,
                "Label" => build_label(api, &string_field(widget, "text")),
                "Text" => build_text(api, widget),
                "RichText" => build_rich_text(api, widget),
                "Input" => build_input(api, runtime, widget),
                "DatePicker" => build_date_picker(api, runtime, widget),
                "Checkbox" => {
                    let check = build_check(api, widget);
                    connect_widget_event(
                        api,
                        check,
                        "toggled",
                        runtime,
                        widget,
                        "change",
                        SignalKind::CheckToggled,
                    );
                    check
                }
                "Radio" => {
                    let radio = build_check(api, widget);
                    connect_widget_event(
                        api,
                        radio,
                        "toggled",
                        runtime,
                        widget,
                        "change",
                        SignalKind::RadioToggled,
                    );
                    radio
                }
                "Select" => {
                    let select = build_select(api, widget);
                    connect_widget_notify(
                        api,
                        select,
                        "notify::selected",
                        runtime,
                        widget,
                        "change",
                        SignalKind::SelectChanged,
                    );
                    select
                }
                "Button" => {
                    let button = (api.gtk_button_new_with_label)(
                        c_string(&string_field(widget, "text")).as_ptr(),
                    );
                    connect_widget_event(
                        api,
                        button,
                        "clicked",
                        runtime,
                        widget,
                        "click",
                        SignalKind::Plain,
                    );
                    button
                }
                "Separator" => (api.gtk_separator_new)(orientation_field(widget)),
                "Slider" => {
                    let slider = build_slider(api, widget);
                    connect_widget_event(
                        api,
                        slider,
                        "value-changed",
                        runtime,
                        widget,
                        "change",
                        SignalKind::SliderChanged,
                    );
                    slider
                }
                "Progress" => build_progress(api, widget),
                "Tabs" => build_tabs(api, runtime, widget)?,
                "Tab" => build_box(api, runtime, widget, GTK_ORIENTATION_VERTICAL)?,
                "ListView" => build_list_view(api, runtime, widget, false),
                "TreeView" => build_tree_view(api, runtime, widget),
                "Image" => build_image(api, runtime, widget),
                _ => build_box(api, runtime, widget, GTK_ORIENTATION_VERTICAL)?,
            }
        };
        if native.is_null() {
            return Err(gui_error(
                "GUI_BACKEND",
                &format!("GTK4 could not create {class}"),
            ));
        }
        WIDGETS.with(|widgets| {
            widgets.borrow_mut().insert(object_key(widget), native);
        });
        if matches!(class.as_str(), "Text" | "RichText" | "Label") {
            connect_click_controller(api, native, runtime, widget);
        }
        apply_common(api, widget, native);
        Ok(native)
    }

    unsafe fn build_box(
        api: &GtkApi,
        runtime: &Runtime,
        widget: &Value,
        orientation: c_int,
    ) -> Result<*mut c_void> {
        let spacing = number_field(widget, "gap", 0.0) as c_int;
        let native = (api.gtk_box_new)(orientation, spacing);
        let is_radio_group = matches!(class_name(widget).as_deref(), Some("RadioGroup"));
        let mut first_radio: Option<*mut c_void> = None;
        for child in children_values(widget) {
            if is_menu_kind_value(&child) {
                continue;
            }
            let child_native = build_widget(api, runtime, &child)?;
            if is_radio_group && matches!(class_name(&child).as_deref(), Some("Radio")) {
                if let Some(group) = first_radio {
                    (api.gtk_check_button_set_group)(child_native, group);
                } else {
                    first_radio = Some(child_native);
                }
            }
            (api.gtk_box_append)(native, child_native);
        }
        Ok(native)
    }

    unsafe fn build_frame(api: &GtkApi, runtime: &Runtime, widget: &Value) -> Result<*mut c_void> {
        let native = (api.gtk_frame_new)(c_string(&string_field(widget, "label")).as_ptr());
        let children = children_values(widget)
            .into_iter()
            .filter(|child| !is_menu_kind_value(child))
            .collect::<Vec<_>>();
        if children.len() == 1 {
            let child_native = build_widget(api, runtime, &children[0])?;
            (api.gtk_frame_set_child)(native, child_native);
        } else if !children.is_empty() {
            let box_native = (api.gtk_box_new)(GTK_ORIENTATION_VERTICAL, 4);
            for child in children {
                let child_native = build_widget(api, runtime, &child)?;
                (api.gtk_box_append)(box_native, child_native);
            }
            (api.gtk_frame_set_child)(native, box_native);
        }
        Ok(native)
    }

    unsafe fn build_text(api: &GtkApi, widget: &Value) -> *mut c_void {
        let native = build_label(api, &string_field(widget, "value"));
        (api.gtk_label_set_wrap)(native, bool_field(widget, "wrap", true) as c_int);
        native
    }

    unsafe fn build_rich_text(api: &GtkApi, widget: &Value) -> *mut c_void {
        let native = build_label(api, "");
        (api.gtk_label_set_markup)(native, c_string(&string_field(widget, "value")).as_ptr());
        (api.gtk_label_set_wrap)(native, 1);
        native
    }

    unsafe fn build_label(api: &GtkApi, text: &str) -> *mut c_void {
        let native = (api.gtk_label_new)(c_string(text).as_ptr());
        (api.gtk_label_set_xalign)(native, 0.0);
        native
    }

    unsafe fn build_input(api: &GtkApi, runtime: &Runtime, widget: &Value) -> *mut c_void {
        let native = (api.gtk_entry_new)();
        (api.gtk_editable_set_text)(native, c_string(&string_field(widget, "value")).as_ptr());
        connect_widget_event(
            api,
            native,
            "changed",
            runtime,
            widget,
            "input",
            SignalKind::EntryChanged,
        );
        native
    }

    unsafe fn build_date_picker(api: &GtkApi, runtime: &Runtime, widget: &Value) -> *mut c_void {
        let native = (api.gtk_box_new)(GTK_ORIENTATION_HORIZONTAL, 0);
        let entry = (api.gtk_entry_new)();
        (api.gtk_editable_set_text)(entry, c_string(&string_field(widget, "value")).as_ptr());
        connect_widget_event(
            api,
            entry,
            "changed",
            runtime,
            widget,
            "change",
            SignalKind::EntryChanged,
        );

        let button = (api.gtk_menu_button_new)();
        (api.gtk_menu_button_set_label)(button, c_string("...").as_ptr());
        let popover = (api.gtk_popover_new)();
        let calendar = (api.gtk_calendar_new)();
        select_calendar_date(api, calendar, &string_field(widget, "value"));
        connect_widget_event(
            api,
            calendar,
            "day-selected",
            runtime,
            widget,
            "change",
            SignalKind::CalendarSelected,
        );
        connect_click_controller_with_kind(
            api,
            calendar,
            runtime,
            widget,
            "change",
            SignalKind::CalendarAccepted,
        );
        (api.gtk_popover_set_child)(popover, calendar);
        (api.gtk_menu_button_set_popover)(button, popover);

        (api.gtk_box_append)(native, entry);
        (api.gtk_box_append)(native, button);
        DATE_PICKER_ENTRIES.with(|entries| {
            entries.borrow_mut().insert(object_key(widget), entry);
        });
        DATE_PICKER_CALENDARS.with(|calendars| {
            calendars.borrow_mut().insert(object_key(widget), calendar);
        });
        DATE_PICKER_BUTTONS.with(|buttons| {
            buttons.borrow_mut().insert(object_key(widget), button);
        });
        native
    }

    unsafe fn build_check(api: &GtkApi, widget: &Value) -> *mut c_void {
        let native = (api.gtk_check_button_new_with_label)(
            c_string(&string_field(widget, "label")).as_ptr(),
        );
        (api.gtk_check_button_set_active)(native, bool_field(widget, "checked", false) as c_int);
        native
    }

    unsafe fn build_select(api: &GtkApi, widget: &Value) -> *mut c_void {
        let labels = option_labels(widget);
        let c_labels = labels
            .iter()
            .map(|label| c_string(label))
            .collect::<Vec<_>>();
        let mut pointers = c_labels
            .iter()
            .map(|label| label.as_ptr())
            .collect::<Vec<_>>();
        pointers.push(std::ptr::null());
        let native = (api.gtk_drop_down_new_from_strings)(pointers.as_ptr());
        if let Some(index) = selected_option_index(widget) {
            (api.gtk_drop_down_set_selected)(native, index as u32);
        }
        native
    }

    unsafe fn build_image(api: &GtkApi, runtime: &Runtime, widget: &Value) -> *mut c_void {
        let src = string_field(widget, "src");
        let native = if src.is_empty() {
            (api.gtk_label_new)(c_string(&string_field(widget, "alt")).as_ptr())
        } else {
            (api.gtk_picture_new_for_filename)(c_string(&src).as_ptr())
        };
        connect_click_controller(api, native, runtime, widget);
        native
    }

    fn connect_click_controller(
        api: &GtkApi,
        native: *mut c_void,
        runtime: &Runtime,
        widget: &Value,
    ) {
        connect_click_controller_with_kind(
            api,
            native,
            runtime,
            widget,
            "click",
            SignalKind::Plain,
        );
    }

    fn connect_click_controller_with_kind(
        api: &GtkApi,
        native: *mut c_void,
        runtime: &Runtime,
        widget: &Value,
        event: &str,
        kind: SignalKind,
    ) {
        unsafe {
            let gesture = (api.gtk_gesture_click_new)();
            let signal = c_string("pressed");
            let handler = Box::into_raw(Box::new(SignalHandler {
                runtime,
                widget: widget.clone(),
                event: event.to_owned(),
                kind,
            }));
            let callback: unsafe extern "C" fn(*mut c_void, c_int, f64, f64, *mut c_void) =
                gesture_pressed;
            (api.g_signal_connect_data)(
                gesture,
                signal.as_ptr(),
                callback as *mut c_void,
                handler.cast(),
                std::ptr::null_mut(),
                0,
            );
            (api.gtk_widget_add_controller)(native, gesture);
        }
    }

    unsafe fn build_slider(api: &GtkApi, widget: &Value) -> *mut c_void {
        let native = (api.gtk_scale_new_with_range)(
            orientation_field(widget),
            number_field(widget, "min", 0.0),
            number_field(widget, "max", 100.0),
            number_field(widget, "step", 1.0),
        );
        (api.gtk_range_set_value)(native, number_field(widget, "value", 0.0));
        native
    }

    unsafe fn build_progress(api: &GtkApi, widget: &Value) -> *mut c_void {
        let native = (api.gtk_progress_bar_new)();
        apply_progress(api, widget, native);
        (api.gtk_progress_bar_set_show_text)(
            native,
            bool_field(widget, "show_text", false) as c_int,
        );
        if bool_field(widget, "indeterminate", false) {
            (api.gtk_progress_bar_pulse)(native);
        }
        native
    }

    unsafe fn apply_progress(api: &GtkApi, widget: &Value, native: *mut c_void) {
        let min = number_field(widget, "min", 0.0);
        let max = number_field(widget, "max", 100.0);
        let value = number_field(widget, "value", 0.0);
        let fraction = if max > min {
            ((value - min) / (max - min)).clamp(0.0, 1.0)
        } else {
            0.0
        };
        (api.gtk_progress_bar_set_fraction)(native, fraction);
    }

    unsafe fn build_tabs(api: &GtkApi, runtime: &Runtime, widget: &Value) -> Result<*mut c_void> {
        let native = (api.gtk_notebook_new)();
        for child in children_values(widget) {
            let page = build_widget(api, runtime, &child)?;
            let title = c_string(&string_field(&child, "title"));
            let label = (api.gtk_label_new)(title.as_ptr());
            (api.gtk_notebook_append_page)(native, page, label);
        }
        Ok(native)
    }

    unsafe fn build_tree_view(api: &GtkApi, runtime: &Runtime, widget: &Value) -> *mut c_void {
        let model = create_tree_list_model(api, widget);
        let selection = (api.gtk_single_selection_new)(model);
        let factory = (api.gtk_signal_list_item_factory_new)();
        connect_tree_item_factory(api, factory, runtime, widget);
        let native = (api.gtk_list_view_new)(selection, factory);
        let key = object_key(widget);
        TREE_LIST_MODELS.with(|models| {
            models.borrow_mut().insert(key, model);
        });
        TREE_SELECTIONS.with(|selections| {
            selections.borrow_mut().insert(key, selection);
        });
        connect_widget_notify(
            api,
            selection,
            "notify::selected",
            runtime,
            widget,
            "select",
            SignalKind::TreeListSelected,
        );
        connect_listview_activate(api, native, runtime, widget);
        apply_tree_view_expansion(api, widget);
        native
    }

    unsafe fn create_tree_list_model(api: &GtkApi, widget: &Value) -> *mut c_void {
        let root = string_list_for_items(api, widget, None);
        let data = Box::into_raw(Box::new(TreeListModelData {
            widget: widget.clone(),
        }));
        (api.gtk_tree_list_model_new)(
            root,
            0,
            0,
            tree_list_create_children,
            data.cast(),
            Some(drop_tree_list_model_data),
        )
    }

    unsafe extern "C" fn drop_tree_list_model_data(data: *mut c_void) {
        if !data.is_null() {
            drop(Box::from_raw(data.cast::<TreeListModelData>()));
        }
    }

    unsafe extern "C" fn tree_list_create_children(
        item: *mut c_void,
        data: *mut c_void,
    ) -> *mut c_void {
        if item.is_null() || data.is_null() {
            return std::ptr::null_mut();
        }
        let Ok(api) = api() else {
            return std::ptr::null_mut();
        };
        let data = &*data.cast::<TreeListModelData>();
        let encoded = c_text((api.gtk_string_object_get_string)(item));
        let path = tree_encoded_path_usize(&encoded);
        string_list_for_items(api, &data.widget, Some(&path))
    }

    unsafe fn string_list_for_items(
        api: &GtkApi,
        widget: &Value,
        parent_path: Option<&[usize]>,
    ) -> *mut c_void {
        let items = match field(widget, "items") {
            Value::Array(items) => match parent_path {
                Some(path) => tree_item_at_path(&items, path)
                    .map(|item| tree_item_children(&item))
                    .unwrap_or_default(),
                None => items,
            },
            _ => Vec::new(),
        };
        if parent_path.is_some() && items.is_empty() {
            return std::ptr::null_mut();
        }
        let list = (api.gtk_string_list_new)(std::ptr::null());
        for (index, item) in items.iter().enumerate() {
            let mut path = parent_path.map(|path| path.to_vec()).unwrap_or_default();
            path.push(index);
            (api.gtk_string_list_append)(list, c_string(&tree_encoded_item(&path, item)).as_ptr());
        }
        list
    }

    fn tree_encoded_item(path: &[usize], item: &Value) -> String {
        format!("{}\t{}", path_key_usize(path), item_label(item))
    }

    fn tree_encoded_path_usize(encoded: &str) -> Vec<usize> {
        encoded
            .split_once('\t')
            .map(|(path, _)| path)
            .unwrap_or(encoded)
            .split('/')
            .filter_map(|index| index.parse::<usize>().ok())
            .collect()
    }

    fn tree_encoded_path_values(encoded: &str) -> Vec<Value> {
        tree_encoded_path_usize(encoded)
            .into_iter()
            .map(|index| Value::Number(index as f64))
            .collect()
    }

    fn tree_encoded_label(encoded: &str) -> String {
        encoded
            .split_once('\t')
            .map(|(_, label)| label.to_owned())
            .unwrap_or_else(|| encoded.to_owned())
    }

    fn connect_tree_item_factory(
        api: &GtkApi,
        factory: *mut c_void,
        runtime: &Runtime,
        widget: &Value,
    ) {
        let setup = c_string("setup");
        let bind = c_string("bind");
        let data = Box::into_raw(Box::new(TreeFactoryData {
            api: api as *const GtkApi,
            runtime,
            widget: widget.clone(),
        }));
        unsafe {
            (api.g_signal_connect_data)(
                factory,
                setup.as_ptr(),
                tree_factory_setup as *mut c_void,
                data.cast(),
                std::ptr::null_mut(),
                0,
            );
            (api.g_signal_connect_data)(
                factory,
                bind.as_ptr(),
                tree_factory_bind as *mut c_void,
                data.cast(),
                std::ptr::null_mut(),
                0,
            );
        }
    }

    unsafe extern "C" fn tree_factory_setup(
        _factory: *mut c_void,
        list_item: *mut c_void,
        data: *mut c_void,
    ) {
        let data = &*data.cast::<TreeFactoryData>();
        let api = &*data.api;
        let Some(expander_new) = api.gtk_tree_expander_new else {
            return;
        };
        let expander = expander_new();
        let label = build_label(api, "");
        if let Some(set_child) = api.gtk_tree_expander_set_child {
            set_child(expander, label);
        }
        (api.gtk_list_item_set_child)(list_item, expander);
    }

    unsafe extern "C" fn tree_factory_bind(
        _factory: *mut c_void,
        list_item: *mut c_void,
        data: *mut c_void,
    ) {
        let data = &*data.cast::<TreeFactoryData>();
        let api = &*data.api;
        let expander = (api.gtk_list_item_get_child)(list_item);
        let row = (api.gtk_list_item_get_item)(list_item);
        if expander.is_null() || row.is_null() {
            return;
        }
        (api.gtk_tree_expander_set_list_row)(expander, row);
        let item = (api.gtk_tree_list_row_get_item)(row);
        if item.is_null() {
            return;
        }
        let encoded = c_text((api.gtk_string_object_get_string)(item));
        let label = (api.gtk_tree_expander_get_child)(expander);
        if !label.is_null() {
            (api.gtk_label_set_text)(label, c_string(&tree_encoded_label(&encoded)).as_ptr());
        }
        connect_tree_row_expanded(api, data.runtime, &data.widget, row);
    }

    unsafe fn connect_tree_row_expanded(
        api: &GtkApi,
        runtime: *const Runtime,
        widget: &Value,
        row: *mut c_void,
    ) {
        let key = row as usize;
        let already_connected = TREE_ROW_EXPANDED_HANDLERS.with(|handlers| {
            let mut handlers = handlers.borrow_mut();
            handlers.insert(key, true).is_some()
        });
        if already_connected {
            return;
        }
        let signal = c_string("notify::expanded");
        let handler = Box::into_raw(Box::new(SignalHandler {
            runtime,
            widget: widget.clone(),
            event: String::new(),
            kind: SignalKind::TreeListSelected,
        }));
        (api.g_signal_connect_data)(
            row,
            signal.as_ptr(),
            tree_row_expanded_signal as *mut c_void,
            handler.cast(),
            std::ptr::null_mut(),
            0,
        );
    }

    unsafe extern "C" fn tree_row_expanded_signal(
        row: *mut c_void,
        _pspec: *mut c_void,
        data: *mut c_void,
    ) {
        if SYNCING.with(|syncing| *syncing.borrow()) {
            return;
        }
        if data.is_null() {
            return;
        }
        let handler = &*data.cast::<SignalHandler>();
        let runtime = &*handler.runtime;
        let Ok(api) = api() else {
            return;
        };
        let Some(path) = tree_list_row_path(api, row) else {
            return;
        };
        let expanded_now = (api.gtk_tree_list_row_get_expanded)(row) != 0;
        let key = path_key_values(&path);
        let mut expanded = expanded_tree_keys(&handler.widget);
        if expanded_now {
            expanded.insert(key, Value::Boolean(true));
        } else {
            expanded.remove(&key);
        }
        SYNCING.with(|syncing| {
            *syncing.borrow_mut() = true;
        });
        set_field(&handler.widget, "expanded_path_keys", Value::Dict(expanded));
        SYNCING.with(|syncing| {
            *syncing.borrow_mut() = false;
        });
        let _ = dispatch_named_event(
            runtime,
            &handler.widget,
            if expanded_now { "expand" } else { "collapse" },
            Value::Null,
        );
    }

    fn connect_listview_activate(
        api: &GtkApi,
        native: *mut c_void,
        runtime: &Runtime,
        widget: &Value,
    ) {
        let signal = c_string("activate");
        let handler = Box::into_raw(Box::new(SignalHandler {
            runtime,
            widget: widget.clone(),
            event: "activate".to_owned(),
            kind: SignalKind::TreeListSelected,
        }));
        unsafe {
            (api.g_signal_connect_data)(
                native,
                signal.as_ptr(),
                listview_activate_signal as *mut c_void,
                handler.cast(),
                std::ptr::null_mut(),
                0,
            );
        }
    }

    fn rebuild_tree_view_model(api: &GtkApi, widget: &Value) {
        unsafe {
            let model = create_tree_list_model(api, widget);
            let key = object_key(widget);
            TREE_LIST_MODELS.with(|models| {
                models.borrow_mut().insert(key, model);
            });
            TREE_SELECTIONS.with(|selections| {
                if let Some(selection) = selections.borrow().get(&key).copied() {
                    (api.gtk_single_selection_set_model)(selection, model);
                }
            });
            apply_tree_view_expansion(api, widget);
        }
    }

    fn apply_tree_view_expansion(api: &GtkApi, widget: &Value) {
        let key = object_key(widget);
        let Some(model) = TREE_LIST_MODELS.with(|models| models.borrow().get(&key).copied()) else {
            return;
        };
        let expanded = expanded_tree_keys(widget);
        unsafe {
            SYNCING.with(|syncing| {
                *syncing.borrow_mut() = true;
            });
            let mut index = 0;
            while index < (api.g_list_model_get_n_items)(model) {
                let row = (api.gtk_tree_list_model_get_row)(model, index);
                if let Some(path) = tree_list_row_path(api, row) {
                    (api.gtk_tree_list_row_set_expanded)(
                        row,
                        expanded.contains_key(&path_key_values(&path)) as c_int,
                    );
                }
                index += 1;
            }
            SYNCING.with(|syncing| {
                *syncing.borrow_mut() = false;
            });
        }
    }

    unsafe fn tree_list_position_path(
        api: &GtkApi,
        widget: &Value,
        position: u32,
    ) -> Option<Vec<Value>> {
        let model =
            TREE_LIST_MODELS.with(|models| models.borrow().get(&object_key(widget)).copied())?;
        let row = (api.gtk_tree_list_model_get_row)(model, position);
        tree_list_row_path(api, row)
    }

    unsafe fn tree_list_row_path(api: &GtkApi, row: *mut c_void) -> Option<Vec<Value>> {
        if row.is_null() {
            return None;
        }
        let item = (api.gtk_tree_list_row_get_item)(row);
        if item.is_null() {
            return None;
        }
        Some(tree_encoded_path_values(&c_text((api
            .gtk_string_object_get_string)(
            item
        ))))
    }

    unsafe fn build_list_view(
        api: &GtkApi,
        runtime: &Runtime,
        widget: &Value,
        tree: bool,
    ) -> *mut c_void {
        let native = (api.gtk_list_box_new)();
        rebuild_list_view(api, native, widget, tree);
        connect_listbox_event(
            api,
            native,
            "row-selected",
            runtime,
            widget,
            "select",
            if tree {
                SignalKind::TreeSelected
            } else {
                SignalKind::ListSelected
            },
        );
        connect_listbox_event(
            api,
            native,
            "row-activated",
            runtime,
            widget,
            "activate",
            if tree {
                SignalKind::TreeSelected
            } else {
                SignalKind::ListSelected
            },
        );
        native
    }

    unsafe fn rebuild_list_view(api: &GtkApi, native: *mut c_void, widget: &Value, tree: bool) {
        clear_list_box(api, native);
        if tree {
            for (label, depth, has_children) in flattened_tree_entries(widget) {
                let row = build_tree_row(api, &label, depth, has_children);
                (api.gtk_list_box_append)(native, row);
            }
        } else {
            for label in item_labels(&field(widget, "items")) {
                let row = build_label(api, &label);
                (api.gtk_list_box_append)(native, row);
            }
        }
    }

    unsafe fn clear_list_box(api: &GtkApi, native: *mut c_void) {
        loop {
            let child = (api.gtk_widget_get_first_child)(native);
            if child.is_null() {
                break;
            }
            (api.gtk_list_box_remove)(native, child);
        }
    }

    unsafe fn build_tree_row(
        api: &GtkApi,
        label: &str,
        depth: usize,
        has_children: bool,
    ) -> *mut c_void {
        let child = build_label(api, label);
        let Some(expander_new) = api.gtk_tree_expander_new else {
            (api.gtk_widget_set_margin_start)(child, (depth as c_int) * 16);
            return child;
        };
        let expander = expander_new();
        if expander.is_null() {
            (api.gtk_widget_set_margin_start)(child, (depth as c_int) * 16);
            return child;
        }
        if let Some(set_child) = api.gtk_tree_expander_set_child {
            set_child(expander, child);
        }
        if let Some(set_indent) = api.gtk_tree_expander_set_indent_for_icon {
            set_indent(expander, has_children as c_int);
        }
        (api.gtk_widget_set_margin_start)(expander, (depth as c_int) * 16);
        expander
    }

    fn orientation_field(widget: &Value) -> c_int {
        if string_field(widget, "orientation") == "vertical" {
            GTK_ORIENTATION_VERTICAL
        } else {
            GTK_ORIENTATION_HORIZONTAL
        }
    }

    fn apply_common(api: &GtkApi, widget: &Value, native: *mut c_void) {
        unsafe {
            (api.gtk_widget_set_sensitive)(native, field(widget, "enabled").is_truthy() as c_int);
            (api.gtk_widget_set_visible)(native, field(widget, "visible").is_truthy() as c_int);
        }
        apply_size_request(api, widget, native);
        apply_padding(api, widget, native);
    }

    fn apply_size_request(api: &GtkApi, widget: &Value, native: *mut c_void) {
        let width = geometry_dimension(widget, "width");
        let height = geometry_dimension(widget, "height");
        if width.is_none() && height.is_none() {
            return;
        }
        unsafe {
            (api.gtk_widget_set_size_request)(native, width.unwrap_or(-1), height.unwrap_or(-1));
        }
    }

    fn geometry_dimension(widget: &Value, axis: &str) -> Option<c_int> {
        let direct = number_field_optional(widget, axis);
        let max = number_field_optional(widget, &format!("max{axis}"));
        let min = number_field_optional(widget, &format!("min{axis}"));
        let mut value = direct.or(max);
        if let Some(min) = min {
            value = Some(value.map(|current| current.max(min)).unwrap_or(min));
        }
        value.map(|value| value.max(0.0) as c_int)
    }

    fn apply_padding(api: &GtkApi, widget: &Value, native: *mut c_void) {
        let Some((top, right, bottom, left)) = padding_edges(widget) else {
            return;
        };
        unsafe {
            (api.gtk_widget_set_margin_top)(native, top);
            (api.gtk_widget_set_margin_end)(native, right);
            (api.gtk_widget_set_margin_bottom)(native, bottom);
            (api.gtk_widget_set_margin_start)(native, left);
        }
    }

    fn padding_edges(widget: &Value) -> Option<(c_int, c_int, c_int, c_int)> {
        match unshared(&field(widget, "padding")) {
            Value::Number(value) => {
                let all = value.max(0.0) as c_int;
                (all != 0).then_some((all, all, all, all))
            }
            Value::Array(values) => {
                let numbers = values.iter().filter_map(padding_number).collect::<Vec<_>>();
                match numbers.as_slice() {
                    [] => None,
                    [all] => Some((*all, *all, *all, *all)),
                    [vertical, horizontal] => {
                        Some((*vertical, *horizontal, *vertical, *horizontal))
                    }
                    [top, horizontal, bottom] => Some((*top, *horizontal, *bottom, *horizontal)),
                    [top, right, bottom, left, ..] => Some((*top, *right, *bottom, *left)),
                }
            }
            _ => None,
        }
    }

    fn padding_number(value: &Value) -> Option<c_int> {
        match unshared(value) {
            Value::Number(value) => Some(value.max(0.0) as c_int),
            Value::String(value) => value
                .parse::<f64>()
                .ok()
                .map(|value| value.max(0.0) as c_int),
            _ => None,
        }
    }

    unsafe fn select_calendar_date(api: &GtkApi, calendar: *mut c_void, text: &str) {
        let Some((year, month, day)) = parse_date_text(text) else {
            return;
        };
        let date = (api.g_date_time_new_local)(year, month, day, 0, 0, 0.0);
        if !date.is_null() {
            (api.gtk_calendar_select_day)(calendar, date);
            (api.g_date_time_unref)(date);
        }
    }

    unsafe fn calendar_date_string(api: &GtkApi, calendar: *mut c_void) -> Option<String> {
        let date = (api.gtk_calendar_get_date)(calendar);
        if date.is_null() {
            return None;
        }
        let year = (api.g_date_time_get_year)(date);
        let month = (api.g_date_time_get_month)(date);
        let day = (api.g_date_time_get_day_of_month)(date);
        (api.g_date_time_unref)(date);
        Some(format!("{year:04}-{month:02}-{day:02}"))
    }

    fn parse_date_text(text: &str) -> Option<(c_int, c_int, c_int)> {
        let mut parts = text.split('-');
        let year = parts.next()?.parse::<c_int>().ok()?;
        let month = parts.next()?.parse::<c_int>().ok()?;
        let day = parts.next()?.parse::<c_int>().ok()?;
        if parts.next().is_some()
            || !(1..=9999).contains(&year)
            || !(1..=12).contains(&month)
            || !(1..=31).contains(&day)
        {
            return None;
        }
        Some((year, month, day))
    }

    fn item_labels(items: &Value) -> Vec<String> {
        match items {
            Value::Array(items) => items.iter().map(item_label).collect(),
            _ => Vec::new(),
        }
    }

    fn item_label(item: &Value) -> String {
        match unshared(item) {
            Value::Dict(map) => map_text_field(&map, "label")
                .or_else(|| map_text_field(&map, "value"))
                .unwrap_or_default(),
            Value::PairList(pairs) => pairlist_text_field(&pairs, "label")
                .or_else(|| pairlist_text_field(&pairs, "value"))
                .unwrap_or_default(),
            Value::Null => String::new(),
            other => other.render(),
        }
    }

    fn flattened_tree_entries(widget: &Value) -> Vec<(String, usize, bool)> {
        let mut entries = Vec::new();
        if let Value::Array(items) = field(widget, "items") {
            let expanded = expanded_tree_keys(widget);
            flatten_tree_entries(&items, Vec::new(), 0, &expanded, &mut entries);
        }
        entries
    }

    fn flatten_tree_entries(
        items: &[Value],
        prefix: Vec<usize>,
        depth: usize,
        expanded: &HashMap<String, Value>,
        entries: &mut Vec<(String, usize, bool)>,
    ) {
        for (index, item) in items.iter().enumerate() {
            let mut path = prefix.clone();
            path.push(index);
            let children = tree_item_children(item);
            let has_children = !children.is_empty();
            entries.push((item_label(item), depth, has_children));
            if has_children && expanded.contains_key(&path_key_usize(&path)) {
                flatten_tree_entries(&children, path, depth + 1, expanded, entries);
            }
        }
    }

    fn flattened_tree_paths(widget: &Value) -> Vec<Vec<Value>> {
        let mut paths = Vec::new();
        if let Value::Array(items) = field(widget, "items") {
            let expanded = expanded_tree_keys(widget);
            flatten_tree_paths(&items, Vec::new(), &expanded, &mut paths);
        }
        paths
    }

    fn flatten_tree_paths(
        items: &[Value],
        prefix: Vec<Value>,
        expanded: &HashMap<String, Value>,
        paths: &mut Vec<Vec<Value>>,
    ) {
        for (index, item) in items.iter().enumerate() {
            let mut path = prefix.clone();
            path.push(Value::Number(index as f64));
            paths.push(path.clone());
            let children = tree_item_children(item);
            if !children.is_empty() && expanded.contains_key(&path_key_values(&path)) {
                flatten_tree_paths(&children, path, expanded, paths);
            }
        }
    }

    fn expanded_tree_keys(widget: &Value) -> HashMap<String, Value> {
        match field(widget, "expanded_path_keys") {
            Value::Dict(map) => map,
            _ => HashMap::new(),
        }
    }

    fn option_labels(widget: &Value) -> Vec<String> {
        match field(widget, "options") {
            Value::Array(options) => options
                .into_iter()
                .map(|option| match unshared(&option) {
                    Value::Dict(map) => map_text_field(&map, "label")
                        .or_else(|| map_text_field(&map, "value"))
                        .unwrap_or_default(),
                    Value::PairList(pairs) => pairlist_text_field(&pairs, "label")
                        .or_else(|| pairlist_text_field(&pairs, "value"))
                        .unwrap_or_default(),
                    other => other.render(),
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    fn option_value_at(widget: &Value, index: usize) -> Value {
        match field(widget, "options") {
            Value::Array(options) => options
                .get(index)
                .map(|option| match unshared(option) {
                    Value::Dict(map) => map
                        .get("value")
                        .cloned()
                        .or_else(|| map.get("label").cloned())
                        .unwrap_or(Value::Null),
                    Value::PairList(pairs) => pairlist_value_field(&pairs, "value")
                        .or_else(|| pairlist_value_field(&pairs, "label"))
                        .unwrap_or(Value::Null),
                    other => other.clone(),
                })
                .unwrap_or(Value::Null),
            _ => Value::Null,
        }
    }

    fn selected_option_index(widget: &Value) -> Option<usize> {
        let selected = field(widget, "value");
        match field(widget, "options") {
            Value::Array(options) => options
                .iter()
                .enumerate()
                .find(|(_, option)| values_equal(&option_value(option), &selected))
                .map(|(index, _)| index),
            _ => None,
        }
    }

    fn option_value(option: &Value) -> Value {
        match unshared(option) {
            Value::Dict(map) => map
                .get("value")
                .cloned()
                .or_else(|| map.get("label").cloned())
                .unwrap_or(Value::Null),
            Value::PairList(pairs) => pairlist_value_field(&pairs, "value")
                .or_else(|| pairlist_value_field(&pairs, "label"))
                .unwrap_or(Value::Null),
            other => other.clone(),
        }
    }

    fn unshared(value: &Value) -> Value {
        let mut current = value.clone();
        for _ in 0..32 {
            match current {
                Value::Shared(shared) => {
                    current = shared.borrow().clone();
                }
                other => return other,
            }
        }
        current
    }

    fn map_text_field(map: &HashMap<String, Value>, key: &str) -> Option<String> {
        map.get(key).map(Value::render)
    }

    fn pairlist_text_field(pairs: &[(String, Value)], key: &str) -> Option<String> {
        pairs
            .iter()
            .find(|(field, _)| field == key)
            .map(|(_, value)| value.render())
    }

    fn pairlist_value_field(pairs: &[(String, Value)], key: &str) -> Option<Value> {
        pairs
            .iter()
            .find(|(field, _)| field == key)
            .map(|(_, value)| value.clone())
    }

    fn api() -> Result<&'static GtkApi> {
        GTK_API.with(|slot| {
            if let Some(api) = *slot.borrow() {
                return Ok(api);
            }
            let api = Box::leak(Box::new(unsafe { GtkApi::load()? }));
            *slot.borrow_mut() = Some(api);
            Ok(api)
        })
    }

    impl GtkApi {
        unsafe fn load() -> Result<Self> {
            let handle = open_gtk_library()?;
            let glib_handle = open_glib_library()?;
            let gio_handle = open_gio_library()?;
            let gobject_handle = open_gobject_library()?;
            Ok(Self {
                gtk_init_check: symbol(handle, "gtk_init_check")?,
                gtk_window_new: symbol(handle, "gtk_window_new")?,
                gtk_window_set_title: symbol(handle, "gtk_window_set_title")?,
                gtk_window_set_default_size: symbol(handle, "gtk_window_set_default_size")?,
                gtk_window_set_child: symbol(handle, "gtk_window_set_child")?,
                gtk_window_present: symbol(handle, "gtk_window_present")?,
                gtk_window_destroy: symbol(handle, "gtk_window_destroy")?,
                gtk_box_new: symbol(handle, "gtk_box_new")?,
                gtk_box_append: symbol(handle, "gtk_box_append")?,
                gtk_frame_new: symbol(handle, "gtk_frame_new")?,
                gtk_frame_set_child: symbol(handle, "gtk_frame_set_child")?,
                gtk_label_new: symbol(handle, "gtk_label_new")?,
                gtk_label_set_xalign: symbol(handle, "gtk_label_set_xalign")?,
                gtk_label_set_wrap: symbol(handle, "gtk_label_set_wrap")?,
                gtk_label_set_text: symbol(handle, "gtk_label_set_text")?,
                gtk_label_set_markup: symbol(handle, "gtk_label_set_markup")?,
                gtk_button_new_with_label: symbol(handle, "gtk_button_new_with_label")?,
                gtk_button_set_label: symbol(handle, "gtk_button_set_label")?,
                gtk_entry_new: symbol(handle, "gtk_entry_new")?,
                gtk_editable_set_text: symbol(handle, "gtk_editable_set_text")?,
                gtk_editable_get_text: symbol(handle, "gtk_editable_get_text")?,
                gtk_check_button_new_with_label: symbol(handle, "gtk_check_button_new_with_label")?,
                gtk_check_button_set_group: symbol(handle, "gtk_check_button_set_group")?,
                gtk_check_button_set_active: symbol(handle, "gtk_check_button_set_active")?,
                gtk_check_button_get_active: symbol(handle, "gtk_check_button_get_active")?,
                gtk_drop_down_new_from_strings: symbol(handle, "gtk_drop_down_new_from_strings")?,
                gtk_drop_down_get_selected: symbol(handle, "gtk_drop_down_get_selected")?,
                gtk_drop_down_set_selected: symbol(handle, "gtk_drop_down_set_selected")?,
                gtk_picture_new_for_filename: symbol(handle, "gtk_picture_new_for_filename")?,
                gtk_calendar_new: symbol(handle, "gtk_calendar_new")?,
                gtk_calendar_get_date: symbol(handle, "gtk_calendar_get_date")?,
                gtk_calendar_select_day: symbol(handle, "gtk_calendar_select_day")?,
                gtk_menu_button_new: symbol(handle, "gtk_menu_button_new")?,
                gtk_menu_button_set_label: symbol(handle, "gtk_menu_button_set_label")?,
                gtk_menu_button_set_popover: symbol(handle, "gtk_menu_button_set_popover")?,
                gtk_menu_button_popdown: symbol(handle, "gtk_menu_button_popdown")?,
                gtk_popover_new: symbol(handle, "gtk_popover_new")?,
                gtk_popover_set_child: symbol(handle, "gtk_popover_set_child")?,
                gtk_separator_new: symbol(handle, "gtk_separator_new")?,
                gtk_scale_new_with_range: symbol(handle, "gtk_scale_new_with_range")?,
                gtk_range_set_value: symbol(handle, "gtk_range_set_value")?,
                gtk_range_get_value: symbol(handle, "gtk_range_get_value")?,
                gtk_progress_bar_new: symbol(handle, "gtk_progress_bar_new")?,
                gtk_progress_bar_set_fraction: symbol(handle, "gtk_progress_bar_set_fraction")?,
                gtk_progress_bar_set_show_text: symbol(handle, "gtk_progress_bar_set_show_text")?,
                gtk_progress_bar_pulse: symbol(handle, "gtk_progress_bar_pulse")?,
                gtk_color_chooser_dialog_new: symbol(handle, "gtk_color_chooser_dialog_new")?,
                gtk_color_chooser_get_rgba: symbol(handle, "gtk_color_chooser_get_rgba")?,
                gtk_color_chooser_set_rgba: symbol(handle, "gtk_color_chooser_set_rgba")?,
                gtk_notebook_new: symbol(handle, "gtk_notebook_new")?,
                gtk_notebook_append_page: symbol(handle, "gtk_notebook_append_page")?,
                gtk_list_box_new: symbol(handle, "gtk_list_box_new")?,
                gtk_list_box_append: symbol(handle, "gtk_list_box_append")?,
                gtk_list_box_remove: symbol(handle, "gtk_list_box_remove")?,
                gtk_list_box_get_selected_row: symbol(handle, "gtk_list_box_get_selected_row")?,
                gtk_list_box_row_get_index: symbol(handle, "gtk_list_box_row_get_index")?,
                gtk_string_list_new: symbol(handle, "gtk_string_list_new")?,
                gtk_string_list_append: symbol(handle, "gtk_string_list_append")?,
                gtk_string_object_get_string: symbol(handle, "gtk_string_object_get_string")?,
                gtk_tree_list_model_new: symbol(handle, "gtk_tree_list_model_new")?,
                gtk_tree_list_model_get_row: symbol(handle, "gtk_tree_list_model_get_row")?,
                gtk_tree_list_row_get_item: symbol(handle, "gtk_tree_list_row_get_item")?,
                gtk_tree_list_row_set_expanded: symbol(handle, "gtk_tree_list_row_set_expanded")?,
                gtk_tree_list_row_get_expanded: symbol(handle, "gtk_tree_list_row_get_expanded")?,
                gtk_signal_list_item_factory_new: symbol(
                    handle,
                    "gtk_signal_list_item_factory_new",
                )?,
                gtk_list_item_set_child: symbol(handle, "gtk_list_item_set_child")?,
                gtk_list_item_get_child: symbol(handle, "gtk_list_item_get_child")?,
                gtk_list_item_get_item: symbol(handle, "gtk_list_item_get_item")?,
                gtk_list_view_new: symbol(handle, "gtk_list_view_new")?,
                gtk_single_selection_new: symbol(handle, "gtk_single_selection_new")?,
                gtk_single_selection_set_model: symbol(handle, "gtk_single_selection_set_model")?,
                gtk_single_selection_get_selected_item: symbol(
                    handle,
                    "gtk_single_selection_get_selected_item",
                )?,
                gtk_tree_expander_new: optional_symbol(handle, "gtk_tree_expander_new"),
                gtk_tree_expander_get_child: symbol(handle, "gtk_tree_expander_get_child")?,
                gtk_tree_expander_set_child: optional_symbol(handle, "gtk_tree_expander_set_child"),
                gtk_tree_expander_set_list_row: symbol(handle, "gtk_tree_expander_set_list_row")?,
                gtk_tree_expander_set_indent_for_icon: optional_symbol(
                    handle,
                    "gtk_tree_expander_set_indent_for_icon",
                ),
                gtk_file_chooser_native_new: symbol(handle, "gtk_file_chooser_native_new")?,
                gtk_native_dialog_show: symbol(handle, "gtk_native_dialog_show")?,
                gtk_native_dialog_destroy: symbol(handle, "gtk_native_dialog_destroy")?,
                gtk_file_chooser_set_select_multiple: symbol(
                    handle,
                    "gtk_file_chooser_set_select_multiple",
                )?,
                gtk_file_chooser_set_current_folder: symbol(
                    handle,
                    "gtk_file_chooser_set_current_folder",
                )?,
                gtk_file_chooser_set_current_name: symbol(
                    handle,
                    "gtk_file_chooser_set_current_name",
                )?,
                gtk_file_chooser_get_file: symbol(handle, "gtk_file_chooser_get_file")?,
                gtk_file_chooser_get_files: symbol(handle, "gtk_file_chooser_get_files")?,
                gtk_widget_set_sensitive: symbol(handle, "gtk_widget_set_sensitive")?,
                gtk_widget_set_visible: symbol(handle, "gtk_widget_set_visible")?,
                gtk_widget_set_size_request: symbol(handle, "gtk_widget_set_size_request")?,
                gtk_widget_get_first_child: symbol(handle, "gtk_widget_get_first_child")?,
                gtk_widget_set_margin_top: symbol(handle, "gtk_widget_set_margin_top")?,
                gtk_widget_set_margin_bottom: symbol(handle, "gtk_widget_set_margin_bottom")?,
                gtk_widget_set_margin_start: symbol(handle, "gtk_widget_set_margin_start")?,
                gtk_widget_set_margin_end: symbol(handle, "gtk_widget_set_margin_end")?,
                gtk_widget_add_controller: symbol(handle, "gtk_widget_add_controller")?,
                gtk_widget_grab_focus: symbol(handle, "gtk_widget_grab_focus")?,
                gtk_gesture_click_new: symbol(handle, "gtk_gesture_click_new")?,
                g_main_context_iteration: symbol(handle, "g_main_context_iteration")?,
                g_date_time_new_local: symbol(glib_handle, "g_date_time_new_local")?,
                g_date_time_get_year: symbol(glib_handle, "g_date_time_get_year")?,
                g_date_time_get_month: symbol(glib_handle, "g_date_time_get_month")?,
                g_date_time_get_day_of_month: symbol(glib_handle, "g_date_time_get_day_of_month")?,
                g_date_time_unref: symbol(glib_handle, "g_date_time_unref")?,
                g_file_new_for_path: symbol(gio_handle, "g_file_new_for_path")?,
                g_file_get_path: symbol(gio_handle, "g_file_get_path")?,
                g_list_model_get_n_items: symbol(gio_handle, "g_list_model_get_n_items")?,
                g_list_model_get_item: symbol(gio_handle, "g_list_model_get_item")?,
                g_object_unref: symbol(gobject_handle, "g_object_unref")?,
                g_error_free: symbol(glib_handle, "g_error_free")?,
                g_free: symbol(glib_handle, "g_free")?,
                gdk_rgba_parse: symbol(handle, "gdk_rgba_parse")?,
                g_signal_connect_data: symbol(gobject_handle, "g_signal_connect_data")?,
            })
        }
    }

    unsafe fn open_gtk_library() -> Result<&'static Library> {
        open_library(&[
            "libgtk-4.so.1",
            "libgtk-4.so",
            "libgtk-4-1.dll",
            "gtk-4-1.dll",
            "libgtk-4.1.dylib",
            "libgtk-4.dylib",
        ])
    }

    unsafe fn open_glib_library() -> Result<&'static Library> {
        open_library(&[
            "libglib-2.0.so.0",
            "libglib-2.0.so",
            "libglib-2.0-0.dll",
            "glib-2.0-0.dll",
            "libglib-2.0.0.dylib",
            "libglib-2.0.dylib",
        ])
    }

    unsafe fn open_gio_library() -> Result<&'static Library> {
        open_library(&[
            "libgio-2.0.so.0",
            "libgio-2.0.so",
            "libgio-2.0-0.dll",
            "gio-2.0-0.dll",
            "libgio-2.0.0.dylib",
            "libgio-2.0.dylib",
        ])
    }

    unsafe fn open_gobject_library() -> Result<&'static Library> {
        open_library(&[
            "libgobject-2.0.so.0",
            "libgobject-2.0.so",
            "libgobject-2.0-0.dll",
            "gobject-2.0-0.dll",
            "libgobject-2.0.0.dylib",
            "libgobject-2.0.dylib",
        ])
    }

    unsafe fn open_library(names: &[&str]) -> Result<&'static Library> {
        let mut errors = Vec::new();
        for name in names {
            match Library::new(name) {
                Ok(library) => return Ok(Box::leak(Box::new(library))),
                Err(error) => errors.push(format!("{name}: {error}")),
            }
        }
        Err(gui_error(
            "GUI_BACKEND",
            &format!("Could not load GTK4: {}", errors.join("; ")),
        ))
    }

    unsafe fn symbol<T: Copy>(library: &'static Library, name: &str) -> Result<T> {
        let c_name = c_string(name);
        let symbol = library
            .get::<T>(c_name.as_bytes_with_nul())
            .map_err(|error| {
                gui_error(
                    "GUI_BACKEND",
                    &format!("Could not load GTK4 symbol {name}: {error}"),
                )
            })?;
        Ok(*symbol)
    }

    unsafe fn optional_symbol<T: Copy>(library: &'static Library, name: &str) -> Option<T> {
        let c_name = c_string(name);
        library
            .get::<T>(c_name.as_bytes_with_nul())
            .ok()
            .map(|symbol| *symbol)
    }

    fn object_key(value: &Value) -> usize {
        match value {
            Value::Object(object) => Rc::as_ptr(object) as usize,
            _ => 0,
        }
    }

    fn string_field(value: &Value, name: &str) -> String {
        match field(value, name) {
            Value::String(text) => text,
            Value::Null => String::new(),
            other => other.render(),
        }
    }

    fn number_field(value: &Value, name: &str, default: f64) -> f64 {
        match field(value, name) {
            Value::Number(number) => number,
            _ => default,
        }
    }

    fn number_field_optional(value: &Value, name: &str) -> Option<f64> {
        match field(value, name) {
            Value::Number(number) => Some(number),
            _ => None,
        }
    }

    fn bool_field(value: &Value, name: &str, default: bool) -> bool {
        match field(value, name) {
            Value::Boolean(value) => value,
            Value::Null => default,
            other => other.is_truthy(),
        }
    }

    fn c_string(text: &str) -> CString {
        CString::new(text.replace('\0', "")).unwrap_or_else(|_| CString::new("").unwrap())
    }

    fn c_text(text: *const c_char) -> String {
        if text.is_null() {
            String::new()
        } else {
            unsafe { CStr::from_ptr(text).to_string_lossy().into_owned() }
        }
    }
}
