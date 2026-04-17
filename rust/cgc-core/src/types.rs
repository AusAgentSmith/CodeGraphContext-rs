/// Core data structures matching the Python dict schemas exactly.
/// These are the outputs of parsing and inputs to the Python persistence layer.

#[derive(Debug, Clone, Default)]
pub struct FileData {
    pub path: String,
    pub functions: Vec<FunctionData>,
    pub classes: Vec<ClassData>,
    pub variables: Vec<VariableData>,
    pub imports: Vec<ImportData>,
    pub function_calls: Vec<CallData>,
    /// Rust-specific `impl Trait for Type` blocks. Python + other langs
    /// leave this empty.
    pub impls: Vec<ImplData>,
    pub is_dependency: bool,
    pub lang: String,
}

/// A Rust `impl Trait for Type` block. We only care about the type/trait
/// names — methods inside an impl are already captured as FunctionData
/// with their class_context set to the target type.
///
/// Inherent impls (`impl Foo { ... }`, no trait) are not emitted since
/// their only useful fact — "Foo has these methods" — is already
/// encoded via class_context + Class CONTAINS Function.
#[derive(Debug, Clone)]
pub struct ImplData {
    pub type_name: String,
    pub trait_name: String,
    pub line_number: usize,
}

#[derive(Debug, Clone)]
pub struct FunctionData {
    pub name: String,
    pub line_number: usize,
    pub end_line: usize,
    pub args: Vec<String>,
    pub cyclomatic_complexity: usize,
    pub context: Option<String>,
    pub context_type: Option<String>,
    pub class_context: Option<String>,
    pub decorators: Vec<String>,
    pub lang: String,
    pub is_dependency: bool,
    pub source: Option<String>,
    pub docstring: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ClassData {
    pub name: String,
    pub line_number: usize,
    pub end_line: usize,
    pub bases: Vec<String>,
    pub context: Option<String>,
    pub decorators: Vec<String>,
    pub lang: String,
    pub is_dependency: bool,
    pub source: Option<String>,
    pub docstring: Option<String>,
    /// Discriminator for the graph label the writer should emit.
    /// The parser sets this; the conversion layer partitions classes
    /// by kind into separate per-kind lists keyed to match SYMBOL_LABELS.
    pub kind: ClassKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClassKind {
    #[default]
    Class,
    Struct,
    Enum,
    Trait,
    Interface,
}

impl ClassKind {
    /// Python-side dict key for lists of this kind — matches SYMBOL_LABELS.
    pub fn py_key(self) -> &'static str {
        match self {
            Self::Class => "classes",
            Self::Struct => "structs",
            Self::Enum => "enums",
            Self::Trait => "traits",
            Self::Interface => "interfaces",
        }
    }
}

#[derive(Debug, Clone)]
pub struct VariableData {
    pub name: String,
    pub line_number: usize,
    pub value: Option<String>,
    pub type_annotation: Option<String>,
    pub context: Option<String>,
    pub class_context: Option<String>,
    pub lang: String,
    pub is_dependency: bool,
}

#[derive(Debug, Clone)]
pub struct ImportData {
    pub name: String,
    pub full_import_name: String,
    pub line_number: usize,
    pub alias: Option<String>,
    /// (context_name, context_type)
    pub context: (Option<String>, Option<String>),
    pub lang: String,
    pub is_dependency: bool,
}

#[derive(Debug, Clone)]
pub struct CallData {
    pub name: String,
    pub full_name: String,
    pub line_number: usize,
    pub args: Vec<String>,
    pub inferred_obj_type: Option<String>,
    /// (context_name, context_type, context_line)
    pub context: (Option<String>, Option<String>, Option<usize>),
    /// (class_name, class_type)
    pub class_context: (Option<String>, Option<String>),
    pub lang: String,
    pub is_dependency: bool,
    pub is_indirect_call: bool,
}

/// Result of parsing: either success with FileData or an error.
#[derive(Debug)]
pub enum ParseResult {
    Ok(FileData),
    Err { path: String, error: String },
}
