//! Helper functions and structures for the translation.
use crate::entity::entity_impl;
use core::u32;

/// Index type of a function defined locally inside the WebAssembly module.
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Debug,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[rkyv(derive(Copy, Clone, PartialEq, Eq))]
#[repr(transparent)]
pub struct LocalFunctionIndex(u32);
entity_impl!(LocalFunctionIndex);
entity_impl!(ArchivedLocalFunctionIndex);

/// Index type of a table defined locally inside the WebAssembly module.
#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct LocalTableIndex(u32);
entity_impl!(LocalTableIndex);

/// Index type of a memory defined locally inside the WebAssembly module.
#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug)]
pub struct LocalMemoryIndex(u32);
entity_impl!(LocalMemoryIndex);

/// Index type of a global defined locally inside the WebAssembly module.
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Debug,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[rkyv(derive(Copy, Clone, PartialEq, Eq))]
#[repr(transparent)]
pub struct LocalGlobalIndex(u32);
entity_impl!(LocalGlobalIndex);
entity_impl!(ArchivedLocalGlobalIndex);

/// Index type of a function (imported or local) inside the WebAssembly module.
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Debug,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[rkyv(derive(Copy, Clone, PartialOrd, Ord, PartialEq, Eq, Hash))]
#[repr(transparent)]
pub struct FunctionIndex(u32);
entity_impl!(FunctionIndex);
entity_impl!(ArchivedFunctionIndex);

/// Index type of a table (imported or local) inside the WebAssembly module.
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Debug,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[rkyv(derive(Copy, Clone, PartialEq, Eq))]
#[repr(transparent)]
pub struct TableIndex(u32);
entity_impl!(TableIndex);
entity_impl!(ArchivedTableIndex);

/// Index type of a global variable (imported or local) inside the WebAssembly module.
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Debug,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[rkyv(derive(Copy, Clone, PartialEq, Eq))]
#[repr(transparent)]
pub struct GlobalIndex(u32);
entity_impl!(GlobalIndex);
entity_impl!(ArchivedGlobalIndex);

/// Index type of a linear memory (imported or local) inside the WebAssembly module.
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Debug,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[rkyv(derive(Copy, Clone, PartialEq, Eq))]
#[repr(transparent)]
pub struct MemoryIndex(u32);
entity_impl!(MemoryIndex);
entity_impl!(ArchivedMemoryIndex);

/// Index type of a signature (imported or local) inside the WebAssembly module.
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Debug,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[rkyv(derive(Copy, Clone, PartialEq, Eq))]
#[repr(transparent)]
pub struct SignatureIndex(u32);
entity_impl!(SignatureIndex);
entity_impl!(ArchivedSignatureIndex);

/// Index type of a passive data segment inside the WebAssembly module.
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Debug,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[repr(transparent)]
#[rkyv(derive(PartialOrd, Ord, PartialEq, Eq))]
pub struct DataIndex(u32);
entity_impl!(DataIndex);

/// Index type of a passive element segment inside the WebAssembly module.
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Debug,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[repr(transparent)]
#[rkyv(derive(PartialOrd, Ord, PartialEq, Eq))]
pub struct ElemIndex(u32);
entity_impl!(ElemIndex);

/// Index type of a custom section inside a WebAssembly module.
#[derive(
    Copy,
    Clone,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    Debug,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[repr(transparent)]
pub struct CustomSectionIndex(u32);
entity_impl!(CustomSectionIndex);

/// An entity to export.
#[derive(
    Clone,
    Debug,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[repr(u8)]
pub enum ExportIndex {
    /// Function export.
    Function(FunctionIndex),
    /// Table export.
    Table(TableIndex),
    /// Memory export.
    Memory(MemoryIndex),
    /// Global export.
    Global(GlobalIndex),
}

/// An entity to import.
#[derive(
    Clone,
    Debug,
    Hash,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    rkyv::Serialize,
    rkyv::Deserialize,
    rkyv::Archive,
)]
#[repr(u8)]
pub enum ImportIndex {
    /// Function import.
    Function(FunctionIndex),
    /// Table import.
    Table(TableIndex),
    /// Memory import.
    Memory(MemoryIndex),
    /// Global import.
    Global(GlobalIndex),
}
