use crate::logic::errors::PrepareError;
use finite_wasm_6::{Fee, wasmparser as wp};
use near_parameters::vm::{Config, VMKind};
use wasm_encoder::{Encode, Section, SectionId};

struct PrepareContext<'a> {
    code: &'a [u8],
    config: &'a Config,
    output_code: Vec<u8>,
    function_limit: u64,
    local_limit: u64,
    validator: wp::Validator,
    func_validator_allocations: wp::FuncValidatorAllocations,
    before_import_section: bool,
}

impl<'a> PrepareContext<'a> {
    fn new(code: &'a [u8], features: crate::features::WasmFeatures, config: &'a Config) -> Self {
        let limits = &config.limit_config;
        Self {
            code,
            config,
            output_code: Vec::with_capacity(code.len()),
            // Practically reaching u64::MAX locals or functions is infeasible, so when the limit is not
            // specified, use that as a limit.
            function_limit: limits.max_functions_number_per_contract.unwrap_or(u64::MAX),
            local_limit: limits.max_locals_per_contract.unwrap_or(u64::MAX),
            validator: wp::Validator::new_with_features(features.into()),
            func_validator_allocations: wp::FuncValidatorAllocations::default(),
            before_import_section: true,
        }
    }

    /// “Early” preparation.
    ///
    /// Must happen before the finite-wasm analysis and is applicable to NearVm just as much as it is
    /// applicable to other runtimes.
    ///
    /// This will validate the module, normalize the memories within, apply limits.
    fn run(&mut self) -> Result<Vec<u8>, PrepareError> {
        self.before_import_section = true;
        let parser = wp::Parser::new(0);
        for payload in parser.parse_all(self.code) {
            let payload = payload.map_err(|err| {
                tracing::trace!(?err, "was not able to early prepare the input module");
                PrepareError::Deserialization
            })?;
            match payload {
                wp::Payload::Version { num, encoding, range } => {
                    self.copy(range.clone())?;
                    self.validator
                        .version(num, encoding, &range)
                        .map_err(|_| PrepareError::Deserialization)?;
                }
                wp::Payload::End(offset) => {
                    self.validator.end(offset).map_err(|_| PrepareError::Deserialization)?;
                }

                wp::Payload::TypeSection(reader) => {
                    self.validator
                        .type_section(&reader)
                        .map_err(|_| PrepareError::Deserialization)?;
                    self.copy_section(SectionId::Type, reader.range())?;
                }

                wp::Payload::ImportSection(reader) => {
                    self.before_import_section = false;
                    self.validator
                        .import_section(&reader)
                        .map_err(|_| PrepareError::Deserialization)?;
                    self.transform_import_section(&reader)?;
                }

                wp::Payload::FunctionSection(reader) => {
                    self.ensure_import_section();
                    self.validator
                        .function_section(&reader)
                        .map_err(|_| PrepareError::Deserialization)?;
                    self.copy_section(SectionId::Function, reader.range())?;
                }
                wp::Payload::TableSection(reader) => {
                    self.ensure_import_section();
                    self.validator
                        .table_section(&reader)
                        .map_err(|_| PrepareError::Deserialization)?;
                    self.copy_section(SectionId::Table, reader.range())?;
                }
                wp::Payload::MemorySection(reader) => {
                    // We do not want to include the implicit memory anymore as we normalized it by
                    // importing the memory instead.
                    self.ensure_import_section();
                    self.validator
                        .memory_section(&reader)
                        .map_err(|_| PrepareError::Deserialization)?;
                }
                wp::Payload::GlobalSection(reader) => {
                    self.ensure_import_section();
                    self.validator
                        .global_section(&reader)
                        .map_err(|_| PrepareError::Deserialization)?;
                    self.copy_section(SectionId::Global, reader.range())?;
                }
                wp::Payload::ExportSection(reader) => {
                    self.ensure_import_section();
                    self.validator
                        .export_section(&reader)
                        .map_err(|_| PrepareError::Deserialization)?;
                    self.copy_section(SectionId::Export, reader.range())?;
                }
                wp::Payload::StartSection { func, range } => {
                    self.ensure_import_section();
                    self.validator
                        .start_section(func, &range)
                        .map_err(|_| PrepareError::Deserialization)?;
                    self.copy_section(SectionId::Start, range.clone())?;
                }
                wp::Payload::ElementSection(reader) => {
                    self.ensure_import_section();
                    self.validator
                        .element_section(&reader)
                        .map_err(|_| PrepareError::Deserialization)?;
                    self.copy_section(SectionId::Element, reader.range())?;
                }
                wp::Payload::DataCountSection { count, range } => {
                    self.ensure_import_section();
                    self.validator
                        .data_count_section(count, &range)
                        .map_err(|_| PrepareError::Deserialization)?;
                    self.copy_section(SectionId::DataCount, range.clone())?;
                }
                wp::Payload::DataSection(reader) => {
                    self.ensure_import_section();
                    self.validator
                        .data_section(&reader)
                        .map_err(|_| PrepareError::Deserialization)?;
                    self.copy_section(SectionId::Data, reader.range())?;
                }
                wp::Payload::CodeSectionStart { size: _, count, range } => {
                    self.ensure_import_section();
                    self.function_limit = self
                        .function_limit
                        .checked_sub(u64::from(count))
                        .ok_or(PrepareError::TooManyFunctions)?;
                    self.validator
                        .code_section_start(count, &range)
                        .map_err(|_| PrepareError::Deserialization)?;
                    self.copy_section(SectionId::Code, range.clone())?;
                }
                wp::Payload::CodeSectionEntry(func) => {
                    let local_reader =
                        func.get_locals_reader().map_err(|_| PrepareError::Deserialization)?;
                    for local in local_reader {
                        let (count, _ty) = local.map_err(|_| PrepareError::Deserialization)?;
                        self.local_limit = self
                            .local_limit
                            .checked_sub(u64::from(count))
                            .ok_or(PrepareError::TooManyLocals)?;
                    }

                    let func_validator = self
                        .validator
                        .code_section_entry(&func)
                        .map_err(|_| PrepareError::Deserialization)?;
                    // PANIC-SAFETY: no big deal if we panic here while the allocations are taken.
                    // Worst-case we are going to be making new allocations again, but in practice
                    // this should never happen as this context should not be reused.
                    let allocs = std::mem::replace(
                        &mut self.func_validator_allocations,
                        wp::FuncValidatorAllocations::default(),
                    );
                    let mut func_validator = func_validator.into_validator(allocs);
                    func_validator.validate(&func).map_err(|_| PrepareError::Deserialization)?;
                    self.func_validator_allocations = func_validator.into_allocations();
                }
                wp::Payload::CustomSection(reader) => {
                    if !self.config.discard_custom_sections {
                        self.ensure_import_section();
                        self.copy_section(SectionId::Custom, reader.range())?;
                    }
                }

                // Extensions not supported.
                wp::Payload::UnknownSection { .. }
                | wp::Payload::TagSection(_)
                | wp::Payload::ModuleSection { .. }
                | wp::Payload::InstanceSection(_)
                | wp::Payload::CoreTypeSection(_)
                | wp::Payload::ComponentSection { .. }
                | wp::Payload::ComponentInstanceSection(_)
                | wp::Payload::ComponentAliasSection(_)
                | wp::Payload::ComponentTypeSection(_)
                | wp::Payload::ComponentCanonicalSection(_)
                | wp::Payload::ComponentStartSection { .. }
                | wp::Payload::ComponentImportSection(_)
                | wp::Payload::ComponentExportSection(_)
                | _ => {
                    tracing::trace!("input module contains unsupported section");
                    return Err(PrepareError::Deserialization);
                }
            }
        }
        Ok(std::mem::replace(&mut self.output_code, Vec::new()))
    }

    fn transform_import_section(
        &mut self,
        reader: &wp::ImportSectionReader,
    ) -> Result<(), PrepareError> {
        let mut new_section = wasm_encoder::ImportSection::new();
        for import in reader.clone() {
            let import = import.map_err(|_| PrepareError::Deserialization)?;
            if import.module != "env" {
                return Err(PrepareError::Instantiate);
            }
            let new_type = match import.ty {
                wp::TypeRef::Func(id) => {
                    // TODO: validate imported function types here.
                    self.function_limit =
                        self.function_limit.checked_sub(1).ok_or(PrepareError::TooManyFunctions)?;
                    wasm_encoder::EntityType::Function(id)
                }
                wp::TypeRef::Table(_) => return Err(PrepareError::Instantiate),
                wp::TypeRef::Global(_) => return Err(PrepareError::Instantiate),
                wp::TypeRef::Memory(_) => return Err(PrepareError::Memory),
                wp::TypeRef::Tag(_) => return Err(PrepareError::Deserialization),
            };
            new_section.import(import.module, import.name, new_type);
        }
        new_section.import("env", "memory", self.memory_import());
        // wasm_encoder a section with all imports and the imported standardized memory.
        new_section.append_to(&mut self.output_code);
        Ok(())
    }

    fn ensure_import_section(&mut self) {
        if self.before_import_section {
            self.before_import_section = false;
            let mut new_section = wasm_encoder::ImportSection::new();
            new_section.import("env", "memory", self.memory_import());
            // wasm_encoder a section with all imports and the imported standardized memory.
            new_section.append_to(&mut self.output_code);
        }
    }

    fn memory_import(&self) -> wasm_encoder::EntityType {
        wasm_encoder::EntityType::Memory(wasm_encoder::MemoryType {
            minimum: u64::from(self.config.limit_config.initial_memory_pages),
            maximum: Some(u64::from(self.config.limit_config.max_memory_pages)),
            memory64: false,
            shared: false,
            page_size_log2: None,
        })
    }

    fn copy_section(
        &mut self,
        id: SectionId,
        range: std::ops::Range<usize>,
    ) -> Result<(), PrepareError> {
        id.encode(&mut self.output_code);
        range.len().encode(&mut self.output_code);
        self.copy(range)
    }

    /// Copy over the payload to the output binary without significant processing.
    fn copy(&mut self, range: std::ops::Range<usize>) -> Result<(), PrepareError> {
        Ok(self.output_code.extend(self.code.get(range).ok_or(PrepareError::Deserialization)?))
    }
}

pub(crate) fn prepare_contract(
    original_code: &[u8],
    features: crate::features::WasmFeatures,
    config: &Config,
    kind: VMKind,
) -> Result<Vec<u8>, PrepareError> {
    let lightly_steamed = PrepareContext::new(original_code, features, config).run()?;

    if kind == VMKind::NearVm {
        // Built-in near-vm code instruments code for itself.
        return Ok(lightly_steamed);
    }

    let res = finite_wasm_6::Analysis::new()
        .with_stack(SimpleMaxStackCfg)
        .with_gas(SimpleGasCostCfg(u64::from(config.regular_op_cost)))
        .analyze(&lightly_steamed)
        .map_err(|err| {
            tracing::error!(?err, ?kind, "Analysis failed");
            PrepareError::Deserialization
        })?
        // Make sure contracts can’t call the instrumentation functions via `env`.
        .instrument("internal", &lightly_steamed)
        .map_err(|err| {
            tracing::error!(?err, ?kind, "Instrumentation failed");
            PrepareError::Serialization
        })?;
    Ok(res)
}

// TODO: refactor to avoid copy-paste with the ones currently defined in near_vm_runner
struct SimpleMaxStackCfg;

impl finite_wasm_6::max_stack::SizeConfig for SimpleMaxStackCfg {
    fn size_of_value(&self, ty: wp::ValType) -> u8 {
        use wp::ValType;
        match ty {
            ValType::I32 => 4,
            ValType::I64 => 8,
            ValType::F32 => 4,
            ValType::F64 => 8,
            ValType::V128 => 16,
            ValType::Ref(_) => 8,
        }
    }
    fn size_of_function_activation(
        &self,
        locals: &prefix_sum_vec::PrefixSumVec<wp::ValType, u32>,
    ) -> u64 {
        let mut res = 64_u64; // Rough accounting for rip, rbp and some registers spilled. Not exact.
        let mut last_idx_plus_one = 0_u64;
        for (idx, local) in locals {
            let idx = u64::from(*idx);
            res = res.saturating_add(
                idx.checked_sub(last_idx_plus_one)
                    .expect("prefix-sum-vec indices went backwards")
                    .saturating_add(1)
                    .saturating_mul(u64::from(self.size_of_value(*local))),
            );
            last_idx_plus_one = idx.saturating_add(1);
        }
        res
    }
}

struct SimpleGasCostCfg(u64);

macro_rules! gas_cost {
    ($( @$proposal:ident $op:ident $({ $($arg:ident: $argty:ty),* })? => $visit:ident ($($ann:tt)*))*) => {
        $(
            fn $visit(&mut self $($(, $arg: $argty)*)?) -> Fee {
                gas_cost!(@@self $visit)
            }
        )*
    };

    (@@$self:ident visit_block) => { Fee::ZERO };
    (@@$self:ident visit_end) => { Fee::ZERO };
    (@@$self:ident visit_else) => { Fee::ZERO };
    (@@$self:ident visit_memory_init) => { Fee { linear: $self.0, constant: $self.0 } };
    (@@$self:ident visit_memory_copy) => { Fee { linear: $self.0, constant: $self.0 } };
    (@@$self:ident visit_memory_fill) => { Fee { linear: $self.0, constant: $self.0 } };
    (@@$self:ident visit_table_init) => { Fee { linear: $self.0, constant: $self.0 } };
    (@@$self:ident visit_table_copy) => { Fee { linear: $self.0, constant: $self.0 } };
    (@@$self:ident visit_table_fill) => { Fee { linear: $self.0, constant: $self.0 } };
    (@@$self:ident $visit:ident) => { Fee::constant($self.0) };
}

impl<'a> wp::VisitOperator<'a> for SimpleGasCostCfg {
    type Output = Fee;
    wp::for_each_visit_operator!(gas_cost);
}

impl<'a> wp::VisitSimdOperator<'a> for SimpleGasCostCfg {
    wp::for_each_visit_simd_operator!(gas_cost);
}

#[cfg(test)]
mod test {
    use super::VMKind;
    use crate::logic::errors::PrepareError;
    use crate::tests::test_vm_config;
    use finite_wasm_6::wasmparser as wp;

    fn wasmparser_decode(
        code: &[u8],
        features: crate::features::WasmFeatures,
    ) -> Result<(Option<u64>, Option<u64>), wp::BinaryReaderError> {
        use wp::ValidPayload;
        let mut validator = wp::Validator::new_with_features(features.into());
        let mut function_count = Some(0u64);
        let mut local_count = Some(0u64);
        for payload in wp::Parser::new(0).parse_all(code) {
            let payload = payload?;

            // The validator does not output `ValidPayload::Func` for imported functions.
            if let wp::Payload::ImportSection(ref import_section_reader) = payload {
                for import_section in import_section_reader.clone() {
                    match import_section?.ty {
                        wp::TypeRef::Func(_) => {
                            function_count = function_count.and_then(|f| f.checked_add(1))
                        }
                        wp::TypeRef::Table(_)
                        | wp::TypeRef::Memory(_)
                        | wp::TypeRef::Global(_)
                        | wp::TypeRef::Tag(_) => {}
                    }
                }
            }

            match validator.payload(&payload)? {
                ValidPayload::Ok => (),
                ValidPayload::Func(validator, body) => {
                    validator.into_validator(Default::default()).validate(&body)?;
                    function_count = function_count.and_then(|f| f.checked_add(1));
                    // Count the global number of local variables.
                    let mut local_reader = body.get_locals_reader()?;
                    for _ in 0..local_reader.get_count() {
                        let (count, _type) = local_reader.read()?;
                        local_count = local_count.and_then(|l| l.checked_add(count.into()));
                    }
                }
                ValidPayload::Parser(_) => {
                    panic!("submodules not supported and should've been rejected")
                }
                ValidPayload::End(_) => {}
            }
        }
        Ok((function_count, local_count))
    }

    pub(crate) fn validate_contract(
        code: &[u8],
        features: crate::features::WasmFeatures,
        config: &near_parameters::vm::Config,
    ) -> Result<(), PrepareError> {
        let (function_count, local_count) = wasmparser_decode(code, features).map_err(|e| {
            tracing::debug!(err=?e, "wasmparser failed decoding a contract");
            PrepareError::Deserialization
        })?;
        // Verify the number of functions does not exceed the limit we imposed. Note that the ordering
        // of this check is important. In the past we first validated the entire module and only then
        // verified that the limit is not exceeded. While it would be more efficient to check for this
        // before validating the function bodies, it would change the results for malformed WebAssembly
        // modules.
        if let Some(max_functions) = config.limit_config.max_functions_number_per_contract {
            if function_count.ok_or(PrepareError::TooManyFunctions)? > max_functions {
                return Err(PrepareError::TooManyFunctions);
            }
        }
        // Similarly, do the same for the number of locals.
        if let Some(max_locals) = config.limit_config.max_locals_per_contract {
            if local_count.ok_or(PrepareError::TooManyLocals)? > max_locals {
                return Err(PrepareError::TooManyLocals);
            }
        }
        Ok(())
    }

    #[test]
    fn v3_preparation_wasmtime_generates_valid_contract_fuzzer() {
        let config = test_vm_config(Some(VMKind::Wasmtime));
        let features = crate::features::WasmFeatures::new(&config);
        bolero::check!().for_each(|input: &[u8]| {
            // DO NOT use ArbitraryModule. We do want modules that may be invalid here, if they
            // pass our validation step!
            if let Ok(_) = validate_contract(input, features, &config) {
                match super::prepare_contract(input, features, &config, VMKind::Wasmtime) {
                    Err(_e) => (), // TODO: this should be a panic, but for now it’d actually trigger
                    Ok(code) => {
                        let mut validator = wp::Validator::new_with_features(features.into());
                        match validator.validate_all(&code) {
                            Ok(_) => (),
                            Err(e) => panic!(
                                "prepared code failed validation: {e:?}\ncontract: {}",
                                hex::encode(input),
                            ),
                        }
                    }
                }
            }
        });
    }

    #[test]
    fn v3_preparation_near_vm_generates_valid_contract_fuzzer() {
        let config = test_vm_config(Some(VMKind::NearVm2));
        let features = crate::features::WasmFeatures::new(&config);
        bolero::check!().for_each(|input: &[u8]| {
            // DO NOT use ArbitraryModule. We do want modules that may be invalid here, if they
            // pass our validation step!
            if let Ok(_) = validate_contract(input, features, &config) {
                match super::prepare_contract(input, features, &config, VMKind::NearVm2) {
                    Err(_e) => (), // TODO: this should be a panic, but for now it’d actually trigger
                    Ok(code) => {
                        let mut validator = wp::Validator::new_with_features(features.into());
                        match validator.validate_all(&code) {
                            Ok(_) => (),
                            Err(e) => panic!(
                                "prepared code failed validation: {e:?}\ncontract: {}",
                                hex::encode(input),
                            ),
                        }
                    }
                }
            }
        });
    }
}
