//! Module that takes care of loading, checking and preprocessing of a
//! wasm module before execution.

use crate::logic::errors::PrepareError;
use near_parameters::vm::{Config, VMKind};

mod prepare_v2;
mod prepare_v3;

/// Loads the given module given in `original_code`, performs some checks on it and
/// does some preprocessing.
///
/// The checks are:
///
/// - module doesn't define an internal memory instance,
/// - imported memory (if any) doesn't reserve more memory than permitted by the `config`,
/// - all imported functions from the external environment matches defined by `env` module,
/// - functions number does not exceed limit specified in Config,
///
/// The preprocessing includes injecting code for gas metering and metering the height of stack.
pub fn prepare_contract(
    original_code: &[u8],
    config: &Config,
    kind: VMKind,
) -> Result<Vec<u8>, PrepareError> {
    let features = crate::features::WasmFeatures::new(config);
    if config.reftypes_bulk_memory {
        prepare_v3::prepare_contract(original_code, features, config, kind)
    } else {
        prepare_v2::prepare_contract(original_code, features, config, kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{test_vm_config, with_vm_variants};
    use assert_matches::assert_matches;

    fn parse_and_prepare_wat(
        config: &Config,
        vm_kind: VMKind,
        wat: &str,
    ) -> Result<Vec<u8>, PrepareError> {
        let wasm = wat::parse_str(wat).unwrap();
        prepare_contract(wasm.as_ref(), &config, vm_kind)
    }

    #[test]
    fn internal_memory_declaration() {
        with_vm_variants(|kind| {
            let config = test_vm_config(Some(kind));
            let r = parse_and_prepare_wat(&config, kind, r#"(module (memory 1 1))"#);
            assert_matches!(r, Ok(_));
        })
    }

    #[test]
    fn memory_imports() {
        with_vm_variants(|kind| {
            let config = test_vm_config(Some(kind));
            // This test assumes that maximum page number is configured to a certain number.
            assert_eq!(config.limit_config.max_memory_pages, 2048);

            let r = parse_and_prepare_wat(
                &config,
                kind,
                r#"(module (import "env" "memory" (memory 1 1)))"#,
            );
            assert_matches!(r, Err(PrepareError::Memory));

            // No memory import
            let r = parse_and_prepare_wat(&config, kind, r#"(module)"#);
            assert_matches!(r, Ok(_));

            // initial exceed maximum
            let r = parse_and_prepare_wat(
                &config,
                kind,
                r#"(module (import "env" "memory" (memory 17 1)))"#,
            );
            assert_matches!(r, Err(PrepareError::Deserialization));

            // no maximum
            let r = parse_and_prepare_wat(
                &config,
                kind,
                r#"(module (import "env" "memory" (memory 1)))"#,
            );
            assert_matches!(r, Err(PrepareError::Memory));

            // requested maximum exceed configured maximum
            let r = parse_and_prepare_wat(
                &config,
                kind,
                r#"(module (import "env" "memory" (memory 1 33)))"#,
            );
            assert_matches!(r, Err(PrepareError::Memory));
        })
    }

    #[test]
    fn multiple_valid_memory_are_disabled() {
        with_vm_variants(|kind| {
            let config = test_vm_config(Some(kind));
            // Our preparation and sanitization pass assumes a single memory, so we should fail when
            // there are multiple specified.
            let r = parse_and_prepare_wat(
                &config,
                kind,
                r#"(module
                    (import "env" "memory" (memory 1 2048))
                    (import "env" "memory" (memory 1 2048))
                )"#,
            );
            assert_matches!(r, Err(_));
            let r = parse_and_prepare_wat(
                &config,
                kind,
                r#"(module
                    (import "env" "memory" (memory 1 2048))
                    (memory 1)
                )"#,
            );
            assert_matches!(r, Err(_));
        })
    }

    #[test]
    fn imports() {
        with_vm_variants(|kind| {
            let config = test_vm_config(Some(kind));
            // nothing can be imported from non-"env" module for now.
            let r = parse_and_prepare_wat(
                &config,
                kind,
                r#"(module (import "another_module" "memory" (memory 1 1)))"#,
            );
            assert_matches!(r, Err(PrepareError::Instantiate));

            let r = parse_and_prepare_wat(
                &config,
                kind,
                r#"(module (import "env" "gas" (func (param i32))))"#,
            );
            assert_matches!(r, Ok(_));

            // TODO: Address tests once we check proper function signatures.
            /*
            // wrong signature
            let r = parse_and_prepare_wat(r#"(module (import "env" "gas" (func (param i64))))"#);
            assert_matches!(r, Err(Error::Instantiate));

            // unknown function name
            let r = parse_and_prepare_wat(r#"(module (import "env" "unknown_func" (func)))"#);
            assert_matches!(r, Err(Error::Instantiate));
            */
        })
    }
}
