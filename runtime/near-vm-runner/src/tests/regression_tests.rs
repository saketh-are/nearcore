use crate::tests::test_builder::test_builder;
use expect_test::expect;

#[test]
fn memory_size_alignment_issue() {
    test_builder()
        .wat(
            r#"
              (module
                (type (;0;) (func))
                (func (;0;) (type 0)
                  memory.size
                  drop
                )
                (memory (;0;) 1 1024)
                (export "foo" (func 0))
              )
            "#,
        )
        .method("foo")
        .expects(&[
            expect![[r#"
                VMOutcome: balance 4 storage_usage 12 return data None burnt gas 90513208 used gas 90513208
            "#]],
        ]);
}

#[test]
fn slow_finite_wasm_gas_was_being_traced_and_thus_slow() {
    test_builder()
        .wat(
            r#"(module
              (type (func))
              (func (type 0)
                loop (result i64)
                  loop
                    br 0
                  end
                  i64.const 0
                end
                drop)
              (export "foo" (func 0)))
            "#,
        )
        .method("foo")
        .expects(&[
            expect![[r#"
              VMOutcome: balance 4 storage_usage 12 return data None burnt gas 100000000000000 used gas 100000000000000
              Err: Exceeded the prepaid gas.
            "#]],
        ]);
}

#[test]
fn gas_intrinsic_did_not_multiply_by_opcode_cost() {
    test_builder()
        .wat(
            r#"
              (module
                (type (func (param i32)))
                (type (func))
                (import "env" "gas" (func (type 0)))
                (func (type 1)
                  i32.const 500000000
                  call 0)
                (export "foo" (func 1)))
            "#,
        )
        .method("foo")
        .expects(&[
            expect![[r#"
            VMOutcome: balance 4 storage_usage 12 return data None burnt gas 100000000000000 used gas 100000000000000
            Err: Exceeded the maximum amount of gas allowed to burn per contract.
            "#]],
        ]);
}

#[test]
fn slow_test_parallel_runtime_invocations() {
    let mut join_handles = Vec::new();
    for _ in 0..128 {
        let handle = std::thread::spawn(|| {
            for _ in 0..16 {
                test_builder()
                .only_near_vm()
                .wat(
                    r#"
                      (module
                        (type (func))
                        (func (type 0)
                          loop
                            br 0
                          end
                        )
                        (export "foo" (func 0)))
                    "#,
                )
                .method("foo")
                .expects(&[
                    expect![[r#"
                    VMOutcome: balance 4 storage_usage 12 return data None burnt gas 100000000000000 used gas 100000000000000
                    Err: Exceeded the prepaid gas.
                    "#]],
                ]);
            }
        });
        join_handles.push(handle);
    }
    for handle in join_handles {
        handle.join().unwrap();
    }
}
