;; ABI v1: JSON event in, packed (pointer << 32 | length) JSON action out.
;; Fresh instance per hook; no WASI or host imports. Adds a header for demo only.
(module
  (memory (export "memory") 1)
  (data (i32.const 1024) "{\22set_headers\22:{\22x-prost-plugin\22:\22wasm\22}}")
  (func (export "prost_abi_version") (result i32) i32.const 1)
  (func (export "alloc") (param i32) (result i32) i32.const 4096)
  (func (export "handle") (param i32 i32) (result i64)
    i64.const 4398046511145)
)
