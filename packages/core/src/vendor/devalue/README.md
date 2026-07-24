# Vendored devalue

This is [devalue](https://github.com/sveltejs/devalue) `5.8.1` (MIT, see
LICENSE) with the pluggable stringify `operations` interface from
[sveltejs/devalue#172](https://github.com/sveltejs/devalue/pull/172) applied
on top, reconciled against the 5.8.1 source (the PR is based on a newer
tree; the merged typed-array/DataView case in `stringify.ts` was ported by
hand to preserve 5.8.1 behavior exactly).

It is vendored — rather than consumed from npm — because the `operations`
option is what lets `serialization/operations.ts` harden serialization and
report passivity taint for retained-VM replay. A stock devalue silently
ignores the option, so shipping a normal npm dependency would silently
disable the hardening for package consumers. See
`serialization/byte-stability.test.ts` for the corpus test proving this copy
is byte-compatible with stock devalue output for well-behaved values.

Files are the upstream JS renamed to `.ts` with a leading `// @ts-nocheck`
(they are JSDoc-typed and compile as-is), plus `declare` property
declarations on `DevalueError` (constructor-assignment inference is a
JS-file-only TypeScript feature). Do not edit them beyond that; apply
upstream diffs instead. If/when upstream merges and releases the
operations interface, this directory should be deleted in favor of the npm
dependency.
