---
'@workflow/core': patch
'workflow': patch
---

Retained-VM boundaries now accept plain data and standard built-ins (`Map`, `Set`, `Date`, typed arrays, `ArrayBuffer`) as step inputs, verifying per boundary that serializing them executes no workflow code. Polyfills that add methods to built-in prototypes keep working.
