---
'@workflow/core': patch
'workflow': patch
---

Retained-VM boundaries now accept plain data and standard built-ins (`Map`, `Set`, `Date`, `Error`, typed arrays, `URL`, `Headers`, …) as step inputs. Serialization reads through captured intrinsics and reports when it had to execute workflow code (getters, proxies, custom serializers); only those boundaries fall back to ordinary replay.
