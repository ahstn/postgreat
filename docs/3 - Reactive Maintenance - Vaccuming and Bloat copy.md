# Moved: Reactive Maintenance - Vacuuming and Bloat

This file was an older duplicate of the vacuuming and bloat guide. It has been replaced by the canonical, version-checked guide:

- [3 - Reactive Maintenance - Vaccuming and Bloat.md](./3%20-%20Reactive%20Maintenance%20-%20Vaccuming%20and%20Bloat.md)

Why it was replaced:

- Its "Bloat Hit List" query did not run. It used `schemaname | '.' | relname`, where `|` is not string concatenation (`||` is).
- Its lock table said plain `REINDEX` takes an `ACCESS EXCLUSIVE` lock on the table. It takes a `SHARE` lock on the table and an `ACCESS EXCLUSIVE` lock on the index. The practical effect is still close to blocking the table, because planning a query locks every index.
- It said plain `VACUUM` never returns space to the operating system. It can, by truncating empty pages at the end of the table.
- It had unlinked citation numbers (such as `42`, `47`, `62`) with no reference list.

The useful parts (lock table, `CLUSTER`, and the decision framework) were corrected and merged into the canonical guide. Every claim from this file is still listed in the audit as `DUP-*` rows in [`docs/audit/claims.md`](./audit/claims.md).
