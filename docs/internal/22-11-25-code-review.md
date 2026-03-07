# Code Review & Architecture Decisions

## Architectural Decisions

### CLI Flags vs. Inference for Context
We have decided to enforce explicit CLI flags (`--storage-type` and `--workload-type`) rather than attempting to infer these attributes from existing settings or system stats.

**Reasoning:**
*   **Determinism:** Explicit flags ensure the analyzer runs against the *intended* state, not the potentially misconfigured *current* state.
*   **Safety:** Inferring "HDD" because `random_page_cost` is 4.0 creates a circular dependency where we validate bad config with bad config.
*   **Clarity:** It forces the user to think about their infrastructure layer before running the tool.

## Potential Future Improvements

### 1. Configurable Soft-Delete Columns
**Current State:** Hardcoded list (`is_deleted`, `deleted_at`, `archived`, `is_archived`).
**Proposal:** Move these definitions to a configuration file (e.g., `.postgreat.yaml`) to allow users to define domain-specific soft-delete patterns (e.g., `hidden_at`, `obsolete_flag`).

### 2. BRIN Index Threshold Tuning
**Current State:** 10MB threshold for BRIN candidacy.
**Proposal:** Increase the threshold significantly (e.g., to 1GB).
**Reasoning:** While 10MB is technically valid, BRIN indexes provide the most dramatic ROI on very large datasets. Raising the threshold will reduce "noise" for smaller tables where B-Trees are perfectly adequate and performant.
