# Phase 2: Reliability & Data Integrity - Implementation Plan (Final)

## 1. Robust Parsing & Error Handling
- **Goal:** Replace all `.unwrap()` calls throughout the codebase with safe error handling using `AppError` and proper `Result` propagation.
- **Constraint:** Do NOT alter any existing logging/output formats or RESP error strings.
- **Tasks:**
    - Refactor `RespDecoder` and protocol parsing helpers.
    - Systematic audit and refactoring of `src/app/handlers/` and `src/app/rdb.rs` to replace `unwrap()` with idiomatic error handling.
- **CHECKPOINT:** Halt and alert user to run external test suite.

## 2. Comprehensive Expiration Logic (Data Integrity)
- **Goal:** Ensure expired keys are never returned or mutated, and are eventually evicted.
- **Tasks:**
    - **Lazy Expiration:** Standardize `Store` accessors to check and evict on-read/on-write.
    - **Active Expiration:** Implement a background task that periodically samples and deletes expired keys.
- **CHECKPOINT:** Halt and alert user to run external test suite.

## 3. Formalize Connection State Machine (Reliability)
- **Goal:** Isolate Transaction and Subscription logic using a State Pattern.
- **Tasks:**
    - Implement `NormalState`, `TransactionState`, and `SubscribedState`.
    - Delegate command routing and lifecycle events to the active state.
- **CHECKPOINT:** Halt and alert user to run external test suite.

## 4. Declarative Argument Parsing & Internal Error Mapping
- **Goal:** Eliminate boilerplate and ensure consistent (but preserved) error responses.
- **Constraint:** ALL `RespValue::Error` strings must remain byte-for-byte identical to the current implementation.
- **Tasks:**
    - Implement a declarative `ArgParser` for `ParsedCommand`.
    - Map internal errors to the exact existing error message strings.

## 5. Automated Propagation (Reliability)
- **Goal:** Prevent "missing propagation" bugs for write commands.
- **Tasks:**
    - Add metadata to the `Command` enum (e.g., `is_write()`).
    - Update the connection loop to automatically trigger propagation.
- **CHECKPOINT:** Halt and alert user to run external test suite.

## 6. Handler Testing Harness
- **Goal:** Enable fast, isolated unit testing for command logic.
- **Tasks:**
    - Create a utility to construct a mock `AppContext` for unit tests.

## Final Validation
- **CHECKPOINT:** Halt and alert user for a final full run of the external test suite.
