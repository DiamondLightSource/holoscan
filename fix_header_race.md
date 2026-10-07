# Fix Header Race: Aligned STXM + Ptycho Transition Plan (Revised)

## Goal
Prevent the header/preemption race that can crash ptychography while keeping STXM and ptycho branches aligned across scan transitions.

## Decisions Locked In
- Header-preemption completion and end-of-scan completion are both treated as full completion.
- Branch alignment is strict: both branches resume from the same first post-transition batch.
- Data arriving during transition should be preserved where possible.
- Overflow during blocked transition is fail-fast.
- Overflow fail-fast behavior: ControlOp publishes a final error signal first, then raises.
- max_blocked_frames is config-driven (default policy value: 10 x batch_size).

## Why The Old Draft Was Not Sufficient
- It did not explicitly separate ptycho flush request from ptycho flush execution.
- It released the transition barrier too early.
- It did not define overflow behavior and signaling.
- It did not define a deterministic transition phase model.

## Transition State Model

### Shared state (thread-safe)
- **File:** pipeline/pipeline.py
- **Where:** StxmApp shared state initialization
- **Add:**
  - transition_blocked_event (threading.Event)
  - transition_phase: idle | waiting_quiesce | waiting_flush_exec
  - ptycho_accum_flushed (bool)
  - ptycho_recon_flushed (bool)
  - max_blocked_frames (config value)
  - transition_error (optional text for diagnostics)

**Reason**
- Multiple scheduler worker threads can read/write transition state concurrently.
- Event plus explicit phase prevents ambiguous behavior.

## Authoritative Transition Sequence

1. **Header is received** in pipeline/header_io.py
   - Validate header.
   - Stage pending_geometry.
   - Set preempt_requested.
   - Set transition_blocked_event.
   - Set transition_phase = waiting_quiesce.
   - Clear ptycho flush ack flags.
   - Emit header token to ControlOp.

2. **ControlOp receives header** in pipeline/control.py
   - Execute STXM-only flush.
   - Do not request ptycho flush here.
   - Keep transition blocked.

3. **Ptycho recon sees preempt_requested** in pipeline/ptychography_ops.py
   - Save partial result if needed.
   - Emit recon_complete.
   - Enter quiesced preemption path.

4. **ControlOp receives recon_complete while phase=waiting_quiesce**
   - Request ptycho flush now (request only):
     - call ptycho_accum.flush()
     - call ptycho_recon.flush()
   - Set transition_phase = waiting_flush_exec.

5. **Actual ptycho flush execution occurs later in operator compute**
   - Accumulator: _perform_flush executes at top of next compute if requested.
   - Recon: _perform_flush executes at top of next compute if requested.
   - Each sets its corresponding ack flag when _perform_flush has actually run.

6. **Barrier release condition**
   - Only release transition when BOTH are true:
     - ptycho_accum_flushed
     - ptycho_recon_flushed
   - Then:
     - clear transition_blocked_event
     - set transition_phase = idle
     - reset ack flags for next transition

This is the key timing rule:
- Ptycho flush is requested in ControlOp on recon_complete during waiting_quiesce.
- Ptycho flush is executed inside ptycho operators in _perform_flush during their next compute tick.
- Barrier is released only after both executions are acknowledged.

## Concrete Code Changes

### 1) Shared transition primitives
- **File:** pipeline/pipeline.py
- **Where:** StxmApp.__init__ and compose wiring
- **Change:**
  - Add thread-safe transition fields to shared state.
  - Pass shared state into GatherOp and ControlOp.

### 2) Gather alignment gate and overflow accounting
- **File:** pipeline/data_io.py
- **Where:** GatherOp.__init__, setup, compute
- **Change:**
  - Store shared transition state reference.
  - Before emit, block when transition_blocked_event is set.
  - Keep matched data cached while blocked.
  - Track blocked cached frame count.
  - Load max_blocked_frames from config/state.

### 3) Gather fail-fast path
- **File:** pipeline/data_io.py and pipeline/control.py
- **Where:** GatherOp.compute overflow branch + ControlOp input handling
- **Change:**
  - When blocked cache exceeds max_blocked_frames:
    - set transition_error in shared state
    - emit control message, e.g. transition_overflow
  - ControlOp handles transition_overflow by:
    - publishing final error signal
    - raising RuntimeError

### 4) Header starts transition cleanly
- **File:** pipeline/header_io.py
- **Where:** after pending geometry staging and preempt_requested
- **Change:**
  - set transition_blocked_event
  - set transition_phase=waiting_quiesce
  - clear prior ack flags
  - keep current scan_state geometry update behavior

### 5) Split ControlOp flush ownership
- **File:** pipeline/control.py
- **Where:** constructor + helpers
- **Change:**
  - Replace single flushable_ops with stxm_flush_ops and ptycho_flush_ops.
  - Add helpers:
    - do_stxm_flush()
    - request_ptycho_flush()
    - do_full_flush() (for non-transition full completion path)

### 6) Control header branch
- **File:** pipeline/control.py
- **Where:** msg == header
- **Change:**
  - STXM-only flush.
  - Never request ptycho flush in this branch.

### 7) Control recon_complete branch (phase-aware)
- **File:** pipeline/control.py
- **Where:** msg == recon_complete
- **Change:**
  - If transition_phase == waiting_quiesce:
    - request ptycho flush
    - set waiting_flush_exec
    - do not release barrier
  - Else:
    - existing full-completion behavior

### 8) Control flush/start branch hardening
- **File:** pipeline/control.py
- **Where:** msg == flush
- **Change:**
  - If transition blocked, do not request ptycho flush from this path.
  - Use STXM-only or no-op policy to avoid race reintroduction.

### 9) Ptycho flush ack on execution
- **File:** pipeline/ptychography_ops.py
- **Where:**
  - PtychoAccumulatorOp._perform_flush
  - PtychoReconstructionOp._perform_flush
- **Change:**
  - Set ack flags when each _perform_flush has actually executed.

### 10) Barrier release at safe point only
- **File:** pipeline/ptychography_ops.py or pipeline/control.py (single owner chosen)
- **Where:** after both ack flags observed true
- **Change:**
  - Release transition barrier only when both ptycho flush executions are confirmed.
  - Do not release solely at end of _apply_pending_geometry.

### 11) Optional extra preemption guard
- **File:** pipeline/ptychography_ops.py
- **Where:** immediately before reconstruction_data/combine launch
- **Change:**
  - Add second preempt check to reduce chance of one extra iteration starting.

## Config Changes
- **File:** pipeline/config_test.yaml and pipeline/config_prod.yaml
- **Add under scheduler or a new transition section:**
  - max_blocked_frames: integer
- **Default policy suggestion:**
  - max_blocked_frames = 10 x image_src.batch_size (computed if unset)

## Logging and Observability
- **File:** pipeline/header_io.py
  - Log transition start, phase, and header id/shape.
- **File:** pipeline/control.py
  - Log phase transitions.
  - Log ptycho flush request moment.
  - Log final error signal publication before raise.
- **File:** pipeline/ptychography_ops.py
  - Log each actual _perform_flush execution and ack set.
  - Log barrier release with both ack flags.
- **File:** pipeline/data_io.py
  - Log blocked-cache growth and threshold crossing.

## Expected Outcome
- No ptycho mid-iteration invalid-state reset from header/start control paths.
- Deterministic and explicit timing for ptycho flush request vs execution.
- Strict STXM/ptycho alignment through transition barrier.
- Bounded blocked buffering with explicit fail-fast and error signaling.

## Notes And Remaining Non-goals
- Multiple rapid header bursts are still out of scope for this pass.
- Best-effort data preservation is targeted, but overflow path intentionally stops the run.

## PR-sized Implementation Plan

### PR 1: Control flush split (refactor, behavior-preserving)
- **Purpose:** Separate STXM vs ptycho flush ownership in ControlOp with minimal behavior change.
- **Includes:**
  - Split ControlOp operator groups into `stxm_flush_ops` and `ptycho_flush_ops`.
  - Add helper methods for STXM-only flush, ptycho-only flush request, and full flush.
  - Update compose wiring to pass split groups.
- **Files:**
  - pipeline/control.py
  - pipeline/pipeline.py
- **Verification:**
  - Existing single-scan behavior remains unchanged.
  - Existing flush topics still publish as before.

### PR 2: Transition state primitives + header phase start
- **Purpose:** Introduce thread-safe transition state and start transition on header.
- **Includes:**
  - Add transition fields in shared state (`transition_blocked_event`, `transition_phase`, ack flags, `transition_error`).
  - Add `max_blocked_frames` state value from config/default policy.
  - Header path sets `waiting_quiesce` and blocks transition on valid header when ptycho is enabled.
- **Files:**
  - pipeline/pipeline.py
  - pipeline/header_io.py
- **Verification:**
  - Header during active recon sets blocked event and phase to `waiting_quiesce`.
  - No data-path behavior change yet.

### PR 3: Phase-aware ControlOp handling
- **Purpose:** Make header and recon_complete handling deterministic by phase.
- **Includes:**
  - Header branch performs STXM-only flush and never requests ptycho flush.
  - recon_complete branch:
    - if `waiting_quiesce`: request ptycho flush and move to `waiting_flush_exec`
    - else: retain existing full-completion behavior
  - flush/start branch hardening while transition is blocked.
- **Files:**
  - pipeline/control.py
- **Verification:**
  - Header no longer triggers immediate ptycho flush request.
  - recon_complete in preemption path triggers ptycho flush request exactly once.

### PR 4: Ptycho flush execution ack + safe barrier release
- **Purpose:** Release transition only after ptycho flush has actually executed.
- **Includes:**
  - Set `ptycho_accum_flushed` in accumulator `_perform_flush`.
  - Set `ptycho_recon_flushed` in recon `_perform_flush`.
  - Release blocked event only when both ack flags are true.
  - Reset phase to `idle` and clear acks for next transition.
- **Files:**
  - pipeline/ptychography_ops.py
  - pipeline/control.py (if release ownership is centralized)
- **Verification:**
  - Logs clearly show: request -> execution ack -> barrier release.
  - No early release before both ptycho flush executions.

### PR 5: Gather alignment gate + fail-fast overflow signaling
- **Purpose:** Enforce strict branch alignment and bounded blocked buffering.
- **Includes:**
  - Pass shared state into GatherOp.
  - Block Gather emit while transition is blocked; keep matched data cached.
  - Track blocked cached frame count.
  - Overflow path (`> max_blocked_frames`):
    - Gather emits `transition_overflow` control message and sets transition error context.
    - ControlOp publishes final error signal first, then raises RuntimeError.
- **Files:**
  - pipeline/data_io.py
  - pipeline/control.py
  - pipeline/pipeline.py
  - pipeline/config_test.yaml
  - pipeline/config_prod.yaml
- **Verification:**
  - Normal transition preserves and resumes aligned batches.
  - Forced overflow path publishes final error signal before raise.

### PR 6: Hardening and observability
- **Purpose:** Improve resilience and diagnosability.
- **Includes:**
  - Optional second preemption check just before PIE iteration launch.
  - Log phase transitions, flush requests, flush execution acks, and barrier release.
  - Tighten comments to document state machine semantics.
- **Files:**
  - pipeline/ptychography_ops.py
  - pipeline/control.py
  - pipeline/header_io.py
  - pipeline/data_io.py
- **Verification:**
  - Repeated header-during-recon runs show deterministic ordering.
  - Logs are sufficient to reconstruct transition timeline end-to-end.

## Suggested Merge Order
1. PR 1
2. PR 2
3. PR 3
4. PR 4
5. PR 5
6. PR 6

This order minimizes risk by landing structure first, then phase logic, then barrier correctness, then buffered alignment/fail-fast, then hardening.
