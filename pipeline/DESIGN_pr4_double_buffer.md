# Design — PR4 double-buffering (projection ping-pong)

Branch `feat/tomo-pr4-loadtest`. Motivated by moving to a **much faster detector**;
the single-buffer design measured sufficient at 3 kHz on the sim but with shrinking
margin as frame rate / frame size grow.

## Why (measured, 2026-07-02)

Load test on daqsim (scan409907, 2 projections, A400). Instrumentation tagged
`PR4-MEASURE` (`data_io.py` GatherOp cache HWM; `ptychography_ops.py` finalize-window
timing + `PR4_ACCUM_QUEUE_CAP` / `PR4_FINALIZE_DELAY_MS` knobs).

- **Run A (baseline, 500 fps, queue=128):** finalize window **~490 ms** (≈ one PIE
  iteration + HDF save). GatherOp cache HWM **256 frames**. Both projections exact.
- **Run B (queue=8, ~2 s injected finalize):** GatherOp cache HWM **336 frames**,
  both projections exact, **no drops, no deadlock abort** at 4× the 500 ms timeout.

**Conclusions:**
1. The current single-buffer design degrades **gracefully** — backpressure is lossless,
   and the `stop_on_deadlock` (500 ms) tripwire does **not** fire during a long finalize
   because the recon thread is busy (counts as progress). So PR4 does **not** need to
   touch the deadlock timeout.
2. Zero loss on the sim is a **sim artifact**: daqsim streams images (PUSH) and positions
   from one loop, so pipeline backpressure on the PUSH socket stalls the whole sim,
   incidentally pausing positions. **A real detector + PandA are independent free-running
   sources** — backpressure during the finalize window can't throttle them, so frames/
   positions land in a full socket and drop (positions especially: PUB/SUB, no flow
   control, no CONFLATE, ~1000 RCVHWM).
3. So the single-buffer failure mode against a real fast detector is **source throttling /
   silent position loss during the ~1-iteration finalize window**, not a crash. Backlog
   ≈ `acq_rate × finalize_window`; the finalize window grows with object/frame size.

Double-buffering removes the finalize-window backpressure: the accumulator never stops
draining, so a free-running detector is never throttled and positions never back up.

## How single-buffer works today (baseline)

One shared GPU buffer set in `ptycho_state`: `raw_gpu (capacity,H,W)`, `positions_full`,
`tilts_full`, one `filled_until` counter. The PtyREX model (`pty_model.scan.positions`,
`pty_data.raw_expanded`) holds **views** into these buffers.

- `PtychoAccumulatorOp` writes batches into `raw_gpu[filled:new_end]`, advances
  `filled_until`. On a projection boundary it splits the straddling batch (head fills the
  projection, tail → `self._carry`).
- Once `filled_until >= no_frames` (tomography), the accumulator **backpressures**:
  `compute` returns before `receive()` (`ptychography_ops.py:147`). Incoming batches wait
  in the `gather → accum` `DOUBLE_BUFFER` queue (capacity 128 ≈ 8k frames).
- `PtychoReconstructionOp` runs the final iteration on the full buffer, saves the
  per-projection HDF (~490 ms), emits `projection_complete`.
- `ControlOp` → `accum.advance_projection()` → next accum tick resets `filled_until=0`,
  writes the carry, resumes draining. Recon observes `filled < no_frames`, resets object
  (probe carried) for the next projection.

The gap: for the whole finalize window the accumulator is **not draining**, so the source
is backpressured. That is what double-buffering eliminates.

## Double-buffer design (2-buffer ping-pong)

### State (`ptycho_state`, allocated once at `capacity` per R-6 — now ×2)
```
raw_gpu:        [bufA, bufB]        # two (capacity,H,W) arrays
positions_full: [posA, posB]
tilts_full:     [tiltA, tiltB]
filled_until:   [fillA, fillB]     # per-buffer fill level (under lock)
write_idx:      0                  # buffer the accumulator writes  (accum owns)
read_idx:       0                  # buffer the recon reads         (recon owns)
buffer_ready:   [Event, Event]     # buffer i full & handed to recon
buffer_free:    [Event, Event]     # buffer i free for the accumulator (both set at init)
```
GPU cost: 2× `raw_gpu` (~64 MB each at 1024×128×128×f32 → ~128 MB total). Fine on the
A400; scales 2× with frame size — watch on the faster detector.

### Accumulator (`PtychoAccumulatorOp`)
- Write into `raw_gpu[write_idx]` until `filled_until[write_idx] == no_frames`.
- **On full (tomography): flip instead of backpressure.**
  1. `buffer_ready[write_idx].set()` (hand this projection to the recon).
  2. If `buffer_free[1-write_idx]` is set → flip `write_idx`, `filled_until[write_idx]=0`,
     clear `buffer_free[write_idx]`, write the carried straddle-tail, keep draining.
  3. Else (recon still on the other buffer — we've lapped it) → **backpressure `return`
     as today.** This is the graceful fallback for sustained slowness: double-buffer buys
     exactly one projection of runway, not infinite throughput.
- Single projection (`num_projections == 1`): unchanged — always buffer 0, no flip.

### Reconstruction (`PtychoReconstructionOp`)
- Read views from `raw_gpu[read_idx]` / `positions_full[read_idx]` (index the existing
  view re-point that already runs each `compute`).
- Run iterations; when `filled_until[read_idx] == no_frames` finalize + save the
  projection HDF (as today).
- **After finalizing:** `buffer_free[read_idx].set()`, clear `buffer_ready[read_idx]`,
  flip `read_idx`, reset object (probe carried) for the next projection.
- Per-projection object reset is the existing advance logic, keyed off the flip.

### Coordination
`write_idx` is touched only by the accumulator, `read_idx` only by the recon; they observe
each other through `buffer_ready` / `buffer_free` events and per-buffer `filled_until`
(under the existing `ptycho_state["lock"]`). Ping-pong invariant: the accumulator never
writes a buffer whose `buffer_free` is clear (recon still reading it) — enforced by step
2/3 above.

### Flush / preempt / advance
- **Full flush (scan end / R-2 start safety / PR2 header preempt):** reset BOTH buffers
  (`filled_until=[0,0]`, zero both `raw_gpu`), `write_idx=read_idx=0`, `buffer_free` both
  set, `buffer_ready` both clear, drop the carry. Fold into `_perform_flush` +
  `_reset_for_new_geometry`.
- **Per-projection advance** is now implicit in the flip — the explicit
  `advance_projection()` / `projection_complete → control → advance` round-trip can be
  simplified or kept as the buffer-free signal. Decision point below.

## Open design decisions (resolve before implementing)
- **D1 — keep or retire the `projection_complete → ControlOp → advance` round-trip?**
  With the flip owning the advance, ControlOp's role shrinks to flush-on-final only. Keep
  it for the final-projection flush; the per-projection advance becomes buffer-local.
- **D2 — 2 buffers vs an N-ring.** Start with 2 (one projection of runway, matches the
  plan). Parameterize `num_buffers` if a load test later shows one projection isn't enough
  headroom for the faster detector.
- **D3 — event objects vs simple int flags under the lock.** Events are clean but add
  threading objects to `ptycho_state`; two ints (`ready_mask`, `free_mask`) under the
  existing lock may be simpler and match the existing deferred-flag style.
- **D4 — does the STXM path need the same treatment?** `SinkAndPublishOp` saves per
  projection but doesn't hold a GPU buffer across a long finalize; it likely doesn't need
  double-buffering, but confirm it isn't coupled to the ptycho backpressure via the shared
  `gather` output.

## Testing plan
- **Regression:** the PR3 2-projection tomo test still produces exact 1024-frame
  projections + all 4 files (`{series}_proj00/01` × STXM + recon).
- **Ping-pong proof:** instrument the flip (`write_idx`/`read_idx` transitions) and confirm
  the accumulator keeps draining (no backpressure `return`) through a boundary while the
  recon finalizes — i.e. GatherOp cache HWM stays flat and the accum input queue does not
  back up during the finalize window.
- **Lapping fallback:** inject a long finalize (`PR4_FINALIZE_DELAY_MS`) longer than one
  projection's accumulation so the recon is lapped; confirm the accumulator falls back to
  clean backpressure (no data loss) rather than overwriting a buffer the recon is reading.
- **Preempt + flush:** header preemption mid-tomo resets both buffers correctly.

## Resolved decisions (as implemented)
- **D1** — kept ControlOp for `recon_complete`/`header`/`flush` only. The per-projection
  `projection_complete` round-trip is **gone**: the accumulator self-flips write buffers,
  and the **recon owns `current_projection`** + the read-buffer flip (`_flip_read`), which
  removes the save-vs-bump filename race. `current_projection` still resets to 0 on a new
  header (`header_io.py`).
- **D2** — exactly 2 buffers (`NUM_BUFFERS = 2`), one projection of runway. `num_buffers`
  is parameterised so an N-ring is a one-constant change if the faster detector needs more.
- **D3** — int-under-lock coordination (`write_idx`/`read_idx`/`filled_until[]`/`buf_free[]`
  under `ptycho_state["lock"]`); no new Event objects.
- **D4** — STXM path untouched (own `_projection` counter, no iterative recon, no buffer
  held across a finalize).

## Validation results (container, A400, 2026-07-02)
All three design tests pass (`test_data/run_pr4c/d/e.log`):
- **Test 1 — regression** (2 proj, no stress): both projections reconstruct to exactly
  1024, all 4 files, same iteration budget as single-buffer (proj0=7, proj1=5). Ping-pong
  logs confirm the flips (`Accumulator flipped to buffer 1` → `flipped read buffer to 1`).
- **Test 2 — overlap** (2 proj, 1.5 s injected finalize): the accumulator flipped to buf1
  and **fully accumulated proj-1 during proj-0's 2 s finalize** — never backpressured,
  GatherOp cache flat. This is the win vs single-buffer Run B (which stalled and relied on
  the sim's PUSH throttle).
- **Test 3 — lapping fallback** (3 proj, 3 s injected finalize): when the recon was lapped
  by a full projection, the accumulator logged `Recon lagging … backpressuring upstream`
  and **recovered** once the recon released the buffer — clean backpressure, no clobbering,
  all 3 projections at 1024, no drops/deadlock.

**O1 is now more urgent (observed):** when the recon lags acquisition, later projections
arrive **pre-filled** and finalize with near-zero refinement iterations (Test 2 proj-1 got
1 iteration vs 5 when filled concurrently). Double-buffering makes this visible; the fix is
per-projection post-stream iterations (deferred O1). On the faster detector this will matter.

## Note
All `PR4-MEASURE` instrumentation + the sim tweaks (position mapping, `PTYCHO_CENTER`,
`dummy_img_index=True`) were working-tree-only test scaffolding, reverted before this commit
(the one kept production log is the `Recon lagging …` backpressure warning). The
`num_buffers`/ping-pong state lives in `ptycho_state`; `config_sim.yaml` + `Dockerfile_bwell`
remain untracked local test scaffolding.
