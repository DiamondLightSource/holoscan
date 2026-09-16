# Testing log — header / dynamic-geometry / tomography feature

Branch `feat/scan-header-tomo` (PR0 → PR3). Tests run in the `ptycho-holoscan:ptyrex`
container on the **A400** (`CUDA_VISIBLE_DEVICES=1`), driven by the daqsim simulator
(`daqsim:latest`, Dectris SIMPLON emulator) streaming scan **409907** (32×32 = 1024
frames, 515×515 uint32). Sim tweaks applied via `test_data/sim_code_tweaks.patch`;
geometry headers sent with `test_data/send_header.py`; scans triggered with
`dectris-hackathon/daqsim/trigger.py --stream both --nimages 1024`.

## PR0 — Unwire PublishToCloudOp
- Regression: STXM + ptycho scans produce identical output with the op unwired. Pipeline
  composes and runs. (Validated during the PR0/PR1 session.)

## PR1 — Flush plumbing + hardening
- Single ptycho scan reconstructs to `total_iterations` and idles; `recon_complete` logged.
- Back-to-back second scan: start-flush safety net fires, clean second reconstruction, two
  output HDFs. No-double-flush confirmed (`Start-flush skipped — already flushed on completion`).

## PR2 — Live header operator + dynamic geometry — ALL PASS (2026-07-02)
Container run, logs captured to `test_data/run34.log`.

| # | Test | Result |
|---|------|--------|
| 1 | Launch with `npoints_*` removed from config | ✅ `buffers allocated at capacity: 1024 frames`, `Scan geometry configured: 1024 frames … object size (…,523,523)` |
| 2 | Baseline scan + back-to-back regression | ✅ both reconstruct to iter 25 and flush; no-double-flush holds |
| 3 | Header before data → full scan on reconfigured geometry | ✅ full handshake (`Received header`→`Header received`→`Recon quiesced`→`Applied new scan geometry`→`Recon reset`); scan then reconstructs to completion, **all 1024 positions valid**. 2nd `before_reconstruction_stream` stable. |
| 4 | Header mid-reconstruction (preemption) | ✅ `Saved partial result before preemption (iter 24)` emitted **before** flush/reconfigure, then quiesce → reconfigure → reset |
| 4b | Recovery after preemption | ✅ `GPU initialisation complete` → fresh scan `Reconstruction complete at iteration 25` |
| 5 | Oversized grid (64×64 = 4096 > capacity 1024) | ✅ `Header grid 64x64 … exceeds capacity 1024 — rejected`; rejected before staging, pipeline stays alive |

**Caveat:** reconfigure tested only to the *same* geometry (32×32 / 1.5 µm, object stayed
523×523). The reconfigure + GPU-reinit code path is fully exercised; a change to a
*different* object size with matching stream data is unverified (needs a 2nd dataset).

## PR3 — Tomography / multi-projection — PASS (2026-07-02)
Container run (A400, `config_sim.yaml`), 2-projection header sent, 2048-frame stream
(`trigger.py --stream both --nimages 2048`, sequence_id 29). Logs in `test_data/run_pr3c.log`.

| # | Test | Result |
|---|------|--------|
| 1 | Header sets tomography mode | ✅ `Received header … num_projections=2` → handshake → geometry applied |
| 2 | Projection 0 completes at frame boundary | ✅ `Wrote projection 0 (1024 frames) → 29_proj00.h5`, `Saved projection recon → 29_proj00_recon.h5`, `Projection 0/2 complete … signal=projection_complete` (early-stopped at iter 7) |
| 3 | Scoped advance (accum + recon reset, GatherOp cache preserved) | ✅ `Projection complete — advanced to projection 1`; accumulator advanced with carry preserved, recon object reset / probe carried, GatherOp **not** flushed |
| 4 | Projection 1 completes | ✅ `Wrote projection 1 (1024 frames) → 29_proj01.h5`, `Saved projection recon → 29_proj01_recon.h5`, `Projection 1/2 complete … signal=recon_complete` (final; early-stopped at iter 4) |
| 5 | Final flush | ✅ `Reconstruction complete — flushing for next scan` |

All four files on disk (`series_id=29`, both projections × STXM + recon), 1024-frame counts
exact, and the non-final vs final completion signals correctly distinguished
(`projection_complete` for proj-0, `recon_complete` for proj-1). No stall.

**Test-tweak note:** the run used `dummy_img_index=True` (continuous synthetic image IDs)
to work around a daqsim looped-replay artifact where image IDs reset per loop while
position IDs continue. Testing exposed an off-by-one in that path
(`data_io.py`: `series_frame_count - 1` produced IDs `-1..N-2`, leaving one frame
unmatched per scan) — fixed to `series_frame_count`. `dummy_img_index` reverted to
`False` before commit; a real single-series tomography acquisition (monotonic IDs)
would not hit the artifact.

**Open item (see DECISIONS O1):** each projection early-stops the moment its frames
arrive, so proj-1 got fewer PIE iterations than proj-0 (cached frames tripped
`all_data_arrived` immediately). Per-projection post-stream refinement iterations
deferred to PR4.
