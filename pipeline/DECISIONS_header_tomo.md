# Decision log — header / dynamic-geometry / tomography feature

Design decisions made while implementing the `feat/scan-header-tomo` branch (PR0→PR3),
recorded for review. Dates are when the decision was made.

## Git / workflow
| # | Decision | Rationale |
|---|----------|-----------|
| 1 | Keep the beamline sim-test tweaks OUT of the feature commits — held in a reversible patch `test_data/sim_code_tweaks.patch` (position mapping `/FMC_IN.VAL1`, `PTYCHO_CENTER` override), applied for testing, reverted before committing. | The tweaks are local test scaffolding (localhost endpoints, fixed scan centre) that must not ship in production code. |
| 2 | Single feature branch `feat/scan-header-tomo` for the whole effort (renamed from `feat/pr1-flush-plumbing`), not stacked per-PR branches. | Simpler to manage/review as one branch. |
| 3 | Dropped the pre-existing `736faf4 "Commit before merge"` via rebase; `Dockerfile_bwell` kept locally (untracked), not pushed. | That commit mixed a Blackwell Dockerfile with debug prints later removed; not part of this feature. |
| 4 | Commit sign-off trailer is `Assisted-By: Claude Opus 4.8 (1M context)`, not `Co-Authored-By:`. | Reflects Claude's assistant role. Applies to all future commits. |

## Architecture (from the plan `dls-holoscan-header-tomo-plan.md`)
| # | Decision | Rationale |
|---|----------|-----------|
| 5 | Header transport = a **dedicated ZMQ SUB socket** (`header_src`), separate from images/positions. | Keeps the geometry channel independent; always listening. |
| 6 | GPU buffers allocated **once at max capacity** (`max_npoints_h/v`), never realloced at runtime (R-6). A header requesting more frames than capacity is **rejected**. | Runtime cupy realloc under live op references risks fragmentation/leaks and a swap-under-recon race. |
| 7 | Header preemption uses a **quiescence handshake** (R-4): header stages geometry + sets `preempt_requested`; recon finishes the in-flight iteration → saves the partial → signals complete → quiesces → applies the new geometry → re-inits GPU. | The recon holds buffer *views* across a PIE iteration; geometry can only change safely while it's idle. |
| 8 | Flush model = flush-on-completion + a skip-if-clean safety flush at scan start (PR1). | Idempotent; removes stale-buffer bugs without double-flushing. |

## Tomography (PR3)
| # | Decision | Rationale |
|---|----------|-----------|
| 9 | One `arm`/`start` wraps **all projections**; `num_projections × no_frames` frames stream continuously between a single start/end. Projection boundary is segmented **by frame count**. | Matches the acquisition model; `series_id` (Dectris series counter, one per arm) is shared across all projections. |
| 10 | Each projection **early-stops** as soon as its `no_frames` are accumulated + the in-flight iteration finishes (does NOT run full `total_iterations` per projection). Single-projection scans still run to `total_iterations`. | Throughput: keep up with a continuous multi-projection stream. |
| 11 | **Projection-boundary advance is scoped** (2026-07-02): flush ONLY the accumulator (reset GPU buffer) + recon (reset object/iters) + advance the STXM sink and `current_projection`. **Do NOT flush GatherOp** — its cached next-projection frames must survive. Only the FINAL projection triggers a full scan-end flush (incl. GatherOp). | A full flush at every boundary would drop the next projection's frames piling up in GatherOp's cache (R-5). |
| 12 | Per-projection output files named `{series_id}_proj{NN}.h5` for both STXM and ptycho recon (2026-07-02). Theta omitted from the name for now (≈0 in current test data; easy to add). | All projections share one `series_id`, so the projection index is mandatory to avoid overwrite (M2). |
| 13 | Single-buffer first with a bounded GatherOp cache; add double-buffering only if a load test shows the boundary backlog overflows (PR4). | Avoid paying 2× memory + complexity before evidence it's needed. |

## Open questions (not blocking — revisit in PR4)
| # | Question | Context |
|---|----------|---------|
| O1 | Should tomography projections get a fixed number of **post-stream refinement iterations** before advancing, instead of stopping the instant all frames arrive? | For tomography (`num_projections > 1`), `is_last = all_data_arrived` (`ptychography_ops.py`), so a projection ends as soon as its `no_frames` are in — in the container test proj-0 got 7 PIE iterations and proj-1 only 4 (its frames were already cached by GatherOp during proj-0, so `all_data_arrived` tripped almost immediately). The single-scan branch grants `post_stream_iterations`; the tomography branch ignores them entirely. On a fast stream this means per-projection reconstructions are coarse. Lever: let each projection run N post-stream iterations after its frames arrive but before advancing, trading throughput for per-projection recon quality. |
