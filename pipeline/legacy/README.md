# Legacy STXM Pipeline Files

**Status:** 🗄️ **ARCHIVED**  
**Date Archived:** 2025-10-27

## Overview

This directory contains legacy versions of the STXM pipeline that have been superseded by the new modular structure. These files are kept for reference only and should not be used for new development.

## Files in This Directory

### Pipeline Files (Superseded by New Modular Structure)

| File | Description | Replaced By |
|------|-------------|-------------|
| `pipeline_stxm_beamtime.py` | Original monolithic STXM pipeline (808 lines) | `pipeline.py` + modules |
| `pipeline_stxm.py` | Earlier STXM pipeline version (230 lines) | `pipeline.py` |
| `pipeline_stxm_image_only.py` | Image-only processing variant (392 lines) | `test_data_ingest.py` |
| `pipeline_publish_raw_data.py` | Raw data publishing variant (336 lines) | `sink_control.py` |
| `pipeline_speed.py` | Speed testing script (332 lines) | `test_data_ingest.py` |
| `scratch.py` | Development/testing code (574 lines) | N/A |

### Data Files

| File | Type | Size | Description |
|------|------|------|-------------|
| `speed_report_decomp_5_mts_4.nsys-rep` | Performance | ~27 MB | Nsight Systems performance report |
| `zmq_panda_output.log` | Log | ~10 MB | ZMQ data output log |

## Migration to New Structure

### If You Were Using `pipeline_stxm_beamtime.py`

**Old:**
```python
python pipeline_stxm_beamtime.py --config holoscan_config.yaml
```

**New:**
```python
python pipeline.py --config holoscan_config.yaml
```

### If You Were Using `pipeline_stxm_image_only.py`

**Old:**
```python
python pipeline_stxm_image_only.py
```

**New:**
```python
python test_data_ingest.py --mode images
```

### If You Were Using `pipeline_speed.py`

**Old:**
```python
python pipeline_speed.py --config holoscan_config.yaml
```

**New:**
```python
python test_data_ingest.py --mode both --config test_ingest_config.yaml
```

## New Modular Structure

The functionality from these legacy files is now split across modular components:

```
stxm/
├── data_rx.py              # Data ingestion (ZMQ, CBOR, decompression)
├── processing.py           # Processing (masking, gathering)
├── sink_control.py         # Output (NATS, HDF5, control)
├── pipeline.py             # Main STXM application
├── test_data_ingest.py     # IO testing utilities
├── stxm_calc.py            # Calculation helpers (kept)
├── map_calc.py             # Map calculations (kept)
└── legacy/                 # This directory
```

## Key Improvements in New Structure

### 1. **Modularity**
- Clear separation of concerns
- Each module has a single responsibility
- Easier to find and modify specific functionality

### 2. **Reusability**
- Operators can be imported and used in other pipelines
- No code duplication
- Single source of truth

### 3. **Testability**
- Modules can be tested independently
- Dedicated test utilities
- Mock data generators included

### 4. **Maintainability**
- Fix bugs in one place
- Add features without affecting other code
- Clear dependencies

## Import Migration Guide

### Old Imports (from legacy files)

```python
# From pipeline_stxm_beamtime.py
from pipeline_stxm_beamtime import (
    ZmqRxImageBatchOp,
    DecompressBatchOp,
    MaskingOp,
    GatherOp,
    SinkAndPublishOp
)
```

### New Imports (from modular structure)

```python
# From new modules
from data_io import ZmqRxImageBatchOp, DecompressBatchOp, GatherOp
from processing import MaskingOp
from publish import SinkAndPublishOp, PublishToCloudOp
from control import ControlOp
from pipeline import StxmApp
```

## Why These Files Were Archived

1. **Code Duplication**: Each file reimplemented similar operators
2. **Hard to Maintain**: Bugs needed to be fixed in multiple places
3. **Inconsistent**: Different files had slightly different implementations
4. **Large File Sizes**: Monolithic files were hard to navigate
5. **Testing Difficulty**: Couldn't test components independently

## Can I Still Use These Files?

**Technical Answer:** Yes, they should still work.

**Recommended Answer:** No, use the new modular structure instead.

**Why?**
- These files won't receive bug fixes
- They lack new features
- They don't follow best practices
- They make the codebase harder to maintain

## Need Help Migrating?

See the documentation:
- **Complete Guide**: `../REFACTORING.md`
- **Quick Start**: `../QUICKSTART_REFACTORED.md`
- **API Reference**: Check docstrings in `data_rx.py`, `processing.py`, `sink_control.py`

## Performance Reports

The `speed_report_decomp_5_mts_4.nsys-rep` file can be opened with Nsight Systems for performance analysis:

```bash
nsys-ui speed_report_decomp_5_mts_4.nsys-rep
```

This shows the performance characteristics of the old pipeline and can be used for comparison with the new structure.

## When to Reference These Files

These files can be useful for:
- **Understanding Evolution**: See how the pipeline developed over time
- **Debugging**: Compare behavior with legacy versions
- **Performance Comparison**: Benchmark old vs. new implementations
- **Learning**: Study different approaches to the same problem

## Removal Timeline

These files are currently scheduled for:
- **Phase 1 (Current)**: Archived but accessible
- **Phase 2 (3 months)**: Move to separate archive repository
- **Phase 3 (6 months)**: Consider removal if no longer needed

If you need these files to remain accessible, please contact the development team.

---

**Archived By:** Refactoring Initiative 2025  
**Last Updated:** 2025-10-27  
**Questions?** See `../REFACTORING.md` or contact the team


