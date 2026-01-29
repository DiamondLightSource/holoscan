# Stats Dict → Metadata API Migration Summary

## ✅ Migration Complete

Successfully replaced the shared `stats` dictionary with Holoscan's built-in metadata API across the main pipeline operators.

## 📊 Changes Made

### 1. **pipeline.py (StxmApp)**
- ✅ Removed `self.stats = {}` initialization
- ✅ Added `self.enable_metadata(True)` to enable metadata feature
- ✅ Removed `stats=self.stats` from all operator instantiations:
  - `ZmqRxImageBatchOp`
  - `SinkAndPublishOp`
  - `PublishToCloudOp`
  - `ControlOp`

### 2. **data_io.py (ZmqRxImageBatchOp)**
- ✅ Removed `stats: dict` parameter from `__init__`
- ✅ Converted stats fields to local instance variables:
  - `self.series_frame_count`
  - `self.series_start_time`
  - `self.first_frame_flag`
  - `self.series_id`
  - `self.series_finished`
- ✅ Updated `compute()` to **set metadata** before emitting:
  ```python
  self.metadata.set("series_id", self.series_id)
  self.metadata.set("series_frame_count", self.series_frame_count)
  self.metadata.set("series_start_time", self.series_start_time)
  # Metadata automatically flows with op_output.emit()
  ```

### 3. **publish.py (SinkAndPublishOp)**
- ✅ Removed `stats: dict` parameter from `__init__`
- ✅ Converted to local instance variables:
  - `self.processed_frame_count`
  - `self.processed_batch_count`
- ✅ Updated `compute()` to **read metadata** from upstream:
  ```python
  # Receive data - metadata automatically merged
  data = op_input.receive("input")
  
  # Read metadata that flowed from upstream
  series_id = self.metadata.get("series_id")
  series_frame_count = self.metadata.get("series_frame_count", 0)
  series_start_time = self.metadata.get("series_start_time", 0.0)
  
  # Use for completion check
  if self.processed_frame_count == series_frame_count:
      op_output.emit("processing_end", "processing_end")
  ```

### 4. **publish.py (PublishToCloudOp)**
- ✅ Removed `stats: dict` parameter from `__init__`
- ✅ Updated `compute()` to read `series_id` from metadata:
  ```python
  trigger = op_input.receive("trigger")
  series_id = self.metadata.get("series_id")
  ```

### 5. **control.py (ControlOp)**
- ✅ Removed unused `stats: dict` parameter from `__init__`

## 🔄 How Metadata Flows

```
ZmqRxImageBatchOp (Source)
    │ Sets metadata:
    │ - series_id
    │ - series_frame_count
    │ - series_start_time
    ↓ (emit automatically sends metadata)
DecompressBatchOp
    ↓ (metadata flows through)
GatherOp
    ↓ (metadata flows through)
MaskingOp
    ↓ (metadata flows through)
SinkAndPublishOp
    │ Reads metadata:
    │ - series_id (for file naming)
    │ - series_frame_count (for completion check)
    │ - series_start_time (for rate calculation)
    ↓ (emit sends metadata onwards)
ControlOp
    ↓ (metadata flows through)
PublishToCloudOp
    │ Reads metadata:
    │ - series_id (for file lookup)
```

## 🎯 Key Advantages

1. **Automatic Propagation**: Metadata flows automatically with `emit()` calls
2. **No Coupling**: Operators don't need to know about each other
3. **Type-Safe**: `metadata.get()` and `metadata.set()` provide clean interface
4. **No Shared State**: Eliminates thread-safety concerns from shared dict
5. **Holoscan-Native**: Uses framework's built-in feature designed for this purpose

### 6. **test_data_ingest.py (Test Applications)**
- ✅ Updated all three test applications:
  - `ImageIngestApp`
  - `BothIngestApp`
  - `SynchronizedIngestApp`
- ✅ Removed `self.stats = {}` initialization
- ✅ Added `self.enable_metadata(True)` to each app
- ✅ Removed `stats=self.stats` from `ZmqRxImageBatchOp` calls

## 📝 Remaining Stats Usage (Intentionally Not Migrated)

The following **legacy** files still use `stats` dict:

- **legacy/pipeline_stxm_beamtime.py**: Legacy implementation
- **legacy/pipeline_stxm_image_only.py**: Legacy implementation
- **legacy/scratch.py**: Development scratch file

These can be migrated later if needed.

## ✅ Verification

Run grep to confirm no stats dict in main pipeline:
```bash
# Should return 0 results in main files
grep -n "stats=" pipeline/pipeline.py pipeline/data_io.py pipeline/publish.py pipeline/control.py pipeline/test_data_ingest.py
```

## 🔧 Troubleshooting

### Error: "python object could not be converted to Arg"

**Problem**: This error occurs when calling an operator that has had the `stats` parameter removed, but the calling code still passes it.

**Example Error**:
```
RuntimeError: python object could not be converted to Arg
```

**Solution**: Remove `stats=self.stats` from the operator instantiation call.

**Before**:
```python
img_src = ZmqRxImageBatchOp(self,
                           stats=self.stats,  # ← Remove this
                           name="image_src",
                           ...)
```

**After**:
```python
img_src = ZmqRxImageBatchOp(self,
                           name="image_src",
                           ...)
```

### Error: "Key 'xxx' already exists" (MetadataPolicy)

**Problem**: The default metadata policy doesn't allow setting the same key multiple times within a single `compute()` call.

**Example Error**:
```
RuntimeError: Key 'series_start_time' already exists. The application should be updated 
to avoid duplicate metadata keys or a different holoscan::MetadataPolicy should be set
```

**Root Cause**: Using `self.metadata.set()` multiple times for the same key in one `compute()` call.

**Solution**: Restructure your code so each key is only set once per `compute()` call. Remember that metadata is cleared at the start of each `compute()`, so you have a fresh slate each time.

**Before**:
```python
def compute(self, op_input, op_output, context):
    if first_message:
        self.metadata.set("key", 0.0)
        # ... more logic ...
    if another_condition:
        self.metadata.set("key", 1.0)  # ❌ Error if both conditions true!
```

**After**:
```python
def compute(self, op_input, op_output, context):
    # Set all metadata once before emitting
    if first_message:
        value = 0.0
    elif another_condition:
        value = 1.0
    
    self.metadata.set("key", value)  # ✅ Set once per compute
    op_output.emit(data, port)
```

### Error: "update(): incompatible function arguments"

**Problem**: The `update()` method expects another `MetadataDictionary`, not a Python dict.

**Example Error**:
```
TypeError: update(): incompatible function arguments. The following argument types are supported:
    1. (self: holoscan.core._core.MetadataDictionary, other: holoscan.core._core.MetadataDictionary) -> None
Invoked with: {}, {'series_start_time': 1769703238.2229972}
```

**Solution**: Use `set(key, value)` for individual key-value pairs, not `update({key: value})`.

**Wrong**:
```python
self.metadata.update({"key": value})  # ❌ Wrong type!
```

**Correct**:
```python
self.metadata.set("key", value)  # ✅ Correct API
```

## 🚀 Testing Recommendations

1. **Test series start**: Verify metadata is set correctly on "start" messages
2. **Test series completion**: Verify completion detection using metadata
3. **Test metadata propagation**: Ensure all operators receive correct metadata
4. **Test file publishing**: Verify `series_id` flows to cloud publishing operator
5. **Test rate calculation**: Verify `series_start_time` is used correctly

## 📚 References

- [Holoscan Metadata API Documentation](https://docs.nvidia.com/holoscan/archive/3.2.0/holoscan_create_app.html#dynamic-application-metadata)
- Metadata is **enabled by default** in Holoscan v3.0+
- Metadata flows automatically with `emit()` and is merged on `receive()`
