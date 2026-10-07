"""
Publishing Module for Holoscan STXM Pipeline

This module contains operators for:
- Publishing data to NATS or ZMQ
- Writing data to files
- Publishing completed datasets to cloud storage
"""

import numpy as np
import cupy as cp
import logging
import time
import os
import h5py
import json
import zmq

from holoscan.core import Operator, OperatorSpec, IOSpec, ConditionType


class PublishBackend:
    """Base class for publishing backends."""
    
    def publish(self, subject: str, data: np.ndarray):
        """Publish data to a subject/topic."""
        raise NotImplementedError


class NatsBackend(PublishBackend):
    """NATS publishing backend."""
    
    def __init__(self, host: str = "localhost:6000"):
        from nats_async import launch_nats_instance
        self.nats_inst = launch_nats_instance(host)
        self.logger = logging.getLogger("NatsBackend")
    
    def publish(self, subject: str, data: np.ndarray):
        """Publish data to NATS subject."""
        self.nats_inst.publish(subject, data)


class ZmqBackend(PublishBackend):
    """ZMQ PUB/SUB publishing backend."""
    
    def __init__(self, endpoint: str = "tcp://*:9999"):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.PUB)
        self.socket.bind(endpoint)
        self.logger = logging.getLogger("ZmqBackend")
        self.logger.info(f"ZMQ publisher bound to {endpoint}")
        # Give time for subscribers to connect
        time.sleep(0.1)
    
    def publish(self, subject: str, data: np.ndarray):
        """Publish data to ZMQ topic as binary numpy."""
        topic = subject.encode('utf-8')
        header = json.dumps({"shape": list(data.shape), "dtype": str(data.dtype)}).encode('utf-8')
        self.socket.send_multipart([topic, header, data.tobytes()])
    
    def close(self):
        """Close ZMQ socket."""
        self.socket.close()
        self.context.term()


class SinkAndPublishOp(Operator):
    """
    Operator for sinking processed data and publishing to NATS or ZMQ.
    
    Receives processed STXM data, publishes individual tensors to subjects/topics,
    and optionally saves data to temporary HDF5 files for cloud publishing.
    """
    
    def __init__(self, fragment, *args,
                 tensor2subject: dict[str, str] = None,
                 publish_folder=None,
                 publish_tensors: list[str] = None,
                 temp_folder: str = None,
                 publish_backend: PublishBackend = None,
                 backend: str = "nats",
                 backend_endpoint: str = None,
                 scan_state: dict = None,
                 **kwargs):
        """
        Initialize sink and publish operator.
        
        Args:
            fragment: Holoscan fragment
            tensor2subject: Mapping of tensor names to subjects/topics
            publish_folder: Folder for final published data
            publish_tensors: List of tensors to include in published files
            temp_folder: Temporary folder for accumulating batches
            publish_backend: Pre-created backend instance (preferred)
            backend: Publishing backend type ('nats' or 'zmq') - used if publish_backend is None
            backend_endpoint: Endpoint for backend - used if publish_backend is None
        """
        self.logger = logging.getLogger(kwargs.get("name", "SinkAndPublishOp"))
        
        # Local counters (not shared)
        self.processed_frame_count = 0
        self.processed_batch_count = 0

        # In-memory accumulator of per-batch arrays for the current scan/projection.
        # Avoids per-batch HDF5 open/close on the compute hot path; the file
        # is written once at scan end (or per projection for tomography).
        self.scan_buffer = []
        # Save-state tracking so flush never discards an unwritten scan buffer.
        self._written = False
        self._series_id = None
        # PR3 tomography: shared holder (num_projections, no_frames) + a LOCAL
        # per-projection index/counter so the STXM path segments itself (S2),
        # independent of the ptycho path's current_projection.
        self.scan_state = scan_state
        self._projection = 0
        self._proj_frame_count = 0

        self.publish_folder = publish_folder
        self.publish_tensors = publish_tensors if publish_tensors is not None else []
        self.tensor2subject = tensor2subject
        self.temp_folder = temp_folder
        self.backend = publish_backend  # Use pre-created backend if provided
        self.backend_type = backend
        self.backend_endpoint = backend_endpoint
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.input("input").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128).condition(ConditionType.NONE)

    def write_scan_file(self, series_id):
        """Write the buffered scan to a single HDF5 file.

        Called once at scan end (processing_end). Concatenates the per-batch
        arrays accumulated in ``self.scan_buffer`` along the frame axis and
        writes one ``stxm`` dataset — a single open/close, off the per-batch
        hot path.
        """
        if self.publish_folder is None or not self.scan_buffer:
            return

        os.makedirs(self.publish_folder, exist_ok=True)
        data = np.concatenate(self.scan_buffer, axis=0)
        filepath = os.path.join(self.publish_folder, f"{series_id}.h5")
        with h5py.File(filepath, 'w') as f:
            f.create_dataset('stxm', data=data)
        self.logger.info(f"Wrote {data.shape[0]} frames to {filepath}")
        # Self-contained: mark saved and clear the buffer so a later flush has
        # nothing to discard.
        self._written = True
        self.scan_buffer = []

    def _write_projection_file(self, series_id, projection):
        """PR3: write the current projection's buffered STXM to its own file,
        named with the shared series_id + projection index (M2), then clear the
        buffer for the next projection."""
        if self.publish_folder is None or not self.scan_buffer:
            return
        os.makedirs(self.publish_folder, exist_ok=True)
        data = np.concatenate(self.scan_buffer, axis=0)
        filepath = os.path.join(self.publish_folder, f"{series_id}_proj{projection:02d}.h5")
        with h5py.File(filepath, 'w') as f:
            f.create_dataset('stxm', data=data)
            f.attrs['projection'] = projection
            f.attrs['series_id'] = str(series_id)
        self.logger.info(f"Wrote projection {projection} ({data.shape[0]} frames) to {filepath}")
        self._written = True
        self.scan_buffer = []

    def flush(self):
        """Reset counters. If the buffer still holds data that was never written
        (a flush arrived before the end-of-scan write), write it out first so a
        scan's STXM file is never lost."""
        if (self.scan_buffer and not self._written
                and self.publish_folder is not None and self._series_id is not None):
            self.logger.warning(
                "Flush with %d unwritten STXM batch(es) — writing before clearing",
                len(self.scan_buffer),
            )
            # Name with the projection index if we're mid-tomography.
            scan_state = self.scan_state or {}
            if int(scan_state.get("num_projections", 1)) > 1:
                self._write_projection_file(self._series_id, self._projection)
            else:
                self.write_scan_file(self._series_id)
        self.processed_frame_count = 0
        self.processed_batch_count = 0
        self.scan_buffer = []
        self._written = False
        self._projection = 0
        self._proj_frame_count = 0
    
    def _publish_stxm_flush(self):
        """Notify downstream consumers that the current STXM projection is done."""
        if self.backend is None:
            return
        import numpy as np
        self.backend.publish("stxm_flush", np.array([1]))
    
    def compute(self, op_input, op_output, context):
        """Receive, publish, and save processed data using metadata."""
        # Initialize backend on first call
        if self.backend is None:
            if self.backend_type == "nats":
                endpoint = self.backend_endpoint or "localhost:6000"
                self.backend = NatsBackend(endpoint)
                self.logger.info(f"Initialized NATS backend at {endpoint}")
            elif self.backend_type == "zmq":
                endpoint = self.backend_endpoint or "tcp://*:9999"
                self.backend = ZmqBackend(endpoint)
                self.logger.info(f"Initialized ZMQ backend at {endpoint}")
            else:
                self.logger.error(f"Unknown backend type: {self.backend_type}")
                return
        
        if self.tensor2subject is None:
            return
        
        # Receive data - metadata is automatically merged from upstream
        data = op_input.receive("input")
        
        if data is None:
            time.sleep(0.1)
            return
            
        # Read metadata that flowed from upstream operators
        series_id = self.metadata.get("series_id")
        series_frame_count = self.metadata.get("series_frame_count", 0)
        series_start_time = self.metadata.get("series_start_time", 0.0)
        
        # self.logger.info(f"Received data with keys {data.keys()}")
        # Handle simple array case
        if isinstance(data, np.ndarray) and len(self.tensor2subject) == 1:
            subject = list(self.tensor2subject.values())[0]
            self.backend.publish(subject, data)
            return
        
        # Collect arrays for file publishing
        if self.publish_folder is not None:
            arrays_to_publish = []
        
        # Publish each tensor to its subject/topic
        for tensor_key, subject in self.tensor2subject.items():
            tensor = cp.asnumpy(data[tensor_key])
            self.backend.publish(subject, tensor)

            # self.logger.info(f"Published {tensor_key} to {subject} with shape {tensor.shape}")
            if self.publish_folder is not None:
                if tensor_key in self.publish_tensors:
                    if tensor.ndim == 1:
                        tensor = tensor.reshape(-1, 1)
                    arrays_to_publish.append(tensor)
        
        # Buffer this batch in memory (no file I/O on the hot path)
        if self.publish_folder is not None:
            if len(arrays_to_publish) > 0:
                self.scan_buffer.append(np.concatenate(arrays_to_publish, axis=1))
                self._written = False        # new unsaved data
                self._series_id = series_id  # remembered for a defensive flush-save

        self.processed_batch_count += 1
        self.processed_frame_count += tensor.shape[0]

        # Share series_id so the ptycho per-projection recon files can be named
        # with the same identifier (PR3).
        if self.scan_state is not None and series_id is not None:
            self.scan_state["series_id"] = series_id

        scan_state = self.scan_state or {}
        num_projections = int(scan_state.get("num_projections", 1))
        proj_no_frames = int(scan_state.get("no_frames", 0))

        if num_projections > 1 and proj_no_frames > 0:
            # Tomography (S2): save one STXM file per projection, segmented by
            # frame count — self-contained, no ControlOp round-trip. Uses >= with
            # a count carry (I1); exact frame boundaries require no_frames to be a
            # multiple of batch_size (a few overshoot frames otherwise land in the
            # current projection's file).
            self._proj_frame_count += tensor.shape[0]
            if self._proj_frame_count >= proj_no_frames:
                if self.publish_folder is not None and series_id is not None:
                    self._write_projection_file(series_id, self._projection)
                    
                # Notify the visualizer / downstream consumers that the
                # previous projection is complete and should be cleared.
                self._publish_stxm_flush()
                self._proj_frame_count -= proj_no_frames   # carry overshoot count
                self._projection += 1
        else:
            # Single scan: write the whole series once (existing behaviour). Use
            # >= (not ==) so a batch overshooting the exact count still triggers.
            if series_frame_count > 0 and self.processed_frame_count >= series_frame_count:
                if self.publish_folder is not None and series_id is not None:
                    self.write_scan_file(series_id)

                _n = self.processed_frame_count
                _b = self.processed_batch_count
                _elapsed = time.time() - series_start_time if series_start_time > 0 else 0
                _rate = _n/_elapsed if _elapsed > 0 else 0
                self.logger.info(f"{_n} processed in {_elapsed:.1f}s. speed: {_rate:.1f} Hz (in {_b} batches)")


class PublishToCloudOp(Operator):
    """
    Operator for publishing completed datasets to cloud storage.

    Triggered when processing completes, consolidates temporary batch files
    into a single HDF5 file and publishes to the final location.

    NOTE: STXM saving now happens in SinkAndPublishOp.write_scan_file (a single
    end-of-scan write), so the per-batch temp files this op consolidated are no
    longer produced. It is retained for a possible future external/temp-file
    workflow and no-ops gracefully when no temp file is present.

    NOT CURRENTLY WIRED into the pipeline (see pipeline.py). To re-enable, feed a
    completion trigger into its "trigger" input (e.g. from SinkAndPublishOp).
    """

    def __init__(self, fragment,
                 publish_folder: str = None,
                 temp_folder: str = None,
                 *args, **kwargs):
        """
        Initialize cloud publishing operator.

        Args:
            fragment: Holoscan fragment
            publish_folder: Final destination folder
            temp_folder: Source temporary folder
        """
        self.logger = logging.getLogger(kwargs.get("name", "PublishToCloudOp"))
        self.publish_folder = publish_folder
        self.temp_folder = temp_folder
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.input("trigger")

    def compute(self, op_input, op_output, context):
        """Consolidate and publish dataset on trigger using metadata."""
        # Receive trigger - metadata is automatically merged
        trigger = op_input.receive("trigger")

        if trigger == "processing_end":
            if self.publish_folder is None or self.temp_folder is None:
                return

            # Get the series ID from metadata that flowed from upstream
            series_id = self.metadata.get("series_id")

            if series_id is None:
                self.logger.warning("No series_id found in metadata, cannot publish")
                return

            temp_file = os.path.join(self.temp_folder, f"{series_id}.h5")
            if not os.path.exists(temp_file):
                # Expected now that SinkAndPublishOp writes the final file
                # directly; nothing to consolidate.
                self.logger.debug(f"No temp file {temp_file} to consolidate")
                return

            try:
                # Read and concatenate all batch arrays
                with h5py.File(temp_file, 'r') as f:
                    batch_keys = sorted([k for k in f.keys() if k.startswith('batch_')])
                    if not batch_keys:
                        self.logger.warning(f"No batch arrays found in {temp_file}")
                        return

                    batches = [f[k][:] for k in batch_keys]
                    concatenated_data = np.concatenate(batches, axis=0)

                # Create publish folder if needed
                os.makedirs(self.publish_folder, exist_ok=True)
                publish_file = os.path.join(self.publish_folder, f"{series_id}.h5")

                # Write concatenated data
                with h5py.File(publish_file, 'w') as f:
                    f.create_dataset('stxm', data=concatenated_data)
                    # Copy attributes from source
                    with h5py.File(temp_file, 'r') as src:
                        for key, value in src.attrs.items():
                            f.attrs[key] = value

                self.logger.info(f"Published concatenated data to {publish_file}")

                # Remove temp file
                os.remove(temp_file)

            except Exception as e:
                self.logger.error(f"Error processing HDF5 file: {str(e)}")
