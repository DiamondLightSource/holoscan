"""
Publishing Module for Holoscan STXM Pipeline

This module contains operators for:
- Publishing data to NATS
- Writing data to files
- Publishing completed datasets to cloud storage
"""

import numpy as np
import cupy as cp
import logging
import time
import os
import h5py

from holoscan.core import Operator, OperatorSpec, IOSpec, ConditionType


class SinkAndPublishOp(Operator):
    """
    Operator for sinking processed data and publishing to NATS.
    
    Receives processed STXM data, publishes individual tensors to NATS subjects,
    and optionally saves data to temporary HDF5 files for cloud publishing.
    """
    
    def __init__(self, fragment, *args,
                 stats: dict = None,
                 tensor2subject: dict[str, str] = None,
                 publish_folder=None,
                 publish_tensors: list[str] = None,
                 temp_folder: str = None,
                 **kwargs):
        """
        Initialize sink and publish operator.
        
        Args:
            fragment: Holoscan fragment
            stats: Shared statistics dictionary
            tensor2subject: Mapping of tensor names to NATS subjects
            publish_folder: Folder for final published data
            publish_tensors: List of tensors to include in published files
            temp_folder: Temporary folder for accumulating batches
        """
        self.logger = logging.getLogger(kwargs.get("name", "SinkAndPublishOp"))
        self.stats = stats
        self.stats[f"processed_frame_count"] = 0
        self.stats[f"processed_batch_count"] = 0
        self.publish_folder = publish_folder
        self.publish_tensors = publish_tensors if publish_tensors is not None else []
        self.tensor2subject = tensor2subject
        self.temp_folder = temp_folder
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.input("input").connector(IOSpec.ConnectorType.DOUBLE_BUFFER, capacity=128).condition(ConditionType.NONE)
        spec.output("processing_end").condition(ConditionType.NONE)

    def publish_to_folder(self, array_list, series_id):
        """
        Save data batch to temporary HDF5 file.
        
        Args:
            array_list: List of arrays to save
            series_id: Series identifier for filename
        """
        if self.publish_folder is None:
            return
            
        if not os.path.exists(self.temp_folder):
            os.makedirs(self.temp_folder)
        
        filepath = os.path.join(self.temp_folder, f"{series_id}.h5")
        mode = 'a' if os.path.exists(filepath) else 'w'
        
        with h5py.File(filepath, mode) as f:
            dataset_key = f"batch_{self.stats['processed_batch_count']}"
            data = np.concatenate(array_list, axis=1)
            f.create_dataset(dataset_key, data=data)

    def flush(self):
        """Reset counters on flush."""
        self.stats[f"processed_frame_count"] = 0
        self.stats[f"processed_batch_count"] = 0

    def compute(self, op_input, op_output, context):
        """Receive, publish, and save processed data."""
        # Import nats instance here to avoid circular imports
        from nats_async import launch_nats_instance
        global nats_inst
        try:
            nats_inst
        except NameError:
            nats_inst = launch_nats_instance("localhost:6000")
        
        if self.tensor2subject is None:
            return
        
        data = op_input.receive("input")
        
        if data is None:
            time.sleep(0.1)
            return
        self.logger.info(f"Received data with keys {data.keys()}")
        # Handle simple array case
        if isinstance(data, np.ndarray) and len(self.tensor2subject) == 1:
            subject = list(self.tensor2subject.values())[0]
            nats_inst.publish(subject, data)
            return
        
        # Collect arrays for file publishing
        if self.publish_folder is not None:
            arrays_to_publish = []
        
        # Publish each tensor to its NATS subject
        for tensor_key, subject in self.tensor2subject.items():
            tensor = cp.asnumpy(data[tensor_key])
            nats_inst.publish(subject, tensor)

            self.logger.info(f"Published {tensor_key} to {subject} with shape {tensor.shape}")
            if self.publish_folder is not None:
                if tensor_key in self.publish_tensors:
                    if tensor.ndim == 1:
                        tensor = tensor.reshape(-1, 1)
                    arrays_to_publish.append(tensor)
        
        # Save to temporary file
        if self.publish_folder is not None:
            if len(arrays_to_publish) > 0:
                self.publish_to_folder(arrays_to_publish, self.stats["series_id"])
            
        self.stats["processed_batch_count"] += 1
        self.stats[f"processed_frame_count"] += tensor.shape[0]
        
        # Check if processing is complete
        if self.stats[f"processed_frame_count"] == self.stats[f"series_frame_count"]:
            op_output.emit("processing_end", "processing_end")

            _n = self.stats[f"processed_frame_count"]
            _b = self.stats[f"processed_batch_count"]
            _elapsed = time.time() - self.stats[f"series_start_time"]
            _rate = _n/_elapsed
            self.logger.info(f"{_n} processed in {_elapsed:.1f}s. speed: {_rate:.1f} Hz (in {_b} batches)")


class PublishToCloudOp(Operator):
    """
    Operator for publishing completed datasets to cloud storage.
    
    Triggered when processing completes, consolidates temporary batch files
    into a single HDF5 file and publishes to the final location.
    """
    
    def __init__(self, fragment,
                 stats: dict = None,
                 publish_folder: str = None,
                 temp_folder: str = None,
                 *args, **kwargs):
        """
        Initialize cloud publishing operator.
        
        Args:
            fragment: Holoscan fragment
            stats: Shared statistics dictionary
            publish_folder: Final destination folder
            temp_folder: Source temporary folder
        """
        self.logger = logging.getLogger(kwargs.get("name", "PublishToCloudOp"))
        self.stats = stats
        self.publish_folder = publish_folder
        self.temp_folder = temp_folder
        super().__init__(fragment, *args, **kwargs)

    def setup(self, spec: OperatorSpec):
        spec.input("trigger")

    def compute(self, op_input, op_output, context):
        """Consolidate and publish dataset on trigger."""
        trigger = op_input.receive("trigger")
        if trigger == "processing_end":
            if self.publish_folder is None or self.temp_folder is None:
                return

            # Get the series ID from stats
            series_id = self.stats["series_id"]

            temp_file = os.path.join(self.temp_folder, f"{series_id}.h5")
            if not os.path.exists(temp_file):
                self.logger.warning(f"Temp file {temp_file} does not exist")
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
