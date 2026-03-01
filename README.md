# Holoscan pipeline for real-time processing of imaging data

The image provides a unified [pixi](https://pixi.sh/) environment for the STXM pipeline (Holoscan) and PtyREX. Source is mounted at runtime so you can edit code on the host and run inside the container without rebuilding.

## Prerequisites

- Docker with NVIDIA GPU support (`--gpus all` / `--runtime=nvidia`).
- **NOTE**: This branch includes PtyREX as a git submodule. When pulling this branch, make sure to pull the submodule code as well using the following command:
  ```bash
  git submodule update --init --recursive
  ```

## Build and run

Build the container:
```bash
./build_container.sh
```

Run a shell (NATS starts on port 6000; `pipeline/`, `PtyREX/`, and `test_data/` are mounted under `/workdir`):
```bash
./run_container.sh
```



## Inside the container

### Install PtyREX (once per session)

Install the mounted PtyREX package into the pixi environment so reconstruction and tools work:
```bash
pixi run install-ptyrex
```

### Holoscan pipeline

start pixi shell:
```bash
pixi shell
```

Run the STXM pipeline:
```bash
# from the pixi shell
python pipeline/pipeline.py --config pipeline/config_prod.yaml
# or
cd pipeline
python pipeline.py --config config_prod.yaml
```

Test pipeline functionality:
```bash
# from the pixi shell
python pipeline/test_data_ingest.py --mode both --config pipeline/config_test.yaml
# or
cd pipeline
python test_data_ingest.py --mode both --config config_test.yaml
#or
python pipeline.py --config config_test.yaml
```

### PtyREX reconstruction

Using test data mounted at `/workdir/test_data`:
```bash
# from the pixi shell
python ./PtyREX/ptyrex_recon -c /workdir/test_data/config.json -p 0
```

### PtyREX visualization

View and save plots of reconstructed data (use paths to your HDF output and desired image):
```bash
python ./PtyREX/ptyrex_save_view -i /workdir/test_data/pty_out/<file>.hdf -o /workdir/test_data/pty_out/output.png
```

## Pipeline Structure

### Modules
The pipeline is organized into modular components:
- **data_io**: Data ingestion, decompression, and synchronization
- **processing**: Image processing and intensity calculations
- **publish**: Data publishing to NATS/ZMQ and file storage
- **control**: Pipeline control and coordination (flush, completion signals)

### Pipeline Diagram

```
Data Flow:                                    Control Flow:

┌─────────────────┐  ┌──────────────────┐
│ ZmqRxImageBatch │  │ ZmqRxPosition    │
│   (detector)    │  │   (PandABox)     │
└────────┬──────┬─┘  └────────┬─────────┘
         │      |             │
batches  │      |- - - - - - -| - - - - - - - - - - - ┐
         |                    |                       ▼
         │                    │                  ┌───────────────────────┐
    ┌────┴─────┐    positions │        flush     │ Control               │
    │ Decomp 0 │              │                  │    (calls flush on:   │
    ├──────────┤              │                  │ Gather, Position,     │
    │ Decomp 1 │──┐           │                  │   SinkAndPublish)     │
    ├──────────┤  │ images    │                  │                       │
    │ Decomp 2 │──┼──────┐    │                  └───────────────────────┘   
    ├──────────┤  │      │    │                        │
    │ Decomp 3 │──┘      ▼    ▼                  
    └──────────┘    ┌──────────┐                       │
                    │  Gather  │────────- - - - - - - - 
                    └─────┬────┘                       |
                          │                            
                          │ (images, positions)        |
                          ▼                            
                    ┌──────────┐                       |
                    │ Masking  │                       
                    └─────┬────┘                       |
                          │                            
                          │ (inner, outer,             |
                          │  positions, IDs)           
                          ▼                            │
                 ┌─────────────────┐    
                 │ SinkAndPublish  │───-- - - - - - - -┤
                 │  (NATS/ZMQ)     │      
                 └─────────────────┘                   |
                          │             
                          │                            │
                          │                              trigger
                          │                            ▼
                          │                     ┌──────────────┐
                          └─────────────────────┤ PublishCloud │
                                                └──────────────┘
```

### Operators

**Data Ingestion (data_io):**
- **ZmqRxPositionOp**: Receives position data from PandA via ZMQ (x, y, z, theta)
- **ZmqRxImageBatchOp**: Receives compressed image batches from detector via ZMQ
- **DecompressBatchOp**: Decompresses images (parallel instances for throughput)
- **GatherOp**: Synchronizes and pairs images with positions based on IDs

**Processing (processing):**
- **MaskingOp**: Applies circular masks to compute inner/outer ring intensities

**Publishing (publish):**
- **SinkAndPublishOp**: Publishes data to NATS or ZMQ topics for visualization and saves to temp HDF5 files
- **PublishToCloudOp**: Consolidates temp files and publishes final dataset to DECTRIS Cloud

**Control (control):**
- **ControlOp**: Coordinates pipeline flow, handles flush signals and completion events

### Data Flow
1. Images arrive from detector, positions from PandABox (both with unique IDs)
2. Images are decompressed in parallel by multiple workers
3. Gather operator synchronizes images with positions using IDs
4. Masking operator computes inner/outer intensities for each position
5. Results are published via NATS/ZMQ for real-time visualization
6. Data is accumulated in temporary files during acquisition
7. On completion, data is consolidated and published to final storage


# Additional utilities

## Stream proxy
This is a golang utility that allows to duplicate the data stream from the detector to Holoscan app and to another client.

To launch this utility, find `daqdup/streamproxy` folder, compile the code, and run the following command:
```
./streamproxy --zmq-recv-addr=tcp://127.0.0.1:5555 --zmq-send-addr1=tcp://127.0.0.1:5566 --zmq-send-addr2=tcp://127.0.0.1:5565
```

where the first address is the address of the detector, the second address is the address of the first client, and the third address is the address of the second client.

There is also a test client that can be used to test the stream proxy. To launch the test client, find `daqdup/streamclient` folder, compile the code, and run the following command:
```
./streamclient --zmq-addr=tcp://127.0.0.1:5566 # or other address as needed
```
## Profiling the pipeline with nsight systems


Profile the app:
```
nsys profile -t cuda,nvtx,osrt,python-gil -o report.nsys-rep -f true python3 pipeline.py --config holoscan_config.yaml
```
Useful: add `-d 30` to profile for 30 seconds.

# Simplon API Simulator

This is ran from a separate repo using private binaries.
