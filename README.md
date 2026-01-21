# Holoscan pipeline for real-time processing of imaging data

## Quick start
The pipeline files are located in `./pipeline`. Currently, stxm processing is supported; ptycho is under development.

To build Holoscan container:
```
./build_container.sh
```
To run the container:
```
./run_container.sh
```
The NATS server with JetStream will start automatically on port 6000.

To run the app:
```
python pipeline.py --config holoscan_config.yaml
```

To run a test for connectivity with the detector, run the following command:
```
python test_data_ingest.py --mode both --config holoscan_config.yaml
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