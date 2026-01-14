# Holoscan pipeline

The ptychography reconstruction app files are located in `./ptycho`.
Ptycho pipeline is under development.

STXM analysis app files are located in `./stxm`.


To build Holoscan container:
```
docker build ./stxm -t ptycho-holoscan:stxm --network host
```

To run the container:
```
docker run -it --rm --ipc=host --privileged \
    --runtime=nvidia \
    --gpus all \
    --ulimit memlock=-1 \
    --ulimit stack=67108864 \
    --network host \
    -v ./ptycho:/workspace/ptycho \
    -v ./stxm:/workspace/stxm \
    ptycho-holoscan:stxm
```
The mounting of the folder is done to avoid rebuilding the container on every code change during the development.

The entrypoint at the moment is simply bash. The app consists of several pieces (TODO: add other pieces):

```
cd ../stxm
pipeline_speed.py - Rx speed test
pipeline_stxm.py - STXM analysis
pipeline_stxm_image_only.py - STXM analysis with image only; positions are ommitted. Also, currently this file has optimized data recieving with buffered message ingest and parallel decompression.
```

Prior to running the app, look at the config file `stxm/holoscan_config.yaml` (or other config file, as needed) and make sure the network ports and data formats are correct.

To run the app:
```
cd /workspace/stxm
python pipeline_stxm_image_only.py # or other file as needed
```

# Stream proxy
This is a golang utility that allows to duplicate the data stream from the detector to Holoscan app and to another client.

To launch this utility, run the following command:
```
cd /workspace/stxm/streamproxy
./streamproxy --zmq-recv-addr=tcp://127.0.0.1:5555 --zmq-send-addr1=tcp://127.0.0.1:5566 --zmq-send-addr2=tcp://127.0.0.1:5565
```

where the first address is the address of the detector, the second address is the address of the first client, and the third address is the address of the second client.

There is also a test client that can be used to test the stream proxy. To launch the test client, run the following command:
```
cd /workspace/stxm/streamclient
./streamclient --zmq-addr=tcp://127.0.0.1:5566 # or other address as needed
```

The corresponding folders also contain the code for the test client (one would need to install the golang compiler to build it).

# Profiling the pipeline with nsight systems

When profiling the pipeline, the Linux operating system’s perf_event_paranoid level must be 2 or less. Use the following command to check:
```
cat /proc/sys/kernel/perf_event_paranoid
```
If the output is >2, then do the following to temporarily adjust the paranoid level (note that this has to be done after each reboot):
```
sudo sh -c 'echo 2 >/proc/sys/kernel/perf_event_paranoid'
```

Profile the app:
```
nsys profile -t cuda,nvtx,osrt,python-gil -o report.nsys-rep -f true python3 pipeline_stxm_image_only.py # or other file as needed
```
Useful: add `-d 30` to profile for 30 seconds.

# Simplon API Simulator

NOTE: the position data stream is working.


TODO: automate the data download.


To run simplon API simulator do the following:
- download daqsim binaries from [here](https://drive.google.com/file/d/1PWsSH2rF0viSYFGsRwVWHaOtjts6A7MM/view) (email Dirk for access) and unpack it to `./holoscan_pipeline/simplon_sim/`
- `cd ./holoscan_pipeline/simplon_sim/daqsim/`
- rename `selun` folder to `selun_test` - this is the folder with mvp test data to test daqsim with a single image
- create `selun` folder and copy `detector_config.pb` to `./holoscan_pipeline/simplon_sim/daqsim/selun`
- test data is not included in the repository, so you need to download it from DECTRIS Cloud
- download image and position data from DECTRID Cloud. For example:
    - `selun_test_20240519_162732_386344_master.h5`
    - `positions_0.h5`
- put the data to `./holoscan_pipeline/simplon_sim/test_data/`


- Build docker file:
```
docker build ./simplon_sim/ -t daqsim:test --network host
```
- run the container:
```
docker run -it --network host \
    -v ./simplon_sim/daqsim:/workspace/daqsim \
    -v ./simplon_sim/test_data:/workspace/daqsim/test_data \
    daqsim:test
```

- To convert the data into cbor format run:
```
python convert_h5_to_cbor.py
```
Depending on the hardware and the number of files chosen (see below), the conversion may take from a few minutes to a few tens of minutes.

The execution of the script is controlled by the `ENV` variables in the `Dockerfile`:
```
HDF5_MASTER_FILE - path to the master file
HDF5_POSITION_FILE - path to the position file
CBOR_NMAX_FILES - number of files to convert. Set to -1 to process all files.
CBOR_OVERWRITE - flag to overwrite existing files
CBOR_OUTPUT_DIR - output directory
```

The test scan represents ptycho-tomography data. To cut the wait time, the `convert_h5_to_cbor.py` script converts data in batches of 4792 with total number of batches `CBOR_NMAX_FILES//4792` spread uniformly across the entire scan. This way, when running the pipeline, one can inspect the data coming in at the full spread of angles.

- To launch data stream send an appropriate request sequence to the daqsim. E.g. in python
```
import requests
REST = "http://0.0.0.0:8000"
r = requests.put(f"{REST}/detector/api/1.8.0/command/initialize")
r = requests.put(f"{REST}/detector/api/1.8.0/command/arm")
r = requests.put(f"{REST}/detector/api/1.8.0/command/trigger")
```
