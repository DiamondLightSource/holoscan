# Holoscan pipeline for real-time processing of imaging data

The pipeline files are located in `./pipeline`. Currently, stxm processing is supported; ptycho is under development.

To build Holoscan container:
```
./build_container.sh
```

To run the container:
```
./run_container.sh
```

To run the app:
```
cd ./workspace/
python pipeline.py --config holoscan_config.yaml
```

To run a test for connectivity with the detector, run the following command:
```
python test_data_ingest.py --mode both --config holoscan_config.yaml
```

# Stream proxy
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
# Profiling the pipeline with nsight systems


Profile the app:
```
nsys profile -t cuda,nvtx,osrt,python-gil -o report.nsys-rep -f true python3 pipeline.py --config holoscan_config.yaml
```
Useful: add `-d 30` to profile for 30 seconds.

# Simplon API Simulator

This is ran from a separate repo using private binaries.