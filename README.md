Flink Event Time Stream Processing

Course: CISC3010 - Distributed Systems

Brief project description--
This project is about how Apache Flink handles late and out-of-order data. I simulated a stream of sensor data to test the difference between Processing Time and Event Time, and how Watermarks can be used to handle network delays.

System architecture
I replicated the original Flink architecture locally using Docker. The system has a Python Client sending data to a Flink cluster running inside Docker, which contains a JobManager (coordinator) and a TaskManager (worker).

Requirements
To run this project you need:

Java 25 (OpenJDK)
Python 3.9
PyFlink 1.20.1
To install Python dependencies: pip install apache-flink==1.20.1

After installing the dependencies, just run the Python script:
python projeto.py

Experiment:
The code simulates events arriving at timestamps 1000, 1500, 2000, and a late event at 1200. To force the window to close, I injected a "future" event at 10000.

To test the trade-off, you can change the watermark_seconds variable inside projeto.py:

Change it to 0 to see the strict behavior (low latency, but risks losing late data).
Change it to 10 to see the tolerant behavior (higher latency, but keeps all data).
Results

You can find the terminal screenshots for both configurations (0s and 10s) in the project files.# flink_project
