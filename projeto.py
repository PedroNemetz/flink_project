import time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.common import WatermarkStrategy, Duration
from pyflink.common.time import Time
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream.functions import AllWindowFunction

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

def generate_stream():
    yield ("sensor1", 1000, 20.0)
    yield ("sensor1", 1500, 22.0)
    yield ("sensor1", 2000, 24.0)
    time.sleep(2) 
    yield ("sensor1", 10000, 99.9) 
    yield ("sensor1", 1200, 21.0)

stream = env.from_collection(generate_stream())

class SensorTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        return value[1]

class PrintWindow(AllWindowFunction):
    def apply(self, window, inputs):
        window_data = list(inputs)
        return [f"Window {window.start} to {window.end}: {window_data}"]

watermark_seconds = 0

print(f"--- EVENT TIME RESULT (Watermark: {watermark_seconds}s) ---")
event_stream = stream.assign_timestamps_and_watermarks(
    WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(watermark_seconds))
    .with_timestamp_assigner(SensorTimestampAssigner())
)
event_stream.window_all(TumblingEventTimeWindows.of(Time.seconds(2))).apply(PrintWindow()).print()

env.execute("Flink Event Time Project")
