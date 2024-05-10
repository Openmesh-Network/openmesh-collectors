import time

class Profiler:

    def __init__(self):
        self.start_times = {}
        self.elapsed_times = {}

    def start(self, key):
        """Start the profiler for the given key"""
        self.start_times[key] = time.time()

    def stop(self, key):
        """Stop the profiler for the given key and print the time since it was started"""
        if key not in self.start_times:
            raise ValueError(f"Profiler with key '{key}' not started")
        
        self.elapsed_times[key] = time.time() - self.start_times[key]
        del self.start_times[key]
        
        print(f"{key} took {self.elapsed_times[key]} seconds")

    def started(self, key):
        """Returns true if the profiler is currently running for the given key"""
        return key in self.start_times.keys()