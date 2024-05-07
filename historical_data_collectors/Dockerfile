FROM python:3.9.4-slim

WORKDIR /src

COPY . ./historical_data_collectors

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r historical_data_collectors/requirements.txt

# Run script when the container launches
ENTRYPOINT ["/bin/bash"]
