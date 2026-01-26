FROM jupyter/pyspark-notebook:spark-3.5.0

USER root

# Install system dependencies if needed (e.g., for pyarrow)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER ${NB_UID}

# Copy requirements
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt