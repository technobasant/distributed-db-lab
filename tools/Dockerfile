# The benchmark client runs inside the Docker network, next to the databases.
# On the machine this lab was measured on, host-to-container traffic passes
# through a network-filtering system extension; keeping the client in the VM
# takes that out of every latency figure.
FROM python:3.12-slim
RUN pip install --no-cache-dir "psycopg[binary]>=3.2" "numpy>=2.0" "httpx>=0.27"
WORKDIR /lab
