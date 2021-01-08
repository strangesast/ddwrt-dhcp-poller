from python:slim

run apt-get update && \
    apt-get install -y openssh-client && \
    rm -rf /var/lib/apt/lists/*

workdir /usr/src/app

copy requirements.txt .
run python3 -m pip install -r requirements.txt

copy main.py .

cmd ["python3", "main.py"]
