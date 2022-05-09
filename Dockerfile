FROM ubuntu:20.04

WORKDIR /usr/src/scrape-test
COPY . .
RUN apt-get update && apt-get install -y python3-pip
RUN pip3 install --no-cache-dir -r requirements.txt
CMD ["python3", "src/add_to_s3.py"]