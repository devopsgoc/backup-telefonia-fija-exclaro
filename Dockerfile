FROM ubuntu:noble
ENV TZ=America/Santiago
RUN apt update && apt -y upgrade && apt install -y python3 python3-pip python3-venv && DEBIAN_FRONTEND=noninteractive apt-get -y install tzdata && \
    ln -fs /usr/share/zoneinfo/$TZ /etc/localtime && \
    dpkg-reconfigure -f noninteractive tzdata
WORKDIR /app
RUN python3 -m venv venv && /app/venv/bin/pip install --upgrade pip
COPY requirements.txt /app/
RUN python3 -m venv venv && /app/venv/bin/pip install -r requirements.txt
COPY *py /app/
#COPY app.py /app/
CMD ["/app/venv/bin/python", "getData.py"]