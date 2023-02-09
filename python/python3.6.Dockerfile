FROM python:3.6
COPY python/* /src/
RUN pip3 install numpy psutil
ENTRYPOINT python3 /src/test.py