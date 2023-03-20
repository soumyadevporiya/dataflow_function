FROM python:3.10
WORKDIR ./
COPY ./requirement.txt ./requirement.txt
RUN pip install -r requirement.txt
COPY ./pipe.py ./pipe.py
CMD ["python3","./pipe.py"]
