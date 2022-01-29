#Dockerfile
FROM python:3.8

COPY deploy/requirements.txt /
RUN pip install --trusted-host pypi.python.org -r requirements.txt

COPY . .

CMD ["python", "src/main.py"]
