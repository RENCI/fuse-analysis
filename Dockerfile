FROM python:3.9-buster

RUN apt-get update
RUN apt-get -y install apt-transport-https ca-certificates curl gnupg2 software-properties-common

EXPOSE 8000

COPY ./requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt
COPY . /app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]