FROM python:3.7

EXPOSE 8000

COPY . /app

RUN pip install -r /app/requirements.txt

RUN apt-get update
RUN apt-get -y install apt-transport-https ca-certificates curl gnupg2 software-properties-common
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | apt-key add -
RUN add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]