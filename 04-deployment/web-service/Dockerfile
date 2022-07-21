FROM python:3.9

WORKDIR /app

COPY ./* /app

RUN pip install -r requirements.txt

EXPOSE 9696

ENTRYPOINT [ "gunicorn", "--bind=0.0.0.0:9696", "predict:app" ]   





