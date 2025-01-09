FROM jupyter/pyspark-notebook:latest

RUN apt-get update && apt-get install -y python3 \
python3-pip && apt-get clean \
&& rm -rf /var/lib/apt/lists/*

# Создание рабочей директории
RUN pip3 install pyspark

COPY main.py /home/spark
WORKDIR /home/spark

CMD ["python", "main.py"]
EXPOSE 4040