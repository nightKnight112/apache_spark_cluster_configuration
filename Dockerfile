FROM registry.fedoraproject.org/f33/python3
USER root
WORKDIR /usr/src/app
RUN yum -y update && \
    yum -y install java-11-openjdk-devel && \
    yum clean all
WORKDIR /usr/src/app
COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p parquet_folder && chmod 777 -R parquet_folder
EXPOSE 5000
CMD ["python", "app.py"]