FROM registry.fedoraproject.org/f33/python3
USER root
WORKDIR /opt/spark
RUN yum -y update && \
    yum -y install java-11-openjdk-devel && \
    yum clean all
COPY requirements.txt ./
RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
EXPOSE 5000
CMD ["python", "app.py"]