FROM apache/airflow:2.10.5-python3.11

USER root

# Install OpenJDK 11 dependencies
# RUN apt-get update && \
#     apt-get install -y --no-install-recommends \
#     openjdk-11-jdk 

RUN apt update
RUN apt install -y default-jdk 



# ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
# ENV PATH="$JAVA_HOME/bin:$PATH"

USER airflow


COPY ./requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
