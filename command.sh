docker run -v ./dqops/volume:/dqo/userhome --network lakehouse_net --name dqops -it  -m=4g -p 8889:8888 dqops/dqo run
docker run -v ./delta-spark:/delta-spark --name delta-spark -it -m=4g -p 8888:8888 deltaio/delta-docker
docker run -it -v ./spark:/spark --name spark-shell spark:3.5.6-scala2.12-java17-python3-ubuntu bash