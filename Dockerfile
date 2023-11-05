FROM couchbase/centos7-systemd:latest
RUN yum install -y wget
RUN wget https://dl.google.com/go/go1.18.linux-amd64.tar.gz
RUN tar -xzvf go1.18.linux-amd64.tar.gz -C /usr/local/
RUN echo export PATH=$PATH:/usr/local/go/bin
RUN source /etc/profile