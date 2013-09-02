=======
pyserver
========

## PreRequest

### RabbitMQ Installation
a generic python-based server framework
1.Enable EPEL
su -c 'rpm -Uvh http://download.fedoraproject.org/pub/epel/6/i386/epel-release-6-8.noarch.rpm'

2.Install (or update) Erlang
sudo yum install erlang

3.Install the RabbitMQ Server
```downloading rpm package
wget -c http://www.rabbitmq.com/releases/rabbitmq-server/v3.0.4/rabbitmq-server-3.0.4-1.noarch.rpm
```install
sudo rpm --import http://www.rabbitmq.com/rabbitmq-signing-key-public.asc
sudo yum install rabbitmq-server-3.0.4-1.noarch.rpm

### Run RabbitMQ Server
sudo chkconfig rabbitmq-server on
sudo /sbin/service rabbitmq-server start

### Management
sudo rabbitmq-plugins enable rabbitmq_management
http://localhost:15672



### Pika Installation
sudo yum install python-pip.noarch
sudo pip-python install pika
