
up:
	docker run -d --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.11-management

with-tls:
	docker run -d --name rabbitmq -v "$(pwd)"/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro -v "$(pwd)"/tls-gen/basic/result:/certs -p 5672:5672 -p 15672:15672 rabbitmq:3.11-management