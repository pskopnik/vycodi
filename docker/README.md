#Docker

##Build

From root folder of project

	sudo docker build -t seoester/vycodi .

##Dependencies

	sudo docker pull redis

##Start

	sudo docker run --name redis-db -d redis
	sudo docker run --name vycodi-host --link redis-db:redis -d -P seoester/vycodi

Get Port

	sudo docker port vycodi-host

View Logs

	sudo docker logs vycodi-host

Stop

	sudo docker stop vycodi-host
	sudo docker stop redis-db
