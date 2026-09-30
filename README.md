# learn-pub-sub-starter (Peril)

This is the starter code used in Boot.dev's [Learn Pub/Sub](https://learn.boot.dev/learn-pub-sub) course.

Command for running application in docker:
```
docker run -it --rm --name rabbitmq -p 5672:5672 -p 15672:15672 rabbitmq:3.13-management
```
This will download and run the rabbitmq server and ManagementUI until you kill it with ctlr + c.
MangementUI can be accessed through http://localhost:15672 using username/password `guest`.

Run RabbitMQ server with Docker:
`./rabbit.sh start`

Once that is going run the server with:
`go run ./cmd/server`
