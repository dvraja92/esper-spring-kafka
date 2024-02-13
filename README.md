## Esper With Kafka Integration

This project provides an Esper example integrated with kafka.

## Getting Started

### Run the docker up command
```shell script
$ docker-compose up -d

# Start the Esper example applicaiton
$ mvn install
$ mvn spring-boot:run
```

### Curl Request for Statement and Instance management -

**_Add Statement_**
```
curl --location --request POST 'http://localhost:8090/api/statement/add-statement/A' \
--header 'Content-Type: application/json' \
--data-raw '{
    "name":"Test",
    "statement":"@KafkaOutputDefault select toLower(*) as word, getLengthOfString(*) as len from String"
}'
```

**_List Statement_** 
```
curl --location --request GET 'http://localhost:8090/api/statement/list-statement/A' \
--header 'Content-Type: application/json'
```

**_Initiailize Instance_**
```
curl --location --request GET 'http://localhost:8090/api/instance/start/A'
```

**_Destroy Instance_**
```
curl --location --request GET 'http://localhost:8090/api/instance/stop/A'
```

* Pending Items

    ** Statement Start/Stop/Update

    ** Output to Websocket.
