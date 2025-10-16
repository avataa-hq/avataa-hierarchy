## Requirements

```
protobuf==3.20.0
confluent-kafka==2.0.2
```
## Enable Kafka feature

To enable Kafka feature add into env KAFKA_TURN_ON=True and KAFKA_URL = kafka_host:port

## Add the ability to get messages with new proto message class

### Step 1
Add new message class into protobuf/obj.proto:

- One for unit of class instance 
- Second as list of units


### Step 2
- If you haven’t installed the compiler, download the package and follow the instructions in 
  the README.
- Now run the compiler, specifying the source directory (where your application’s source code lives – the current directory is used if you don’t provide a value), the destination directory (where you want the generated code to go; often the same as $SRC_DIR), and the path to your .proto. In this case, you…:

```
protoc -I=$SRC_DIR --python_out=$DST_DIR $SRC_DIR/addressbook.proto
```



### Step 3
In kafka_config/config.py add information about your new class into the dict KAFKA_PROTOBUF_DESERIALIZERS:
Example:
```
KAFKA_PROTOBUF_DESERIALIZERS = {
    'MO': inventory_instances_pb2.ListMO,
    'TMO': inventory_instances_pb2.ListTMO,
    'TPRM': inventory_instances_pb2.ListTPRM,
    'PRM': inventory_instances_pb2.ListPRM
}
```

- 'MO' - class name
- inventory_instances_pb2.ListMO - proto message class for list of class instances (step 2)
