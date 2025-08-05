## Artemis FQQN Redistribution issue
```mvn test```

- There are two test classes
 - ConsumerBeforeTopologyFailTest: If the FQQN already exists, the cluster fails to distribute messages initially to the broker where the consumer is present, if the consumer is created before the topology update (a cluster if formed).
 - ConsumerAfterTopologyPassTest: If the FQQN already exists, the cluster successfully sends the message to the consumer if it is created after the topology update (the cluster is formed).