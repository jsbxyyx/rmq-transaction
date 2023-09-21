# rmq transaction
Sending message consistency based on RocketMQ

# how to use
```
<repositories>
    <repository>
        <id>jitpack.io</id>
        <url>https://jitpack.io</url>
    </repository>
</repositories>

<dependency>
    <groupId>com.github.jsbxyyx</groupId>
    <artifactId>rmq-transaction</artifactId>
    <version>main-SNAPSHOT</version>
</dependency>
```

# samples
```
@Transactional(rollbackFor = Exception.class)
public int addOrder(Order order) {
    // ...
    RMQHelper.syncSend(rocketMQTemplate, "order:add", new GenericMessage<>(payload, headers));
    return order.getId();
}
```