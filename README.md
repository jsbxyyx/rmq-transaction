# rmq transaction
Sending message consistency based on RocketMQ

[![rmq-transaction](https://img.shields.io/maven-central/v/io.github.jsbxyyx/rmq-transaction?label=rmq-transaction)](https://central.sonatype.com/artifact/io.github.jsbxyyx/rmq-transaction)

# how to use
```
<dependency>
    <groupId>io.github.jsbxyyx</groupId>
    <artifactId>rmq-transaction</artifactId>
    <version>${rmq-transaction.version}</version>
</dependency>

CREATE TABLE `tb_mq_msg` (
  `id` BIGINT NOT NULL,
  `status` VARCHAR(20) NOT NULL COMMENT '事件状态(待发布NEW)',
  `mq_template_name` VARCHAR(1000) NOT NULL,
  `mq_destination` VARCHAR(1000) NOT NULL,
  `mq_timeout` BIGINT NOT NULL,
  `mq_delay` VARCHAR(255) NOT NULL,
  `payload` TEXT NOT NULL,
  `retry_times` INT NOT NULL,
  `gmt_create` DATETIME NOT NULL,
  `gmt_modified` DATETIME NOT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_status` (`status`)
) ENGINE=INNODB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
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