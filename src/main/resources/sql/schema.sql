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
