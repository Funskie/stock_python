USE test;
CREATE TABLE `stocks` (
	`id` INT(11) NOT NULL AUTO_INCREMENT,
	`code` CHAR(4) NOT NULL COLLATE 'utf8mb4_unicode_ci',
	`name` VARCHAR(32) NOT NULL COLLATE 'utf8mb4_unicode_ci',
	`created_at` TIMESTAMP NOT NULL DEFAULT current_timestamp(),
	`updated_at` TIMESTAMP NOT NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
	`deleted_at` TIMESTAMP NULL DEFAULT NULL,
	PRIMARY KEY (`id`)
)
COLLATE='utf8mb4_unicode_ci'
ENGINE=InnoDB;

-- docker exec -i mariadb mysql -u root --password=root < test_sql.sql