-- Run once before starting control planes built with partition IDs.
ALTER TABLE `partition` ADD COLUMN partition_id VARCHAR(32) NULL;
UPDATE `partition` SET partition_id = REPLACE(UUID(), '-', '') WHERE partition_id IS NULL;
ALTER TABLE `partition` MODIFY partition_id VARCHAR(32) NOT NULL,
    ADD UNIQUE KEY partition_id (partition_id);

-- Existing data-node rows remain unlabelled until a routed write or copy.
-- Data nodes add their nullable SQLite partition_id column on startup.
