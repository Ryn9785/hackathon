-- GT06 ingest challenge - canonical schema
-- Your server MUST write into these exact tables (database name: hackathon).
-- Scoring verifies rows in these tables byte-for-byte against generated traffic.

CREATE DATABASE IF NOT EXISTS hackathon;
USE hackathon;

-- One row per unique device (IMEI). Created on first login of that IMEI.
CREATE TABLE IF NOT EXISTS devices (
    id          INT UNSIGNED    NOT NULL AUTO_INCREMENT,
    imei        CHAR(15)        NOT NULL,
    created_at  TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id),
    UNIQUE KEY uk_imei (imei)
) ENGINE=InnoDB;

-- One row per accepted position packet (deduplicated on device_id + serial).
CREATE TABLE IF NOT EXISTS positions (
    id          BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    device_id   INT UNSIGNED    NOT NULL,
    serial      SMALLINT UNSIGNED NOT NULL,      -- information serial number from the packet
    fix_time    DATETIME        NOT NULL,        -- GPS datetime from packet, UTC
    valid       TINYINT(1)      NOT NULL,        -- GPS fix valid bit
    latitude    DOUBLE          NOT NULL,        -- signed degrees (south negative)
    longitude   DOUBLE          NOT NULL,        -- signed degrees (west negative)
    speed       SMALLINT UNSIGNED NOT NULL,      -- km/h, 0-255
    course      SMALLINT UNSIGNED NOT NULL,      -- degrees, 0-359
    satellites  TINYINT UNSIGNED NOT NULL,       -- 0-15
    server_time TIMESTAMP(3)    NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    UNIQUE KEY uk_device_serial (device_id, serial),
    KEY idx_device_time (device_id, fix_time),
    CONSTRAINT fk_pos_device FOREIGN KEY (device_id) REFERENCES devices (id)
) ENGINE=InnoDB;
