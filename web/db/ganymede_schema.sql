DROP SCHEMA IF EXISTS `ganymede`;
CREATE SCHEMA `ganymede`;
use `ganymede`;

DROP TABLE IF EXISTS `ganymede`.`geo_config`;
CREATE TABLE IF NOT EXISTS `ganymede`.`geo_config` (
  id int unsigned auto_increment primary key,
  short_name varchar(25) not null, -- northamerica, southamerica, europe, etc.
  long_name varchar(255) not null, -- North America, South America, Europe, etc.
  db_schema varchar(255) not null default 'schema_name', -- the schema to dump from
  db_host varchar(255) not null,
  db_user varchar(255) not null,
  db_pass text not null, -- encrypted
  enabled tinyint not null default 0 -- 0 = disabled, 1 = enabled
) Engine=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `ganymede`.`geo_release`;
CREATE TABLE IF NOT EXISTS `ganymede`.`geo_release` (
  id int unsigned auto_increment primary key,
  cc_release varchar(255) not null, -- the release name
  db_tables text not null
) Engine=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `ganymede`.`agent`;
CREATE TABLE IF NOT EXISTS `ganymede`.`agent` (
  id int unsigned auto_increment primary key,
  uid varchar(255) not null, -- UUID
  name varchar(255) not null, -- display-friendly name
  enabled tinyint not null default 1, -- 0 = disabled, 1 = enabled
  active tinyint not null default 1 -- 0 = inactive, 1 = active
) Engine=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `ganymede`.`log`;
CREATE TABLE IF NOT EXISTS `ganymede`.`log` (
  id int unsigned auto_increment primary key,
  transid varchar(255) not null, -- UUID
  geo int unsigned not null,
  tstamp datetime not null,
  status varchar(255) not null, -- SUCCESS, ERROR
  stage varchar(255) not null, -- DUMP, TRANSFER, LOAD, TRANSFORM, etc.
  message text not null,
  nonce text -- one-time passphrase for encryption/decryption
) Engine=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `ganymede`.`assignment`;
CREATE TABLE IF NOT EXISTS `ganymede`.`assignment` (
  id int unsigned auto_increment primary key,
  agent_id int unsigned not null,
  geo_config_id int unsigned not null,
  geo_release_id int unsigned not null
) Engine=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `ganymede`.`upload`;
CREATE TABLE IF NOT EXISTS `ganymede`.`upload` (
  id int unsigned auto_increment primary key,
  transid varchar(255) not null,
  filename text not null -- we use text in case of large path+file names
) Engine=InnoDB DEFAULT CHARSET=utf8;

-- Test data
-- Create GEOs
INSERT INTO `ganymede`.`geo_config` (id, short_name, long_name, db_host, db_schema, db_user, db_pass, enabled) VALUES (1, 'NA', 'North America', 'x.x.x.x', 'schema1', 'gagent','gagenttest01', 1);
INSERT INTO `ganymede`.`geo_config` (id, short_name, long_name, db_host, db_schema, db_user, db_pass, enabled) VALUES (2, 'SA', 'South America', 'x.x.x.x', 'schema1', 'gagent','gagenttest01', 1);
INSERT INTO `ganymede`.`geo_config` (id, short_name, long_name, db_host, db_schema, db_user, db_pass, enabled) VALUES (3, 'EU', 'Europe', 'x.x.x.x', 'schema1', 'gagent','gagenttest01', 1);
INSERT INTO `ganymede`.`geo_config` (id, short_name, long_name, db_host, db_schema, db_user, db_pass, enabled) VALUES (4, 'MEA', 'Africa', 'x.x.x.x', 'schema1', 'gagent','gagenttest01', 1);
INSERT INTO `ganymede`.`geo_config` (id, short_name, long_name, db_host, db_schema, db_user, db_pass, enabled) VALUES (5, 'AU', 'Australia', 'x.x.x.x', 'schema1', 'noone','nothing', 1);

-- Create Agents
INSERT INTO `ganymede`.`agent` (id, uid, name, enabled, active) VALUES (1, 'a001', 'NA Agent', 1, 1);
INSERT INTO `ganymede`.`agent` (id ,uid, name, enabled, active) VALUES (2, 'a002', 'SA Agent', 1, 1);
INSERT INTO `ganymede`.`agent` (id, uid, name, enabled, active) VALUES (3, 'a003', 'EU Agent', 1, 1);
INSERT INTO `ganymede`.`agent` (id, uid, name, enabled, active) VALUES (4, 'a004', 'MEA Agent', 1, 1);
INSERT INTO `ganymede`.`agent` (id, uid, name, enabled, active) VALUES (5, 'a005', 'AU Agent', 1, 1);

-- Set up releases
INSERT INTO `ganymede`.`geo_release` (id, cc_release, db_tables) VALUES (1, 'release_1', 'list_of_tables');
INSERT INTO `ganymede`.`geo_release` (id, cc_release, db_tables) VALUES (2, 'release_2', 'list_of_tables');

-- Assign an GEO Release to a GEO Config to an Agent all in one fell swoop
INSERT INTO `ganymede`.`assignment` (agent_id, geo_config_id, geo_release_id) VALUES (1, 1, 2);
INSERT INTO `ganymede`.`assignment` (agent_id, geo_config_id, geo_release_id) VALUES (2, 2, 1);
INSERT INTO `ganymede`.`assignment` (agent_id, geo_config_id, geo_release_id) VALUES (3, 3, 1);
INSERT INTO `ganymede`.`assignment` (agent_id, geo_config_id, geo_release_id) VALUES (4, 4, 1);
INSERT INTO `ganymede`.`assignment` (agent_id, geo_config_id, geo_release_id) VALUES (5, 5, 1);
