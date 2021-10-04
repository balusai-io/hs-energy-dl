CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.tbl_unit
(
   id             serial NOT NULL  PRIMARY KEY,
   unit_short     varchar(30),
   display_name   varchar(100)   NOT NULL,
   description    varchar(500),
   date_created   timestamp      NOT NULL,
   created_by     varchar(100)   NOT NULL,
   date_updated   timestamp      NOT NULL,
   updated_by     varchar(100)   NOT NULL,
   date_approved  timestamp      NOT NULL,
   approved_by    varchar(100)   NOT NULL
);

CREATE TABLE IF NOT EXISTS metadata.tbl_energy_product
(
   id                    serial NOT NULL  PRIMARY KEY,
   energy_product        varchar(100)   NOT NULL,
   display_name          varchar(100)   NOT NULL,
   description           varchar(500),
   country_region_group  integer,
   date_created          timestamp      NOT NULL,
   created_by            varchar(100)   NOT NULL,
   date_updated          timestamp      NOT NULL,
   updated_by            varchar(100)   NOT NULL,
   date_approved         timestamp      NOT NULL,
   approved_by           varchar(100)   NOT NULL,
   is_api                boolean        NOT NULL
);

CREATE SCHEMA IF NOT EXISTS collections;

CREATE TABLE IF NOT EXISTS collections.tbl_cnlopb_fields
(
	ID serial NOT NULL  PRIMARY KEY,
	field_name varchar(30) unique NOT NULL,
	date_created timestamp without time zone NOT NULL DEFAULT
	(current_timestamp AT TIME ZONE 'UTC')
);

create table IF NOT EXISTS collections.tbl_cnlopb_wells
(
    ID serial   NOT NULL  PRIMARY KEY,
	field_id    integer   Not Null      references collections.tbl_cnlopb_fields(ID) ,
	well_name   varchar(30) unique NOT NULL,
	date_created timestamp without time zone NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);

create table IF NOT EXISTS collections.tbl_cnlopb_production
(
    ID serial NOT NULL  PRIMARY KEY,
	well_id integer Not Null references collections.tbl_cnlopb_wells(ID),
	energy_product_id integer Not Null references metadata.tbl_energy_product(ID),
	unit_of_measure_id integer Not Null references metadata.tbl_unit(ID) ,
	month date,
	value double precision,
	date_created timestamp without time zone NOT NULL DEFAULT (current_timestamp AT TIME ZONE 'UTC')
);

commit;