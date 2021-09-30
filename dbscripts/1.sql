CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.tbl_unit
(
   id             serial,
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
   id                    serial,
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

commit;