drop table if exists quotes;

create table quotes(
  name varchar not null,
  price float,
  symbol varchar,
  ts bigint not null,
  type varchar,
  volume bigint,
  constraint my_pk primary key (name, ts)
);
