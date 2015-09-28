drop table if exists orders;

create table orders(
  ts bigint not null,
  symbol varchar not null,
  strategy varchar not null,
  price float,
  volume bigint,
  constraint my_pk primary key (ts, symbol, strategy)
) SALT_BUCKETS=1;
