-- Database per service: each service owns its data and no other service may
-- touch it. Here they share one Postgres SERVER for convenience; in
-- production they are usually separate instances.
CREATE DATABASE orders;
CREATE DATABASE payments;
-- Used only by the integration tests, so they never touch development data.
CREATE DATABASE orders_test;
