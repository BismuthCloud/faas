#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "template1" <<-EOSQL
    REVOKE ALL ON DATABASE template1 FROM public;
    REVOKE ALL ON SCHEMA public FROM public;
    GRANT ALL ON SCHEMA public TO postgres;

    REVOKE ALL ON pg_user FROM public;
    REVOKE ALL ON pg_roles FROM public;
    REVOKE ALL ON pg_group FROM public;
    REVOKE ALL ON pg_authid FROM public;
    REVOKE ALL ON pg_auth_members FROM public;

    REVOKE ALL ON pg_database FROM public;
    REVOKE ALL ON pg_tablespace FROM public;
    REVOKE ALL ON pg_settings FROM public;
EOSQL
