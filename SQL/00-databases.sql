-- Airflow keeps its own bookkeeping -- users, connections, DAG and task run
-- history -- in a metadata database. It used to share stockdw, which put 42
-- Airflow tables in the same schema as the nine warehouse tables: pg_dump of
-- the warehouse carried Airflow's state with it, `airflow db reset` pointed at
-- the schema holding the facts, and \dt was unreadable.
--
-- Runs only on a first container init. An existing volume needs the same
-- statement applied by hand; see REHAB.md.
SELECT 'CREATE DATABASE airflow OWNER ' || quote_ident(current_user)
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow')
\gexec
