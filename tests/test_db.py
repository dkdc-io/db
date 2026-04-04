"""Integration tests for the dkdc-db Python bindings."""

import pytest

from dkdc_db import Db, run_cli


# ---------------------------------------------------------------------------
# 1. Import and basic API availability
# ---------------------------------------------------------------------------


class TestImportsAndApi:
    def test_import_db_class(self):
        assert Db is not None

    def test_import_run_cli(self):
        assert callable(run_cli)

    def test_db_constructor_default(self):
        """Db() uses default localhost URL."""
        client = Db()
        assert client is not None

    def test_db_constructor_custom_url(self):
        """Db(url) accepts a custom URL."""
        client = Db("http://127.0.0.1:9999")
        assert client is not None

    def test_api_methods_exist(self, db):
        """All expected methods are present on the Db instance."""
        for method in [
            "create_db",
            "drop_db",
            "list_dbs",
            "execute",
            "query",
            "query_oltp",
            "list_tables",
            "table_schema",
            "health",
        ]:
            assert hasattr(db, method), f"Missing method: {method}"
            assert callable(getattr(db, method))


# ---------------------------------------------------------------------------
# 2. Creating and dropping databases
# ---------------------------------------------------------------------------


class TestDatabaseLifecycle:
    def test_health_check(self, db):
        assert db.health() is True

    def test_create_and_list_db(self, db):
        import uuid

        name = f"test_{uuid.uuid4().hex[:8]}"
        db.create_db(name)
        try:
            dbs = db.list_dbs()
            assert name in dbs
        finally:
            db.drop_db(name)

    def test_drop_db(self, db):
        import uuid

        name = f"test_{uuid.uuid4().hex[:8]}"
        db.create_db(name)
        db.drop_db(name)
        dbs = db.list_dbs()
        assert name not in dbs

    def test_create_multiple_dbs(self, db):
        import uuid

        names = [f"test_{uuid.uuid4().hex[:8]}" for _ in range(3)]
        try:
            for n in names:
                db.create_db(n)
            dbs = db.list_dbs()
            for n in names:
                assert n in dbs
        finally:
            for n in names:
                try:
                    db.drop_db(n)
                except Exception:
                    pass


# ---------------------------------------------------------------------------
# 3. Execute (write) operations
# ---------------------------------------------------------------------------


class TestExecute:
    def test_create_table(self, test_db):
        db, name = test_db
        db.execute(name, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
        tables = db.list_tables(name)
        assert "users" in tables

    def test_insert_returns_affected(self, test_db):
        db, name = test_db
        db.execute(name, "CREATE TABLE t (id INTEGER, val TEXT)")
        affected = db.execute(name, "INSERT INTO t VALUES (1, 'hello')")
        assert affected == 1

    def test_multiple_inserts(self, test_db):
        db, name = test_db
        db.execute(name, "CREATE TABLE t (id INTEGER)")
        for i in range(5):
            db.execute(name, f"INSERT INTO t VALUES ({i})")
        resp = db.query_oltp(name, "SELECT count(*) FROM t")
        assert resp["rows"][0][0] == 5

    def test_update_returns_affected(self, test_db):
        db, name = test_db
        db.execute(name, "CREATE TABLE t (id INTEGER, val TEXT)")
        db.execute(name, "INSERT INTO t VALUES (1, 'a')")
        db.execute(name, "INSERT INTO t VALUES (2, 'b')")
        affected = db.execute(name, "UPDATE t SET val = 'x' WHERE id = 1")
        assert affected == 1

    def test_delete_returns_affected(self, test_db):
        db, name = test_db
        db.execute(name, "CREATE TABLE t (id INTEGER)")
        db.execute(name, "INSERT INTO t VALUES (1)")
        db.execute(name, "INSERT INTO t VALUES (2)")
        affected = db.execute(name, "DELETE FROM t WHERE id = 1")
        assert affected == 1

    def test_list_tables(self, test_db):
        db, name = test_db
        assert db.list_tables(name) == []
        db.execute(name, "CREATE TABLE foo (id INTEGER)")
        db.execute(name, "CREATE TABLE bar (id INTEGER)")
        tables = db.list_tables(name)
        assert len(tables) == 2
        assert set(tables) == {"foo", "bar"}

    def test_table_schema(self, test_db):
        db, name = test_db
        db.execute(
            name,
            "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
        )
        schema = db.table_schema(name, "users")
        assert "columns" in schema
        assert "rows" in schema
        assert len(schema["rows"]) == 3


# ---------------------------------------------------------------------------
# 4. Query (analytical) and query_oltp (fast path)
# ---------------------------------------------------------------------------


class TestQuery:
    def test_query_via_datafusion(self, test_db):
        """query() routes through DataFusion with qualified table names."""
        db, name = test_db
        db.execute(name, "CREATE TABLE users (id INTEGER, name TEXT)")
        db.execute(name, "INSERT INTO users VALUES (1, 'alice')")
        db.execute(name, "INSERT INTO users VALUES (2, 'bob')")

        resp = db.query(f"SELECT * FROM {name}.public.users ORDER BY id")
        assert "columns" in resp
        assert "rows" in resp
        assert len(resp["columns"]) == 2
        assert resp["columns"][0]["name"] == "id"
        assert resp["columns"][1]["name"] == "name"
        assert len(resp["rows"]) == 2
        assert resp["rows"][0][0] == 1
        assert resp["rows"][0][1] == "alice"
        assert resp["rows"][1][0] == 2
        assert resp["rows"][1][1] == "bob"

    def test_query_oltp_path(self, test_db):
        """query_oltp() goes through turso fast path."""
        db, name = test_db
        db.execute(name, "CREATE TABLE t (id INTEGER, val TEXT)")
        db.execute(name, "INSERT INTO t VALUES (1, 'hello')")

        resp = db.query_oltp(name, "SELECT * FROM t")
        assert len(resp["rows"]) == 1
        assert resp["rows"][0][0] == 1
        assert resp["rows"][0][1] == "hello"

    def test_query_response_structure(self, test_db):
        """Query response has correct dict structure."""
        db, name = test_db
        db.execute(name, "CREATE TABLE t (id INTEGER, name TEXT, score REAL)")
        db.execute(name, "INSERT INTO t VALUES (1, 'alice', 9.5)")

        resp = db.query_oltp(name, "SELECT * FROM t")
        assert isinstance(resp, dict)
        assert isinstance(resp["columns"], list)
        assert isinstance(resp["rows"], list)
        assert isinstance(resp["columns"][0], dict)
        assert "name" in resp["columns"][0]
        assert "type" in resp["columns"][0]
        assert isinstance(resp["rows"][0], list)

    def test_aggregation_query(self, test_db):
        """Aggregations work through DataFusion."""
        db, name = test_db
        db.execute(name, "CREATE TABLE sales (region TEXT, amount REAL)")
        for i in range(20):
            region = "north" if i % 2 == 0 else "south"
            db.execute(name, f"INSERT INTO sales VALUES ('{region}', {float(i)})")

        resp = db.query(
            f"SELECT region, count(*) as cnt FROM {name}.public.sales GROUP BY region ORDER BY region"
        )
        assert len(resp["rows"]) == 2
        assert resp["rows"][0][0] == "north"
        assert resp["rows"][0][1] == 10
        assert resp["rows"][1][0] == "south"
        assert resp["rows"][1][1] == 10

    def test_empty_table_query(self, test_db):
        db, name = test_db
        db.execute(name, "CREATE TABLE t (id INTEGER)")
        resp = db.query_oltp(name, "SELECT * FROM t")
        assert resp["rows"] == []

    def test_null_values(self, test_db):
        """NULL values are returned as None in Python."""
        db, name = test_db
        db.execute(name, "CREATE TABLE t (id INTEGER, val TEXT)")
        db.execute(name, "INSERT INTO t VALUES (1, NULL)")

        resp = db.query_oltp(name, "SELECT * FROM t")
        assert resp["rows"][0][1] is None


# ---------------------------------------------------------------------------
# 5. Error handling
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_invalid_sql_execute(self, test_db):
        db, name = test_db
        with pytest.raises(RuntimeError):
            db.execute(name, "NOT VALID SQL AT ALL")

    def test_invalid_sql_query(self, db):
        with pytest.raises(RuntimeError):
            db.query("NOT VALID SQL AT ALL")

    def test_invalid_sql_query_oltp(self, test_db):
        db, name = test_db
        with pytest.raises(RuntimeError):
            db.query_oltp(name, "NOT VALID SQL AT ALL")

    def test_execute_nonexistent_db(self, db):
        with pytest.raises(RuntimeError):
            db.execute("does_not_exist_xyz", "SELECT 1")

    def test_query_oltp_nonexistent_db(self, db):
        with pytest.raises(RuntimeError):
            db.query_oltp("does_not_exist_xyz", "SELECT 1")

    def test_list_tables_nonexistent_db(self, db):
        with pytest.raises(RuntimeError):
            db.list_tables("does_not_exist_xyz")

    def test_table_schema_nonexistent_table(self, test_db):
        """Querying schema for a nonexistent table returns empty rows."""
        db, name = test_db
        resp = db.table_schema(name, "no_such_table")
        assert resp["rows"] == []

    def test_drop_nonexistent_db(self, db):
        with pytest.raises(RuntimeError):
            db.drop_db("does_not_exist_xyz")

    def test_connection_refused(self):
        """Connecting to a port with no server raises RuntimeError."""
        client = Db("http://127.0.0.1:19999")
        with pytest.raises(RuntimeError):
            client.health()

    def test_write_through_query_rejected(self, test_db):
        """Writes through the query (read) path should be rejected."""
        db, name = test_db
        db.execute(name, "CREATE TABLE t (id INTEGER)")
        with pytest.raises(RuntimeError):
            db.query(f"INSERT INTO {name}.public.t VALUES (1)")


# ---------------------------------------------------------------------------
# 6. Cross-database queries
# ---------------------------------------------------------------------------


class TestCrossDatabase:
    def test_cross_db_join(self, db):
        """Cross-database JOIN via DataFusion qualified names."""
        import uuid

        db1 = f"test_{uuid.uuid4().hex[:8]}"
        db2 = f"test_{uuid.uuid4().hex[:8]}"
        try:
            db.create_db(db1)
            db.create_db(db2)

            db.execute(db1, "CREATE TABLE employees (id INTEGER, name TEXT)")
            db.execute(db1, "INSERT INTO employees VALUES (1, 'alice')")
            db.execute(db1, "INSERT INTO employees VALUES (2, 'bob')")

            db.execute(
                db2,
                "CREATE TABLE orders (id INTEGER, emp_id INTEGER, amount REAL)",
            )
            db.execute(db2, "INSERT INTO orders VALUES (1, 1, 100.0)")
            db.execute(db2, "INSERT INTO orders VALUES (2, 2, 200.0)")

            resp = db.query(
                f"SELECT e.name, o.amount "
                f"FROM {db1}.public.employees e "
                f"JOIN {db2}.public.orders o ON e.id = o.emp_id "
                f"ORDER BY e.name"
            )
            assert len(resp["rows"]) == 2
            assert resp["rows"][0][0] == "alice"
            assert resp["rows"][0][1] == 100.0
            assert resp["rows"][1][0] == "bob"
            assert resp["rows"][1][1] == 200.0
        finally:
            for n in [db1, db2]:
                try:
                    db.drop_db(n)
                except Exception:
                    pass

    def test_cross_db_union(self, db):
        """Cross-database UNION via DataFusion."""
        import uuid

        db1 = f"test_{uuid.uuid4().hex[:8]}"
        db2 = f"test_{uuid.uuid4().hex[:8]}"
        try:
            db.create_db(db1)
            db.create_db(db2)

            db.execute(db1, "CREATE TABLE items (name TEXT)")
            db.execute(db1, "INSERT INTO items VALUES ('apple')")

            db.execute(db2, "CREATE TABLE items (name TEXT)")
            db.execute(db2, "INSERT INTO items VALUES ('banana')")

            resp = db.query(
                f"SELECT name FROM {db1}.public.items "
                f"UNION ALL "
                f"SELECT name FROM {db2}.public.items "
                f"ORDER BY name"
            )
            assert len(resp["rows"]) == 2
            assert resp["rows"][0][0] == "apple"
            assert resp["rows"][1][0] == "banana"
        finally:
            for n in [db1, db2]:
                try:
                    db.drop_db(n)
                except Exception:
                    pass
