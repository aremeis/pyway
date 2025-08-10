import pytest
import os
from pyway.migrate import Migrate
from pyway.settings import ConfigFile
from pyway.dbms.database import factory


@pytest.fixture
def sqlite_connect_async(autouse: bool = True):
    """Setup SQLite database for async migration testing"""
    # Delete any existing databases
    try:
        os.remove("./unittest-async-migrate.sqlite")
    except Exception:
        pass

    args = ConfigFile()
    args.database_type = "sqlite"
    args.database_name = "./unittest-async-migrate.sqlite"
    args.database_table = "pyway"

    return factory(args.database_type)(args)


@pytest.mark.asyncio
@pytest.mark.migrate_test
@pytest.mark.sqlite_test
@pytest.mark.python_test
async def test_async_python_migration_execution(sqlite_connect_async) -> None:
    """Test that async Python migrations execute correctly"""
    config = ConfigFile()
    config.database_type = "sqlite"
    config.database_name = './unittest-async-migrate.sqlite'
    config.database_table = 'pyway'
    config.async_mode = True

    import tempfile

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create an async Python migration
        async_migration_content = '''async def migrate(connection):
    """Async Python migration test"""
    # Simulate async operation
    import asyncio
    await asyncio.sleep(0.01)

    cursor = connection.cursor()
    cursor.execute("CREATE TABLE async_test (id INTEGER PRIMARY KEY, data TEXT)")
    cursor.execute("INSERT INTO async_test (data) VALUES ('async_data')")
'''

        migration_file = os.path.join(temp_dir, 'V01_01__async_test.py')
        with open(migration_file, 'w') as f:
            f.write(async_migration_content)

        config.database_migration_dir = temp_dir

        # Run the async migration
        output = await Migrate(config).run_async()

        # Check that the migration was executed
        assert "V01_01__async_test.py SUCCESS" in output

        # Verify the table was created and data inserted
        db = sqlite_connect_async
        connection = db.connect()
        cursor = connection.cursor()

        # Check table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='async_test'")
        table_exists = cursor.fetchone()
        assert table_exists is not None

        # Check data was inserted
        cursor.execute("SELECT data FROM async_test")
        result = cursor.fetchone()
        assert result[0] == 'async_data'

        connection.close()


@pytest.mark.asyncio
@pytest.mark.migrate_test
@pytest.mark.sqlite_test
@pytest.mark.python_test
async def test_mixed_sync_async_migrations(sqlite_connect_async) -> None:
    """Test that sync and async migrations can be mixed in async mode"""
    config = ConfigFile()
    config.database_type = "sqlite"
    config.database_name = './unittest-async-migrate.sqlite'
    config.database_table = 'pyway'
    config.async_mode = True

    import tempfile

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create SQL migration
        sql_migration_content = "CREATE TABLE sync_sql_test (id INTEGER PRIMARY KEY, data TEXT);"
        sql_file = os.path.join(temp_dir, 'V01_01__sql_test.sql')
        with open(sql_file, 'w') as f:
            f.write(sql_migration_content)

        # Create sync Python migration
        sync_migration_content = '''def migrate(connection):
    """Sync Python migration in async mode"""
    cursor = connection.cursor()
    cursor.execute("INSERT INTO sync_sql_test (data) VALUES ('sync_python')")
'''
        sync_file = os.path.join(temp_dir, 'V01_02__sync_python.py')
        with open(sync_file, 'w') as f:
            f.write(sync_migration_content)

        # Create async Python migration
        async_migration_content = '''async def migrate(connection):
    """Async Python migration"""
    import asyncio
    await asyncio.sleep(0.01)  # Simulate async work
    cursor = connection.cursor()
    cursor.execute("INSERT INTO sync_sql_test (data) VALUES ('async_python')")
'''
        async_file = os.path.join(temp_dir, 'V01_03__async_python.py')
        with open(async_file, 'w') as f:
            f.write(async_migration_content)

        config.database_migration_dir = temp_dir

        # Run migrations in async mode
        output = await Migrate(config).run_async()

        # All should succeed
        assert "V01_01__sql_test.sql SUCCESS" in output
        assert "V01_02__sync_python.py SUCCESS" in output
        assert "V01_03__async_python.py SUCCESS" in output

        # Verify data integrity
        db = sqlite_connect_async
        connection = db.connect()
        cursor = connection.cursor()

        cursor.execute("SELECT data FROM sync_sql_test ORDER BY data")
        results = cursor.fetchall()
        assert len(results) == 2
        assert results[0][0] == 'async_python'
        assert results[1][0] == 'sync_python'

        connection.close()


@pytest.mark.migrate_test
@pytest.mark.sqlite_test
@pytest.mark.python_test
def test_async_migration_without_async_flag() -> None:
    """Test error handling when async migration is run without --async flag"""
    config = ConfigFile()
    config.database_type = "sqlite"
    config.database_name = './unittest-async-error.sqlite'
    config.database_table = 'pyway'
    config.async_mode = False  # Async mode NOT enabled

    try:
        os.remove("./unittest-async-error.sqlite")
    except Exception:
        pass

    import tempfile

    with tempfile.TemporaryDirectory() as temp_dir:
        # Create async Python migration
        async_migration_content = '''async def migrate(connection):
    """Async migration that should fail in sync mode"""
    pass
'''

        migration_file = os.path.join(temp_dir, 'V01_01__async_fail.py')
        with open(migration_file, 'w') as f:
            f.write(async_migration_content)

        config.database_migration_dir = temp_dir

        # Should raise an error
        with pytest.raises(RuntimeError) as excinfo:
            Migrate(config).run()

        assert "has async migrate() function - use --async flag" in str(excinfo.value)
