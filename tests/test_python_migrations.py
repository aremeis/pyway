import pytest
import os
from strip_ansi import strip_ansi
from pyway.migrate import Migrate
from pyway.settings import ConfigFile
from pyway.dbms.database import factory


@pytest.fixture
def sqlite_connect_python(autouse: bool = True):
    """Setup SQLite database for Python migration testing"""
    # Delete any existing databases
    try:
        os.remove("./unittest-python-migrate.sqlite")
    except Exception:
        pass

    args = ConfigFile()
    args.database_type = "sqlite"
    args.database_name = "./unittest-python-migrate.sqlite"
    args.database_table = "pyway"

    return factory(args.database_type)(args)


@pytest.mark.migrate_test
@pytest.mark.sqlite_test
@pytest.mark.python_test
def test_python_migration_execution(sqlite_connect_python) -> None:
    """Test that Python migrations execute correctly"""
    config = ConfigFile()
    config.database_type = "sqlite"
    config.database_name = './unittest-python-migrate.sqlite'
    config.database_table = 'pyway'
    
    # Create a temporary directory with a Python migration
    import tempfile
    import shutil
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Copy the Python migration file to temp directory
        python_migration_content = '''def migrate(connection):
    """Test Python migration"""
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE python_test (id INTEGER PRIMARY KEY, name TEXT)")
    cursor.execute("INSERT INTO python_test (name) VALUES ('test_data')")
'''
        
        migration_file = os.path.join(temp_dir, 'V01_01__python_test.py')
        with open(migration_file, 'w') as f:
            f.write(python_migration_content)
        
        config.database_migration_dir = temp_dir
        
        # Run the migration
        output = Migrate(config).run()
        
        # Check that the migration was executed
        assert "V01_01__python_test.py SUCCESS" in strip_ansi(output)
        
        # Verify the table was created and data inserted
        db = sqlite_connect_python
        connection = db.connect()
        cursor = connection.cursor()
        
        # Check table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='python_test'")
        table_exists = cursor.fetchone()
        assert table_exists is not None
        
        # Check data was inserted
        cursor.execute("SELECT name FROM python_test")
        result = cursor.fetchone()
        assert result[0] == 'test_data'
        
        connection.close()


@pytest.mark.migrate_test  
@pytest.mark.sqlite_test
@pytest.mark.python_test
def test_mixed_sql_python_migrations(sqlite_connect_python) -> None:
    """Test that SQL and Python migrations can be mixed"""
    config = ConfigFile()
    config.database_type = "sqlite"
    config.database_name = './unittest-python-migrate.sqlite'
    config.database_table = 'pyway'
    
    import tempfile
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create SQL migration
        sql_migration_content = "CREATE TABLE sql_test (id INTEGER PRIMARY KEY, data TEXT);"
        sql_file = os.path.join(temp_dir, 'V01_01__sql_test.sql')
        with open(sql_file, 'w') as f:
            f.write(sql_migration_content)
        
        # Create Python migration 
        python_migration_content = '''def migrate(connection):
    """Python migration following SQL migration"""
    cursor = connection.cursor()
    cursor.execute("INSERT INTO sql_test (data) VALUES ('inserted_by_python')")
'''
        python_file = os.path.join(temp_dir, 'V01_02__python_test.py')
        with open(python_file, 'w') as f:
            f.write(python_migration_content)
            
        config.database_migration_dir = temp_dir
        
        # Run migrations
        output = Migrate(config).run()
        
        # Both should succeed
        assert "V01_01__sql_test.sql SUCCESS" in strip_ansi(output)
        assert "V01_02__python_test.py SUCCESS" in strip_ansi(output)
        
        # Verify data integrity
        db = sqlite_connect_python
        connection = db.connect()
        cursor = connection.cursor()
        
        cursor.execute("SELECT data FROM sql_test")
        result = cursor.fetchone()
        assert result[0] == 'inserted_by_python'
        
        connection.close()


@pytest.mark.migrate_test
@pytest.mark.sqlite_test  
@pytest.mark.python_test
def test_python_migration_error_handling() -> None:
    """Test error handling for malformed Python migrations"""
    config = ConfigFile()
    config.database_type = "sqlite"
    config.database_name = './unittest-python-migrate-error.sqlite'
    config.database_table = 'pyway'
    
    try:
        os.remove("./unittest-python-migrate-error.sqlite")
    except Exception:
        pass
    
    import tempfile
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create Python migration without migrate function
        bad_python_content = '''def wrong_function_name(connection):
    pass
'''
        
        migration_file = os.path.join(temp_dir, 'V01_01__bad_python.py')
        with open(migration_file, 'w') as f:
            f.write(bad_python_content)
        
        config.database_migration_dir = temp_dir
        
        # Should raise an error
        with pytest.raises(RuntimeError) as excinfo:
            Migrate(config).run()
        
        assert "must define a 'migrate(connection)' function" in str(excinfo.value)