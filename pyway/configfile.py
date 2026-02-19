import os
import sys
from typing import Any, Union


class ConfigFile():
    def __init__(self, **kwargs: Any) -> None:
        self.database_migration_dir = os.environ.get('PYWAY_DATABASE_MIGRATION_DIR', kwargs.get('database_migration_dir'))
        self.database_table = os.environ.get('PYWAY_TABLE', kwargs.get('database_table'))
        self.database_type = os.environ.get('PYWAY_TYPE', kwargs.get('database_type'))
        self.database_host = os.environ.get('PYWAY_DATABASE_HOST', kwargs.get('database_host'))
        self.database_port = os.environ.get('PYWAY_DATABASE_PORT', kwargs.get('database_port'))
        self.database_name = os.environ.get('PYWAY_DATABASE_NAME', kwargs.get('database_name'))
        self.database_username = os.environ.get('PYWAY_DATABASE_USERNAME', kwargs.get('database_username'))
        self.database_password = os.environ.get('PYWAY_DATABASE_PASSWORD', kwargs.get('database_password'))
        self.database_collation = os.environ.get('PYWAY_DATABASE_COLLATION', kwargs.get('database_collation'))
        self.schema_file: Union[str, None] = None
        self.checksum_file = None
        self.config = os.environ.get('PYWAY_CONFIG_FILE', '.pyway.conf')
        self.version = False
        self.async_mode = None
        self.cmd = None
        self.prepared_for_python_migrations = False

    def merge(self, other: 'ConfigFile') -> None:
        for key, value in vars(other).items():
            if value is not None:
                setattr(self, key, value)

    def prepare_for_python_migrations(self) -> None:
        if self.prepared_for_python_migrations:
            return

        # Add the current working directory to the Python path
        # This is necessary for the Python migrations to be able to reference modules relative to the current working directory
        sys.path.append(os.getcwd())

        # Make sure all database connection environment variables are set
        os.environ['PYWAY_DATABASE_TYPE'] = self.database_type
        os.environ['PYWAY_DATABASE_HOST'] = self.database_host
        os.environ['PYWAY_DATABASE_PORT'] = str(self.database_port)
        os.environ['PYWAY_DATABASE_NAME'] = self.database_name
        os.environ['PYWAY_DATABASE_USERNAME'] = self.database_username
        os.environ['PYWAY_DATABASE_PASSWORD'] = self.database_password
        os.environ['PYWAY_DATABASE_COLLATION'] = self.database_collation


class MockConfig():
    pass


class MockArgs():
    pass
