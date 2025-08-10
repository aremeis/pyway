import os
import sys
import importlib.util
import asyncio
import inspect
from typing import List, Any

from pyway.helpers import Utils
from pyway.migration import Migration
from pyway.dbms.database import factory
from pyway.errors import MIGRATIONS_NOT_FOUND
from pyway.helpers import bcolors
from pyway.configfile import ConfigFile


class Migrate():

    def __init__(self, args: ConfigFile) -> None:
        self._db = factory(args.database_type)(args)
        self.migration_dir = args.database_migration_dir
        self.args = args

    def run(self) -> str:
        output = ''
        migrations_to_be_executed = self._get_migration_files_to_be_executed()
        if not migrations_to_be_executed:
            output += Utils.color("Nothing to do\n", bcolors.FAIL)
            return output

        for migration in migrations_to_be_executed:
            output += Utils.color(f"Migrating --> {migration.name}\n", bcolors.OKBLUE)
            try:
                if migration.extension.upper() == 'PY':
                    self._execute_python_migration(migration)
                else:
                    # Treat all other extensions as SQL migrations
                    self._execute_sql_migration(migration)
                self._db.upgrade_version(migration)
                output += Utils.color(f"{migration.name} SUCCESS\n", bcolors.OKBLUE)
            except Exception as error:
                raise RuntimeError(error)
        return output

    async def run_async(self) -> str:
        """Async version of run() method"""
        output = ''
        migrations_to_be_executed = self._get_migration_files_to_be_executed()
        if not migrations_to_be_executed:
            output += Utils.color("Nothing to do\n", bcolors.FAIL)
            return output

        for migration in migrations_to_be_executed:
            output += Utils.color(f"Migrating --> {migration.name}\n", bcolors.OKBLUE)
            try:
                if migration.extension.upper() == 'PY':
                    await self._execute_python_migration_async(migration)
                else:
                    # SQL migrations remain synchronous
                    self._execute_sql_migration(migration)
                self._db.upgrade_version(migration)
                output += Utils.color(f"{migration.name} SUCCESS\n", bcolors.OKBLUE)
            except Exception as error:
                raise RuntimeError(error)
        return output

    def _get_migration_files_to_be_executed(self) -> List:
        all_local_migrations = self._get_all_local_migrations()
        all_db_migrations = Migration.from_list(self._db.get_all_schema_migrations())

        if all_db_migrations and not all_local_migrations:
            raise RuntimeError(MIGRATIONS_NOT_FOUND % self.migration_dir)
        return Utils.subtract(all_local_migrations, all_db_migrations)

    def _get_all_local_migrations(self) -> List:
        local_files = Utils.get_local_files(self.migration_dir)
        if not local_files:
            return []
        migrations = [Migration.from_name(local_file, self.migration_dir) for local_file in local_files]
        return Utils.sort_migrations_list(migrations)

    def _execute_sql_migration(self, migration: Migration) -> None:
        """Execute SQL migration file"""
        with open(os.path.join(os.getcwd(), self.migration_dir, migration.name), "r", encoding='utf-8') as sqlfile:
            self._db.execute(sqlfile.read())

    def _load_python_module(self, migration: Migration) -> Any:
        """Load and validate Python migration module"""
        migration_path = os.path.join(os.getcwd(), self.migration_dir, migration.name)

        # Load the Python module dynamically
        spec = importlib.util.spec_from_file_location("migration_module", migration_path)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Could not load Python migration: {migration.name}")

        migration_module = importlib.util.module_from_spec(spec)

        # Add the migration directory to Python path temporarily
        sys.path.insert(0, os.path.join(os.getcwd(), self.migration_dir))
        spec.loader.exec_module(migration_module)

        # Look for the migrate function
        if not hasattr(migration_module, 'migrate'):
            raise RuntimeError(f"Python migration {migration.name} must define a 'migrate(connection)' function")

        return migration_module

    def _execute_python_migration(self, migration: Migration) -> None:
        """Execute Python migration file (sync version)"""
        original_path = sys.path[:]
        connection = None
        try:
            migration_module = self._load_python_module(migration)

            # Check if migrate is async
            if inspect.iscoroutinefunction(migration_module.migrate):
                error_msg = f"Migration {migration.name} has async migrate() function - " \
                           "use --async flag to run async migrations"
                raise RuntimeError(error_msg)

            # Execute the migration function
            connection = self._db.connect()
            migration_module.migrate(connection)

            # Auto-commit the transaction (consistent with SQL migrations)
            connection.commit()

        finally:
            # Close connection if it was opened and database requires it
            if connection and self._db.should_close_connection():
                connection.close()
            # Restore original Python path
            sys.path[:] = original_path

    async def _execute_python_migration_async(self, migration: Migration) -> None:
        """Execute Python migration file (async version)"""
        original_path = sys.path[:]
        connection = None
        try:
            migration_module = self._load_python_module(migration)

            # Execute the migration function
            connection = self._db.connect()

            # Check if migrate is async
            if inspect.iscoroutinefunction(migration_module.migrate):
                # Run async migration directly
                await migration_module.migrate(connection)
            else:
                # Run sync migration in thread pool to avoid blocking
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, migration_module.migrate, connection)

            # Auto-commit the transaction (consistent with SQL migrations)
            connection.commit()

        finally:
            # Close connection if it was opened and database requires it
            if connection and self._db.should_close_connection():
                connection.close()
            # Restore original Python path
            sys.path[:] = original_path
