def migrate(connection):
    """
    SQLite-specific Python migration example.
    Args:
        connection: SQLite connection object
    """
    cursor = connection.cursor()

    # Create a table specific to SQLite features
    cursor.execute("""
        CREATE TABLE sqlite_python_test (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            data TEXT,
            metadata JSON,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # Insert some test data with SQLite-specific features
    test_records = [
        ('{"type": "user", "active": true}', '{"source": "python_migration"}'),
        ('{"type": "admin", "active": true}', '{"source": "python_migration", "priority": "high"}'),
    ]

    for data, metadata in test_records:
        cursor.execute(
            "INSERT INTO sqlite_python_test (data, metadata) VALUES (?, ?)",
            (data, metadata)
        )
