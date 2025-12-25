from scripts.monitoring.pipeline_monitor import db_connect

def test_database_connection():
    conn, response_time = db_connect()
    assert conn is not None
    assert response_time >= 0
    conn.close()
