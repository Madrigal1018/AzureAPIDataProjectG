import pytest
from unittest.mock import MagicMock, patch
from app.crud import insert_batch
from app.models import Department, Job, HiredEmployee
from datetime import date

@pytest.fixture
def mock_connection():
    """Mock a SQL connection with cursor."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    return mock_conn, mock_cursor

@patch("crud.get_connection")
def test_insert_valid_departments(mock_get_conn, mock_connection):
    mock_conn, mock_cursor = mock_connection
    mock_get_conn.return_value = mock_conn

    data = [
        Department(id=1, department="Engineering"),
        Department(id=2, department="Marketing")
    ]
    insert_batch("departments", data, ["id", "department"])

    # Check that cursor.execute was called twice (once per row)
    assert mock_cursor.execute.call_count == 2
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()

@patch("crud.get_connection")
def test_insert_with_missing_field(mock_get_conn, mock_connection):
    mock_conn, mock_cursor = mock_connection
    mock_get_conn.return_value = mock_conn

    # Simulate one bad row by using a dict instead of Pydantic model
    data = [{"id": 1}]  # missing 'department'

    insert_batch("departments", data, ["id", "department"])

    mock_cursor.execute.assert_not_called()
    mock_conn.commit.assert_not_called()
    mock_conn.close.assert_called_once()

@patch("crud.get_connection")
def test_insert_hired_employee_foreign_key_fail(mock_get_conn, mock_connection):
    mock_conn, mock_cursor = mock_connection
    mock_get_conn.return_value = mock_conn

    # Fake DB error for foreign key
    mock_cursor.execute.side_effect = Exception("Foreign key constraint fails")

    data = [
        HiredEmployee(
            id=1, name="Alice", datetime='2021-07-27T19:04:09Z',
            department_id=999, job_id=999
        )
    ]

    insert_batch("hired_employees", data, ["id", "name", "datetime", "department_id", "job_id"])

    mock_conn.commit.assert_not_called()  # Should not commit on failure
    mock_conn.close.assert_called_once()

@patch("crud.get_connection")
def test_insert_empty_batch(mock_get_conn, mock_connection):
    mock_conn, mock_cursor = mock_connection
    mock_get_conn.return_value = mock_conn

    insert_batch("jobs", [], ["id", "job"])  # No data

    mock_cursor.execute.assert_not_called()
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()

@patch("crud.get_connection")
def test_insert_dict_data_from_avro(mock_get_conn, mock_connection):
    mock_conn, mock_cursor = mock_connection
    mock_get_conn.return_value = mock_conn

    data = [
        {"id": 10, "job": "Tester"},
        {"id": 11, "job": "UX Designer"}
    ]

    insert_batch("jobs", data, ["id", "job"])

    assert mock_cursor.execute.call_count == 2
    mock_conn.commit.assert_called_once()
    mock_conn.close.assert_called_once()
