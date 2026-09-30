import json
from datetime import datetime
import pytest
from consumer import process_message

def test_process_message_valid():
    valid_msg = json.dumps({
        "transaction_id": "123",
        "user_id": "abc",
        "amount": 100.50,
        "currency": "USD",
        "transaction_type": "deposit",
        "status": "completed",
        "timestamp": "2023-10-01T12:00:00"
    })
    
    result = process_message(valid_msg)
    
    assert result is not None
    assert result['transaction_id'] == "123"
    assert isinstance(result['timestamp'], datetime)

def test_process_message_invalid_json():
    invalid_msg = "{invalid_json: true"
    result = process_message(invalid_msg)
    assert result is None

def test_process_message_missing_timestamp():
    msg = json.dumps({
        "transaction_id": "123"
    })
    result = process_message(msg)
    assert result is None
