import pytest
from unittest.mock import Mock, patch, call
from datetime import datetime, timedelta
import sqlite3
from src.message_service import MessageService
from src.db import MessagesDB
import os

@pytest.fixture
def mock_db():
    return Mock()

@pytest.fixture
def mock_sender():
    return Mock()

@pytest.fixture
def service(mock_db, mock_sender):
    return MessageService(mock_sender)

@pytest.fixture
def test_db():
    # Use shared in-memory SQLite database for testing
    db = MessagesDB("file::memory:?cache=shared")
    db.initialize_db()
    return db

@pytest.fixture
def real_db():
    # Use the actual messages.db from the repository
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src', 'messages.db')
    db = MessagesDB(db_path)
    return db

@pytest.fixture(autouse=True)
def clear_tables(test_db):
    with sqlite3.connect(test_db.db_path) as conn:
        conn.execute("DELETE FROM message")
        conn.execute("DELETE FROM handle")
        conn.commit()

class TestMessageService:
    def test_send_message_success(self, service, mock_sender):
        mock_sender.send.return_value = True
        assert service.send_message("test@example.com", "Hello") == True
        mock_sender.send.assert_called_once_with("test@example.com", "Hello")

    def test_send_message_failure(self, service, mock_sender):
        mock_sender.send.return_value = False
        with pytest.raises(Exception, match="Failed to send message"):
            service.send_message("test@example.com", "Hello")

    def test_get_pending_messages_with_context(self, service, mock_db):
        # Setup mock returns
        mock_messages = [
            {
                'msg_id': 1,
                'contact': 'test1@example.com',
                'text': 'Hello',
                'timestamp': int(datetime.now().timestamp()),
                'is_from_me': 0
            },
            {
                'msg_id': 2,
                'contact': 'test2@example.com',
                'text': 'Hi there',
                'timestamp': int(datetime.now().timestamp()),
                'is_from_me': 0
            }
        ]
        
        mock_context = {
            'contact': 'test1@example.com',
            'text': 'Hello',
            'prev_msg_text': 'Previous message',
            'next_msg_text': None,
            'group_chat': None,
            'group_title': None
        }
        
        service.db.get_unresponded_messages = Mock(return_value=mock_messages)
        service.db.get_message_with_context = Mock(return_value=mock_context)
        service.db.get_daily_message_count = Mock(return_value=3)

        messages = service.get_pending_messages(days_lookback=7)
        
        assert len(messages) == 2
        assert messages[0]['context'] == mock_context
        assert messages[0]['daily_count'] == 3
        service.db.get_unresponded_messages.assert_called_once_with(7)

    def test_conversation_history_limit(self, service, mock_db):
        contact_id = "test@example.com"
        limit = 5
        
        mock_messages = [
            {'msg_id': i, 'text': f'Message {i}'} for i in range(limit)
        ]
        service.db.get_contact_messages = Mock(return_value=mock_messages)

        history = service.get_conversation_history(contact_id, limit)
        
        assert len(history) == limit
        service.db.get_contact_messages.assert_called_once_with(contact_id, limit)

    def test_reply_to_message_marks_responded(self, service, mock_sender):
        msg_id = 1
        response = "Reply message"
        contact = "test@example.com"
        
        mock_context = {'contact': contact, 'text': 'Original message'}
        service.db.get_message_with_context = Mock(return_value=mock_context)
        service.db.mark_message_responded = Mock()
        mock_sender.send.return_value = True
        
        service.reply_to_message(msg_id, response)
        
        mock_sender.send.assert_called_once_with(contact, response)
        service.db.mark_message_responded.assert_called_once_with(msg_id)

class TestMessagesDB:
    def test_initialize_db(self, test_db):
        # Verify tables were created
        with sqlite3.connect(test_db.db_path) as conn:
            cursor = conn.cursor()
            
            # Check handle table
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='handle'
            """)
            assert cursor.fetchone() is not None
            
            # Check message table
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name='message'
            """)
            assert cursor.fetchone() is not None

    def test_get_message_with_context_empty(self, test_db):
        result = test_db.get_message_with_context(1)
        assert result == {}

    def test_get_message_with_context_full(self, test_db):
        # Insert test data
        with sqlite3.connect(test_db.db_path) as conn:
            # Insert handle
            conn.execute(
                "INSERT INTO handle (id) VALUES (?)",
                ("test@example.com",)
            )
            handle_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            
            # Insert messages
            timestamp = int(datetime.now().timestamp())
            messages = [
                (handle_id, "Previous message", (timestamp - 100) * 1000000000 + 978307200, 0),
                (handle_id, "Current message", timestamp * 1000000000 + 978307200, 0),
                (handle_id, "Next message", (timestamp + 100) * 1000000000 + 978307200, 0)
            ]
            
            for msg in messages:
                conn.execute(
                    "INSERT INTO message (handle_id, text, date, is_from_me) VALUES (?, ?, ?, ?)",
                    msg
                )
            msg_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0] - 1

        result = test_db.get_message_with_context(msg_id)
        
        assert result['text'] == "Current message"
        assert result['contact'] == "test@example.com"
        assert result['prev_msg_text'] == "Previous message"
        assert result['next_msg_text'] == "Next message"

    def test_daily_message_count(self, test_db):
        contact = "test@example.com"
        
        # Insert test data
        with sqlite3.connect(test_db.db_path) as conn:
            # Insert handle
            conn.execute("INSERT INTO handle (id) VALUES (?)", (contact,))
            handle_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            
            # Insert messages for today
            today_timestamp = int(datetime.now().timestamp())
            messages = [
                (handle_id, f"Message {i}", 
                 (today_timestamp - i * 3600) * 1000000000 + 978307200, 0)
                for i in range(5)
            ]
            
            for msg in messages:
                conn.execute(
                    "INSERT INTO message (handle_id, text, date, is_from_me) VALUES (?, ?, ?, ?)",
                    msg
                )

        count = test_db.get_daily_message_count(contact)
        assert count == 5

    def test_mark_message_responded(self, test_db):
        # Insert test message
        with sqlite3.connect(test_db.db_path) as conn:
            conn.execute("INSERT INTO handle (id) VALUES (?)", ("test@example.com",))
            handle_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            
            conn.execute(
                "INSERT INTO message (handle_id, text, date, is_from_me) VALUES (?, ?, ?, ?)",
                (handle_id, "Test message", int(datetime.now().timestamp()) * 1000000000 + 978307200, 0)
            )
            msg_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # Mark as responded
        test_db.mark_message_responded(msg_id)

        # Verify message was marked as responded
        with sqlite3.connect(test_db.db_path) as conn:
            cursor = conn.execute("SELECT is_from_me FROM message WHERE ROWID = ?", (msg_id,))
            assert cursor.fetchone()[0] == 1

    def test_get_unresponded_messages(self, test_db):
        # Insert test data
        with sqlite3.connect(test_db.db_path) as conn:
            # Insert handle
            conn.execute("INSERT INTO handle (id) VALUES (?)", ("test@example.com",))
            handle_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            
            # Insert messages with varying timestamps and response status
            now = datetime.now()
            messages = [
                # Unresponded recent message
                (handle_id, "Recent unresponded", 
                 int((now - timedelta(days=1)).timestamp()) * 1000000000 + 978307200, 0),
                # Responded recent message
                (handle_id, "Recent responded", 
                 int((now - timedelta(days=1)).timestamp()) * 1000000000 + 978307200, 1),
                # Old unresponded message
                (handle_id, "Old unresponded", 
                 int((now - timedelta(days=20)).timestamp()) * 1000000000 + 978307200, 0)
            ]
            
            for msg in messages:
                conn.execute(
                    "INSERT INTO message (handle_id, text, date, is_from_me) VALUES (?, ?, ?, ?)",
                    msg
                )

            # Debug: Check what's in the database
            cursor = conn.execute("""
                SELECT message.ROWID, message.text, message.date/1000000000 + 978307200 as timestamp,
                       strftime('%s', 'now', '-14 days') as cutoff
                FROM message
                WHERE is_from_me = 0
            """)
            for row in cursor:
                print(f"Message: {row[1]}, Timestamp: {row[2]}, Cutoff: {row[3]}")

        # Get unresponded messages from last 14 days
        messages = test_db.get_unresponded_messages(14)
        
        assert len(messages) == 1
        assert messages[0]['text'] == "Recent unresponded"
        assert messages[0]['is_from_me'] == 0

class TestRealMessagesDB:
    """Tests that run against the actual messages.db from the repository"""
    
    def test_get_unresponded_messages(self, real_db):
        messages = real_db.get_unresponded_messages(14)
        # Print some debug info about what we found
        print(f"\nFound {len(messages)} unresponded messages in the last 14 days:")
        for msg in messages:
            print(f"- From {msg['contact']}: {msg['text'][:50]}...")
        
        # We don't assert specific counts since the data is real,
        # but we can verify the structure
        for msg in messages:
            assert 'msg_id' in msg
            assert 'text' in msg
            assert 'contact' in msg
            assert 'timestamp' in msg
            assert msg['is_from_me'] == 0  # Should only be incoming messages
    
    def test_get_message_with_context(self, real_db):
        # Get a recent message to test with
        messages = real_db.get_unresponded_messages(14)
        if not messages:
            pytest.skip("No recent messages found to test with")
        
        msg_id = messages[0]['msg_id']
        context = real_db.get_message_with_context(msg_id)
        
        print(f"\nMessage context for msg_id {msg_id}:")
        print(f"- Current: {context.get('text', '')[:50]}...")
        print(f"- Previous: {context.get('prev_msg_text', '')[:50]}...")
        print(f"- Next: {context.get('next_msg_text', '')[:50]}...")
        
        assert 'text' in context
        assert 'contact' in context
        assert 'prev_msg_text' in context  # May be None
        assert 'next_msg_text' in context  # May be None
    
    def test_daily_message_count(self, real_db):
        # Get a recent message to test with
        messages = real_db.get_unresponded_messages(14)
        if not messages:
            pytest.skip("No recent messages found to test with")
        
        contact = messages[0]['contact']
        count = real_db.get_daily_message_count(contact)
        
        print(f"\nDaily message count for {contact}: {count}")
        assert isinstance(count, int)
        assert count >= 0