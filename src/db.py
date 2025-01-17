import sqlite3
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime, timedelta
import os

logger = logging.getLogger(__name__)


class MessagesDB:
   def __init__(self, db_path: str):
       self.db_path = db_path
       os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
       self.initialize_db()

   def initialize_db(self):
       with sqlite3.connect(self.db_path) as conn:
           conn.execute("""
           CREATE TABLE IF NOT EXISTS handle (
               ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
               id TEXT NOT NULL
           )
           """)

           conn.execute("""
           CREATE TABLE IF NOT EXISTS message (
               ROWID INTEGER PRIMARY KEY AUTOINCREMENT,
               handle_id INTEGER,
               text TEXT,
               date INTEGER,
               is_from_me INTEGER,
               cache_roomnames TEXT,
               group_title TEXT,
               FOREIGN KEY (handle_id) REFERENCES handle(ROWID)
           )
           """)
           conn.commit()

   def execute_query(self, query: str, params: Optional[tuple] = None) -> List[Dict[str, Any]]:
       with sqlite3.connect(self.db_path) as conn:
           conn.row_factory = sqlite3.Row
           cursor = conn.execute(query, params or ())
           return [dict(row) for row in cursor.fetchall()]

   def execute_write(self, query: str, params: tuple) -> None:
       with sqlite3.connect(self.db_path) as conn:
           conn.execute(query, params)

   def get_message_with_context(self, msg_id: int) -> Dict[str, Any]:
       query = """
       WITH MessageContext AS (
           SELECT 
               message.ROWID as msg_id,
               message.date/1000000000 + 978307200 as timestamp,
               message.text,
               message.is_from_me,
               handle.id as contact,
               COALESCE(chat.display_name, message.cache_roomnames) as group_chat,
               COALESCE(chat.chat_identifier, message.group_title) as group_id,
               message.account,
               message.service,
               LAG(message.text) OVER (
                   PARTITION BY COALESCE(chat.ROWID, message.cache_roomnames, handle.id)
                   ORDER BY message.date
               ) as prev_msg_text,
               LEAD(message.text) OVER (
                   PARTITION BY COALESCE(chat.ROWID, message.cache_roomnames, handle.id)
                   ORDER BY message.date
               ) as next_msg_text
           FROM message 
           JOIN handle ON message.handle_id = handle.ROWID
           LEFT JOIN chat_message_join ON message.ROWID = chat_message_join.message_id
           LEFT JOIN chat ON chat_message_join.chat_id = chat.ROWID
           WHERE COALESCE(chat.ROWID, message.cache_roomnames, handle.id) = (
               SELECT COALESCE(c.ROWID, m.cache_roomnames, h.id)
               FROM message m 
               JOIN handle h ON m.handle_id = h.ROWID
               LEFT JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
               LEFT JOIN chat c ON cmj.chat_id = c.ROWID
               WHERE m.ROWID = ?
           )
       )
       SELECT * FROM MessageContext WHERE msg_id = ?
       """
       results = self.execute_query(query, (msg_id, msg_id))
       return results[0] if results else {}

   def get_daily_message_count(self, contact_id: str) -> int:
       query = """
       SELECT COUNT(*) as count
       FROM message 
       JOIN handle ON message.handle_id = handle.ROWID
       WHERE handle.id = ?
       AND datetime(message.date/1000000000 + 978307200, 'unixepoch') >= datetime('now', 'start of day')
       """
       results = self.execute_query(query, (contact_id,))
       return results[0]['count'] if results else 0

   def get_unresponded_messages(self, days_lookback: int = 14) -> List[Dict[str, Any]]:
       """Get messages that haven't been responded to within the specified number of days"""
       query = """
       WITH MessageContext AS (
           SELECT DISTINCT
               message.ROWID as msg_id,
               COALESCE(message.text, '') as text,
               hex(message.attributedBody) as attributed_body_hex,
               message.date as raw_date,
               message.date/1000000000 + 978307200 as unix_timestamp,
               datetime(message.date/1000000000 + 978307200, 'unixepoch', 'localtime') as formatted_time,
               message.is_from_me,
               message.service,
               message.account,
               COALESCE(chat.display_name, message.cache_roomnames) as group_name,
               COALESCE(chat.chat_identifier, message.group_title) as group_id,
               handle.id as contact,
               message.cache_has_attachments,
               (
                   SELECT GROUP_CONCAT(filename)
                   FROM attachment
                   JOIN message_attachment_join
                   ON attachment.ROWID = message_attachment_join.attachment_id
                   WHERE message_attachment_join.message_id = message.ROWID
               ) as attachments
           FROM message
           JOIN handle ON message.handle_id = handle.ROWID
           LEFT JOIN chat_message_join ON message.ROWID = chat_message_join.message_id
           LEFT JOIN chat ON chat_message_join.chat_id = chat.ROWID
           WHERE message.is_from_me = 0
           AND date(message.date/1000000000 + 978307200, 'unixepoch', 'localtime') >= date('now', '-' || ? || ' days')
           AND NOT EXISTS (
               SELECT 1
               FROM message m2
               WHERE m2.is_from_me = 1
               AND m2.date > message.date
               AND (
                   -- Direct message response
                   m2.handle_id = message.handle_id
                   OR (
                       -- Group chat response
                       m2.cache_roomnames = message.cache_roomnames
                       AND m2.cache_roomnames IS NOT NULL
                   )
                   OR (
                       -- Alternative group chat response
                       m2.group_title = message.group_title
                       AND m2.group_title IS NOT NULL
                   )
               )
           )
           ORDER BY message.date DESC
       )
       SELECT * FROM MessageContext
       """
       
       results = self.execute_query(query, (str(days_lookback),))
       messages = []
       for row in results:
           msg = {
               'msg_id': row['msg_id'],
               'text': row['text'] or '',
               'raw_date': row['raw_date'],
               'unix_timestamp': row['unix_timestamp'],
               'formatted_time': row['formatted_time'],
               'is_from_me': bool(row['is_from_me']),
               'service': row['service'],
               'account': row['account'],
               'group_name': row['group_name'],
               'group_id': row['group_id'],
               'contact': row['contact'],
               'has_attachments': bool(row['cache_has_attachments']),
               'attachments': row['attachments'].split(',') if row['attachments'] else []
           }
           
           # Try to get text from attributedBody if text is empty
           if not msg['text'] and row['attributed_body_hex']:
               try:
                   msg['text'] = self.extract_text_from_hex(row['attributed_body_hex']) or ''
               except:
                   pass
           
           messages.append(msg)
       
       return messages

#    def save_response(self, msg_id: int, response: str) -> None:
#        query = """
#        INSERT INTO message (text, is_from_me, handle_id, date) 
#        SELECT ?, 1, handle_id, ? 
#        FROM message WHERE ROWID = ?
#        """
#        current_time = int(datetime.now().timestamp() * 1000000000 - 978307200)
#        self.execute_write(query, (response, current_time, msg_id))

   def get_contact_messages(self, contact_id: str, limit: int = 10) -> List[Dict[str, Any]]:
       """Get recent conversation history with a contact including group messages"""
       query = """
       SELECT DISTINCT
           message.ROWID as msg_id,
           message.text,
           message.date,
           message.is_from_me,
           handle.id as contact,
           COALESCE(chat.display_name, message.cache_roomnames) as group_name,
           COALESCE(chat.chat_identifier, message.group_title) as group_id,
           message.account,
           message.service,
           datetime(message.date/1000000000 + 978307200, 'unixepoch', 'localtime') as formatted_time
       FROM message 
       JOIN handle ON message.handle_id = handle.ROWID
       LEFT JOIN chat_message_join ON message.ROWID = chat_message_join.message_id
       LEFT JOIN chat ON chat_message_join.chat_id = chat.ROWID
       WHERE (
           handle.id = ? 
           OR EXISTS (
               SELECT 1 
               FROM chat_message_join cmj2
               JOIN chat c2 ON cmj2.chat_id = c2.ROWID
               JOIN message m2 ON cmj2.message_id = m2.ROWID
               WHERE m2.handle_id = handle.ROWID
               AND (
                   c2.ROWID = chat.ROWID 
                   OR m2.cache_roomnames = message.cache_roomnames
               )
           )
       )
       ORDER BY message.date DESC
       LIMIT ?
       """
       return self.execute_query(query, (contact_id, limit))

   def mark_message_responded(self, msg_id: int) -> None:
       query = """
       UPDATE message 
       SET is_from_me = 1
       WHERE ROWID = ?
       """
       self.execute_write(query, (msg_id,))

   def get_conversation_history(self, contact_id: str = "", limit: int = 10) -> List[Dict]:
       """Get conversation history with a contact"""
       query = """
       WITH contact_messages AS (
           SELECT DISTINCT
               message.ROWID as msg_id,
               message.text,
               hex(message.attributedBody) as attributed_body_hex,
               message.date,
               message.is_from_me,
               handle.id as contact,
               COALESCE(chat.display_name, message.cache_roomnames) as group_name,
               COALESCE(chat.chat_identifier, message.group_title) as group_id,
               message.account,
               message.service,
               datetime(message.date/1000000000 + 978307200, 'unixepoch', 'localtime') as formatted_time
           FROM message
           LEFT JOIN handle ON message.handle_id = handle.ROWID
           LEFT JOIN chat_message_join ON message.ROWID = chat_message_join.message_id
           LEFT JOIN chat ON chat_message_join.chat_id = chat.ROWID
           WHERE (handle.id = ? OR ? = '')
           ORDER BY message.date DESC
           LIMIT ?
       )
       SELECT * FROM contact_messages;
       """
       
       results = self.execute_query(query, (contact_id, contact_id, limit))
       messages = []
       for row in results:
           msg = dict(row)
           # Try to get text from attributedBody if text is None
           if msg['text'] is None and msg['attributed_body_hex']:
               try:
                   msg['text'] = self.extract_text_from_hex(msg['attributed_body_hex'])
               except:
                   msg['text'] = None
           messages.append(msg)
       return messages

   def extract_text_from_hex(self, hex_data: str) -> str:
       """Extract text from hex encoded attributed body data"""
       if not hex_data:
           return None
            
       # Convert hex to bytes
       try:
           data = bytes.fromhex(hex_data)
            
           # Look for text between NSString+ and NSDictionary
           text = data.decode('utf-8', errors='ignore')
           start = text.find('NSString+') + 9
           end = text.find('NSDictionary')
           if start > 8 and end > start:
               text = text[start:end].strip()
               # Clean up control characters
               text = ''.join(char for char in text if ord(char) >= 32 or char in '\n\t')
               return text
       except:
           pass
       return None

   def get_recent_messages(self, days_lookback: int = 14) -> List[Dict[str, Any]]:
       """Get all messages from the last N days"""
       query = """
       SELECT 
           message.ROWID as msg_id,
           message.text,
           message.date as raw_date,
           message.date/1000000000 + 978307200 as unix_timestamp,
           datetime(message.date/1000000000 + 978307200, 'unixepoch', 'localtime') as formatted_time,
           message.is_from_me,
           message.service,
           message.account,
           handle.id as contact,
           COALESCE(chat.display_name, message.cache_roomnames) as group_name,
           COALESCE(chat.chat_identifier, message.group_title) as group_id,
           message.cache_has_attachments,
           (
               SELECT GROUP_CONCAT(filename)
               FROM attachment
               JOIN message_attachment_join
               ON attachment.ROWID = message_attachment_join.attachment_id
               WHERE message_attachment_join.message_id = message.ROWID
           ) as attachments
       FROM message
       JOIN handle ON message.handle_id = handle.ROWID
       LEFT JOIN chat_message_join ON message.ROWID = chat_message_join.message_id
       LEFT JOIN chat ON chat_message_join.chat_id = chat.ROWID
       WHERE date(message.date/1000000000 + 978307200, 'unixepoch', 'localtime') >= date('now', '-' || ? || ' days')
       AND message.text IS NOT NULL
       ORDER BY message.date DESC
       """
       
       results = self.execute_query(query, (str(days_lookback),))
       messages = []
       for row in results:
           msg = {
               'msg_id': row['msg_id'],
               'text': row['text'] or '',
               'raw_date': row['raw_date'],
               'unix_timestamp': row['unix_timestamp'],
               'formatted_time': row['formatted_time'],
               'is_from_me': bool(row['is_from_me']),
               'service': row['service'],
               'account': row['account'],
               'group_name': row['group_name'],
               'group_id': row['group_id'],
               'contact': row['contact'],
               'has_attachments': bool(row['cache_has_attachments']),
               'attachments': row['attachments'].split(',') if row['attachments'] else []
           }
           messages.append(msg)
        
       return messages