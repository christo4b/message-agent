import sqlite3
from typing import List, Dict, Any, Optional
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class MessagesDB:
   def __init__(self, db_path: str):
       self.db_path = db_path

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
       SELECT 
           message.ROWID as msg_id,
           message.date/1000000000 + 978307200 as timestamp,
           message.text,
           message.is_from_me,
           handle.id as contact,
           message.cache_roomnames as group_chat,
           message.group_title,
           LAG(message.date/1000000000 + 978307200) OVER (
               PARTITION BY handle.id ORDER BY message.date
           ) as prev_msg_time,
           LAG(message.text) OVER (
               PARTITION BY handle.id ORDER BY message.date
           ) as prev_msg_text,
           LEAD(message.date/1000000000 + 978307200) OVER (
               PARTITION BY handle.id ORDER BY message.date
           ) as next_msg_time,
           LEAD(message.text) OVER (
               PARTITION BY handle.id ORDER BY message.date
           ) as next_msg_text
       FROM message 
       LEFT JOIN handle ON message.handle_id = handle.ROWID
       WHERE message.ROWID = ?
       """
       results = self.execute_query(query, (msg_id,))
       return results[0] if results else {}

   def get_daily_message_count(self, contact_id: str) -> int:
       query = """
       SELECT COUNT(*) as count
       FROM message 
       LEFT JOIN handle ON message.handle_id = handle.ROWID
       WHERE handle.id = ?
       AND message.date/1000000000 + 978307200 >= strftime('%s', 'now', 'start of day')
       """
       return self.execute_query(query, (contact_id,))[0]['count']

   def get_unresponded_messages(self, days_ago: int = 14) -> List[Dict[str, Any]]:
       query = """
       WITH MessageContext AS (
           SELECT 
               message.ROWID as msg_id,
               message.date/1000000000 + 978307200 as timestamp,
               message.text,
               message.is_from_me,
               handle.id as contact,
               LEAD(message.date/1000000000 + 978307200) OVER (
                   PARTITION BY handle.id ORDER BY message.date
               ) as next_msg_time
           FROM message 
           LEFT JOIN handle ON message.handle_id = handle.ROWID
           WHERE message.date/1000000000 + 978307200 >= ?
       )
       SELECT *
       FROM MessageContext
       WHERE is_from_me = 0 
       AND (next_msg_time IS NULL OR (next_msg_time - timestamp) > 86400)
       ORDER BY timestamp DESC
       """
       timestamp = int((datetime.now() - timedelta(days=days_ago)).timestamp())
       return self.execute_query(query, (timestamp,))

#    def save_response(self, msg_id: int, response: str) -> None:
#        query = """
#        INSERT INTO message (text, is_from_me, handle_id, date) 
#        SELECT ?, 1, handle_id, ? 
#        FROM message WHERE ROWID = ?
#        """
#        current_time = int(datetime.now().timestamp() * 1000000000 - 978307200)
#        self.execute_write(query, (response, current_time, msg_id))

   def get_contact_messages(self, contact_id: str, limit: int = 10) -> List[Dict[str, Any]]:
       query = """
       SELECT 
           message.ROWID as msg_id,
           message.date/1000000000 + 978307200 as timestamp,
           message.text,
           message.is_from_me,
           handle.id as contact
       FROM message 
       LEFT JOIN handle ON message.handle_id = handle.ROWID
       WHERE handle.id = ?
       ORDER BY message.date DESC
       LIMIT ?
       """
       return self.execute_query(query, (contact_id, limit))