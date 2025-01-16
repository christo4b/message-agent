from datetime import datetime, timedelta
from typing import List, Dict, Any
from db import MessagesDB
from imessage_utils import MessageSender

class MessageService:
   def __init__(self, db: MessagesDB, message_sender: MessageSender):
       self.db = db
       self.message_sender = message_sender

   def get_pending_messages(self, days_lookback: int = 14) -> List[Dict[str, Any]]:
       messages = self.db.get_unresponded_messages(days_lookback)
       for message in messages:
           message['context'] = self.db.get_message_with_context(message['msg_id'])
           message['daily_count'] = self.db.get_daily_message_count(message['contact'])
       return messages

   def reply_to_message(self, msg_id: int, response: str) -> None:
       contact_info = self.db.get_message_with_context(msg_id)
       self.message_sender.send_message(contact_info['contact'], response)

   def get_conversation_history(self, contact_id: str, limit: int = 10) -> List[Dict[str, Any]]:
       return self.db.get_contact_messages(contact_id, limit)