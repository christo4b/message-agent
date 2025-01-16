from datetime import datetime, timedelta
from typing import List, Dict, Any
from src.db import MessagesDB
from imessage_utils.sender import IMessageSender
import os


class MessageService:
   def __init__(self, message_sender: IMessageSender):
       db_path = os.path.join(os.path.dirname(__file__), 'messages.db')  # Use local messages.db
       self.db = MessagesDB(db_path)
       self.message_sender = message_sender

   def get_pending_messages(self, days_lookback: int = 14) -> List[Dict[str, Any]]:
       messages = self.db.get_unresponded_messages(days_lookback)
       for message in messages:
           message['context'] = self.db.get_message_with_context(message['msg_id'])
           message['daily_count'] = self.db.get_daily_message_count(message['contact'])
       return messages

   def send_message(self, contact: str, message: str) -> bool:
       """Send a message to a contact. Returns True if successful, False otherwise."""
       success = self.message_sender.send(contact, message)
       if not success:
           raise Exception(f"Failed to send message to {contact}")
       return success

   def reply_to_message(self, msg_id: int, response: str) -> None:
       contact_info = self.db.get_message_with_context(msg_id)
       self.send_message(contact_info['contact'], response)
       self.db.mark_message_responded(msg_id)

   def get_conversation_history(self, contact_id: str, limit: int = 10) -> List[Dict[str, Any]]:
       return self.db.get_contact_messages(contact_id, limit)