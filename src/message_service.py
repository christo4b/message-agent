from datetime import datetime, timedelta
from typing import List, Dict, Any
from src.db import MessagesDB
from imessage_utils.sender import IMessageSender
import os
import shutil


class MessageService:
    """Service for interacting with iMessage"""

    def __init__(self, message_sender: IMessageSender, db_path: str = None):
        """Initialize the message service

        Args:
            message_sender: The message sender implementation
            db_path: Optional path to messages.db. If not provided, uses default location
        """
        self.message_sender = message_sender
        if db_path is None:
            db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'messages.db')
        self.db_path = db_path
        self.db = MessagesDB(self.db_path)

    def get_pending_messages(self, days_lookback: int = 14) -> List[Dict[str, Any]]:
        """Get messages that need responses"""
        messages = self.db.get_unresponded_messages(days_lookback)
        for message in messages:
            message['context'] = self.db.get_message_with_context(message['msg_id'])
            message['daily_count'] = self.db.get_daily_message_count(message['contact'])
        return messages

    def get_conversation_history(self, contact_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent conversation history with a contact"""
        return self.db.get_contact_messages(contact_id, limit)

    def send_message(self, contact: str, message: str) -> bool:
        """Send a message to a contact. Returns True if successful, False otherwise."""
        success = self.message_sender.send(contact, message)
        if not success:
            raise Exception(f"Failed to send message to {contact}")
        return success

    def reply_to_message(self, msg_id: int, response: str) -> None:
        """Reply to a specific message and mark it as responded"""
        contact_info = self.db.get_message_with_context(msg_id)
        self.send_message(contact_info['contact'], response)
        self.db.mark_message_responded(msg_id)