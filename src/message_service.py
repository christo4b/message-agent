import os
from typing import Dict, List, Optional
from .db import MessagesDB
from imessage_utils.sender import IMessageSender

class MessageService:
    """Service for managing iMessages"""

    def __init__(self, message_sender: IMessageSender, db_path: str = None):
        """Initialize the service with a message sender and database"""
        if db_path is None:
            db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'messages.db')
        self.db = MessagesDB(db_path)
        self.message_sender = message_sender
        self.draft_messages = {}  # Store draft messages for approval

    def get_recent_messages(self, days_lookback: int = 14) -> List[Dict]:
        """Get all messages from the last N days"""
        return self.db.get_recent_messages(days_lookback)

    def get_pending_messages(self, days_lookback: int = 14) -> List[Dict]:
        """Get messages that need responses"""
        messages = self.get_recent_messages(days_lookback)
        # Filter messages that need responses
        return [
            msg for msg in messages 
            if not msg['is_from_me']  # Not sent by me
            and msg.get('text')  # Has text content
        ]

    def draft_message(self, contact: str, message: str) -> str:
        """Draft a message for later approval"""
        draft_id = len(self.draft_messages)
        self.draft_messages[draft_id] = {
            'contact': contact,
            'message': message
        }
        return f"Draft message #{draft_id} created for {contact}: '{message}'\nPlease ask the user to review and approve this message before sending."

    def get_conversation_history(self, contact_id: str = "", limit: int = 10) -> List[Dict]:
        """Get conversation history with a contact"""
        return self.db.get_conversation_history(contact_id, limit)

    def send_message(self, contact: str, message: str) -> bool:
        """Send a message to a contact"""
        return self.message_sender.send_message(contact, message)
