from typing import List, Dict, Any
from langchain_core.tools import StructuredTool
from pydantic import BaseModel, Field
from langchain.agents.structured_chat.base import StructuredChatAgent
from langchain_ollama import OllamaLLM
from langchain.agents import AgentExecutor
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from .message_service import MessageService
from .db import MessagesDB
from imessage_utils.sender import IMessageSender
import sqlite3


class GetPendingMessagesSchema(BaseModel):
    days_lookback: int = Field(description="Number of days to look back")

class SendMessageSchema(BaseModel):
    contact: str = Field(description="Contact to send message to")
    message: str = Field(description="Message to send")

class GetConversationHistorySchema(BaseModel):
    contact_id: str = Field(description="Contact to get history for")
    limit: int = Field(description="Maximum number of messages to return", default=10)

class GetContactsSchema(BaseModel):
    pass


class MessageTools:
    """Tools for interacting with iMessage"""

    def __init__(self, message_service: MessageService):
        self.message_service = message_service

    def get_pending_messages(self, days_lookback: int = 14) -> List[Dict[str, Any]]:
        """Get messages that need responses"""
        return self.message_service.get_pending_messages(days_lookback)

    def send_message(self, contact: str, message: str) -> bool:
        """Send a message to a contact"""
        return self.message_service.send_message(contact, message)

    def get_conversation_history(self, contact_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent conversation history with a contact"""
        return self.message_service.get_conversation_history(contact_id, limit)

    def get_contacts(self) -> List[str]:
        """Get a list of all contacts with message history"""
        with sqlite3.connect(self.message_service.db.db_path) as conn:
            cursor = conn.execute("SELECT DISTINCT id FROM handle")
            return [row[0] for row in cursor.fetchall()]


class MessageAgent:
    """An agent that can review and respond to messages"""

    def __init__(self, model_name: str = "mistral"):
        self.llm = OllamaLLM(model=model_name)
        self.message_service = MessageService(IMessageSender())
        self.tools = MessageTools(self.message_service)
        self.chat_history = []

        # Define tools
        self.tool_list = [
            StructuredTool(
                name="get_pending_messages",
                description="Get messages that need responses. Returns a list of messages with context.",
                func=self.tools.get_pending_messages,
                args_schema=GetPendingMessagesSchema
            ),
            StructuredTool(
                name="send_message",
                description="Send a message to a contact.",
                func=self.tools.send_message,
                args_schema=SendMessageSchema
            ),
            StructuredTool(
                name="get_conversation_history",
                description="Get recent conversation history with a contact.",
                func=self.tools.get_conversation_history,
                args_schema=GetConversationHistorySchema
            ),
            StructuredTool(
                name="get_contacts",
                description="Get a list of all contacts with message history.",
                func=self.tools.get_contacts,
                args_schema=GetContactsSchema
            )
        ]

        # Create system message
        system_message = SystemMessage(content="""You are an AI assistant that helps manage iMessages. Your role is to:
1. Review pending messages for urgency and importance
2. Identify spam or unwanted messages
3. Draft appropriate responses when needed
4. Send responses after user approval

Always be professional and courteous. If a message seems urgent, highlight that.
If you're unsure about sending a response, ask for user confirmation.

You have access to these tools:
- get_contacts: Get a list of all contacts with message history (no arguments needed)
- get_pending_messages: Get messages that need responses (requires days_lookback)
- send_message: Send a message to a contact (requires contact and message)
- get_conversation_history: Get recent conversation history with a contact (requires contact_id and optional limit)

Before sending any message, analyze the conversation history and context.

When using tools, make sure to provide ALL required arguments in the correct format:
- For get_contacts: {}
- For get_conversation_history: {"contact_id": "contact@example.com", "limit": 10}
- For get_pending_messages: {"days_lookback": 7}
- For send_message: {"contact": "contact@example.com", "message": "message"}

If you need to get conversation history, first use get_contacts to find available contacts.""")

        # Create agent
        self.agent = StructuredChatAgent.from_llm_and_tools(
            llm=self.llm,
            tools=self.tool_list,
            system_message=system_message
        )

        self.agent_executor = AgentExecutor(
            agent=self.agent,
            tools=self.tool_list,
            verbose=True
        )

    async def process_messages(self, days_lookback: int = 14) -> str:
        """Process pending messages and suggest actions"""
        return await self.agent_executor.ainvoke({
            "input": f"Please review messages from the last {days_lookback} days and suggest appropriate actions.",
            "chat_history": self.chat_history
        })

    async def handle_message(self, contact: str, message: str) -> str:
        """Handle a specific message and suggest a response"""
        history = self.tools.get_conversation_history(contact)
        return await self.agent_executor.ainvoke({
            "input": f"Please review this message from {contact}: '{message}'\nSuggest an appropriate response based on the conversation history.",
            "chat_history": self.chat_history
        })