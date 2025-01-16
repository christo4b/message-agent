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

class DraftMessageSchema(BaseModel):
    contact: str = Field(description="Contact to draft message for")
    message: str = Field(description="Draft message content")

class GetConversationHistorySchema(BaseModel):
    contact_id: str = Field(description="Contact to get history for")
    limit: int = Field(description="Maximum number of messages to return", default=10)

class MessageTools:
    """Tools for interacting with iMessage"""

    def __init__(self, message_service: MessageService):
        self.message_service = message_service
        self.draft_messages = {}  # Store draft messages for approval

    def get_pending_messages(self, days_lookback: int = 14) -> List[Dict[str, Any]]:
        """Get messages that need responses"""
        return self.message_service.get_pending_messages(days_lookback)

    def draft_message(self, contact: str, message: str) -> str:
        """Draft a message for later approval"""
        draft_id = len(self.draft_messages)
        self.draft_messages[draft_id] = {
            'contact': contact,
            'message': message
        }
        return f"Draft message #{draft_id} created for {contact}: '{message}'\nPlease ask the user to review and approve this message before sending."

    def get_conversation_history(self, contact_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent conversation history with a contact"""
        return self.message_service.get_conversation_history(contact_id, limit)


class MessageAgent:
    """An agent that can review and respond to messages"""

    def __init__(self, model_name: str = "mistral", db_path: str = None):
        self.llm = OllamaLLM(model=model_name)
        self.message_service = MessageService(IMessageSender(), db_path=db_path)
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
                name="draft_message",
                description="Draft a message for user review and approval.",
                func=self.tools.draft_message,
                args_schema=DraftMessageSchema
            ),
            StructuredTool(
                name="get_conversation_history",
                description="Get recent conversation history with a contact.",
                func=self.tools.get_conversation_history,
                args_schema=GetConversationHistorySchema
            )
        ]

        # Create system message
        system_message = SystemMessage(content="""You are an AI assistant that helps manage iMessages. Your role is to:
1. Review pending messages for urgency and importance
2. Identify spam or unwanted messages
3. Draft appropriate responses for user review
4. NEVER send messages directly - always get user approval first

IMPORTANT: You can only DRAFT messages. You cannot send them directly. 
Always use the draft_message tool to create message drafts and ask the user to review and approve them.

Always be professional and courteous. If a message seems urgent, highlight that.
Always ask for user confirmation before any action that would send a message.

You have access to these tools:
- get_pending_messages: Get messages that need responses (requires days_lookback)
- draft_message: Create a draft message for user review (requires contact and message)
- get_conversation_history: Get recent conversation history with a contact (requires contact_id and optional limit)

Before drafting any message, analyze the conversation history and context.

When using tools, make sure to provide ALL required arguments in the correct format:
- For get_conversation_history: {"contact_id": "contact@example.com", "limit": 10}
- For get_pending_messages: {"days_lookback": 7}
- For draft_message: {"contact": "contact@example.com", "message": "message"}

Remember: NEVER send messages directly. Always use draft_message and wait for user approval.""")

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
            "input": f"Please review messages from the last {days_lookback} days and suggest appropriate actions. Remember to only draft messages, never send them directly.",
            "chat_history": self.chat_history
        })

    async def handle_message(self, contact: str, message: str) -> str:
        """Handle a specific message and suggest a response"""
        history = self.tools.get_conversation_history(contact)
        return await self.agent_executor.ainvoke({
            "input": f"Please review this message from {contact}: '{message}'\nSuggest an appropriate response based on the conversation history. Remember to only draft messages, never send them directly.",
            "chat_history": self.chat_history
        })