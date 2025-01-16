from typing import List, Dict, Any
from langchain_core.tools import Tool
from langchain_core.pydantic_v1 import BaseModel, Field
from langchain.agents import create_openai_functions_agent
from langchain_community.llms.ollama import Ollama
from langchain.agents import AgentExecutor
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
from langchain.tools.render import format_tool_to_openai_function
from src.message_service import MessageService
from src.db import MessagesDB
from imessage_utils.sender import IMessageSender


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


class MessageAgent:
    """An agent that can review and respond to messages"""

    def __init__(self, model_name: str = "mistral"):
        self.llm = Ollama(model=model_name)
        self.message_service = MessageService(IMessageSender())
        self.tools = MessageTools(self.message_service)

        # Define tools
        self.tool_list = [
            Tool(
                name="get_pending_messages",
                description="Get messages that need responses. Returns a list of messages with context.",
                func=self.tools.get_pending_messages,
                args_schema=type("GetPendingMessages", (BaseModel,), {
                    "days_lookback": (int, Field(description="Number of days to look back"))
                })
            ),
            Tool(
                name="send_message",
                description="Send a message to a contact.",
                func=self.tools.send_message,
                args_schema=type("SendMessage", (BaseModel,), {
                    "contact": (str, Field(description="Contact to send message to")),
                    "message": (str, Field(description="Message to send"))
                })
            ),
            Tool(
                name="get_conversation_history",
                description="Get recent conversation history with a contact.",
                func=self.tools.get_conversation_history,
                args_schema=type("GetConversationHistory", (BaseModel,), {
                    "contact_id": (str, Field(description="Contact to get history for")),
                    "limit": (int, Field(description="Maximum number of messages to return"))
                })
            )
        ]

        # Create prompt
        prompt = ChatPromptTemplate.from_messages([
            ("system", """You are an AI assistant that helps manage iMessages. Your role is to:
1. Review pending messages for urgency and importance
2. Identify spam or unwanted messages
3. Draft appropriate responses when needed
4. Send responses after user approval

Always be professional and courteous. If a message seems urgent, highlight that.
If you're unsure about sending a response, ask for user confirmation.

You have access to these tools:
- get_pending_messages: Get messages that need responses
- send_message: Send a message to a contact
- get_conversation_history: Get recent conversation history with a contact

Before sending any message, analyze the conversation history and context."""),
            MessagesPlaceholder(variable_name="chat_history"),
            ("human", "{input}"),
            MessagesPlaceholder(variable_name="agent_scratchpad"),
        ])

        # Create agent
        self.agent = create_openai_functions_agent(
            llm=self.llm,
            prompt=prompt,
            tools=self.tool_list
        )

        self.agent_executor = AgentExecutor(
            agent=self.agent,
            tools=self.tool_list,
            verbose=True
        )

    async def process_messages(self, days_lookback: int = 14) -> str:
        """Process pending messages and suggest actions"""
        return await self.agent_executor.ainvoke({
            "input": f"Please review messages from the last {days_lookback} days and suggest appropriate actions."
        })

    async def handle_message(self, contact: str, message: str) -> str:
        """Handle a specific message and suggest a response"""
        history = self.tools.get_conversation_history(contact)
        return await self.agent_executor.ainvoke({
            "input": f"Please review this message from {contact}: '{message}'\nSuggest an appropriate response based on the conversation history."
        })