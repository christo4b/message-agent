import asyncio
from typing import Dict, List, Optional
from langchain_core.tools import tool
from langchain_ollama import OllamaLLM
from pydantic import BaseModel, Field
from langchain.agents import AgentExecutor, create_react_agent
from langchain_core.prompts import PromptTemplate

from .message_service import MessageService, IMessageSender

class GetPendingMessagesSchema(BaseModel):
    days_lookback: int = Field(description="Number of days to look back")

class SendMessageSchema(BaseModel):
    contact: str = Field(description="Contact to send message to")
    message: str = Field(description="Message to send")

class GetConversationHistorySchema(BaseModel):
    contact_id: str = Field(description="Contact to get history for")
    limit: int = Field(description="Maximum number of messages to return", default=10)

class MessageAgent:
    """Agent for managing iMessages with AI assistance"""

    def __init__(self, model_name: str = "mistral"):
        """Initialize the agent with a model name"""
        self.message_service = MessageService(IMessageSender())
        
        # Initialize LLM with better parameters
        self.llm = OllamaLLM(
            model=model_name,
            temperature=0.7,
            timeout=30
        )

    def process_messages(self, days: int) -> str:
        """Process pending messages and suggest actions"""
        try:
            messages = self.message_service.get_pending_messages(days)
            if not messages:
                return "No messages need responses."
            
            responses = []
            for msg in messages:
                response = self.handle_message(msg['contact'], msg['text'])
                responses.append(f"\nMessage from {msg['contact']} at {msg['formatted_time']}:\n{msg['text']}\n\nSuggested response:\n{response}")
            
            return "\n---\n".join(responses)
        except Exception as e:
            return f"Error processing messages: {str(e)}"

    def handle_message(self, contact: str, message: str) -> str:
        """Handle a specific message and suggest a response"""
        try:
            # Get conversation history
            history = self.message_service.get_conversation_history(contact, limit=5)
            history_text = "\n".join([
                f"{'→' if msg['is_from_me'] else '←'} {msg['text']}"
                for msg in history
            ])
            
            # Create prompt for the LLM
            prompt = f"""You are an AI assistant helping to manage iMessages. Please suggest a response to this message:

From: {contact}
Message: {message}

Recent conversation history:
{history_text}

Guidelines:
- Keep responses concise and natural
- Be friendly but professional
- If it's a verification code or automated message, respond with "No response needed"
- If it's a marketing message, respond with "No response needed"
- If it's a spam message, respond with "No response needed"

Suggested response:"""
            
            response = self.llm.invoke(prompt)
            return response.strip()
        except Exception as e:
            return f"Error handling message: {str(e)}"

    def send_message(self, contact: str, message: str) -> bool:
        """Send a message to a contact"""
        try:
            return self.message_service.send_message(contact, message)
        except Exception as e:
            print(f"Error sending message: {str(e)}")
            return False