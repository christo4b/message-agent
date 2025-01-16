import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.message_service import MessageService
from src.db import MessagesDB
from imessage_utils.sender import IMessageSender


def test_message_sending():
    # Initialize dependencies
    sender = IMessageSender()
    
    # Create service
    message_service = MessageService(sender)
    
    # Get test contact from environment variable
    test_contact = os.getenv('TEST_CONTACT')
    if not test_contact:
        print("Error: TEST_CONTACT environment variable not set")
        print("Please set it with: export TEST_CONTACT='+1234567890'")
        return
    
    # Test message
    test_message = "This is a test message from the MessageService. If you receive this, the sending functionality is working!"
    
    try:
        # Test direct message sending first
        success = message_service.send_message(test_contact, test_message)
        if success:
            print(f"✅ Test message sent successfully to {test_contact}")
        else:
            print(f"❌ Failed to send message to {test_contact}")
        
    except Exception as e:
        print(f"❌ Error sending message: {str(e)}")
        raise  # Re-raise the exception for proper test failure

if __name__ == "__main__":
    test_message_sending() 