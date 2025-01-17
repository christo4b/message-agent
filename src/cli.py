import asyncio
import click
import os
from datetime import datetime, timedelta
from .agent import MessageAgent
from .db import MessagesDB
import sqlite3
import re
import subprocess

@click.group()
def cli():
    """CLI for managing iMessages with AI assistance"""
    pass

@cli.command()
@click.option('--days', default=14, help='Number of days to look back')
def review(days):
    """Review pending messages and suggest actions"""
    agent = MessageAgent()
    result = asyncio.run(agent.process_messages(days))
    click.echo(result)

@cli.command()
@click.argument('contact')
@click.argument('message')
def handle(contact, message):
    """Handle a specific message and suggest a response"""
    agent = MessageAgent()
    result = asyncio.run(agent.handle_message(contact, message))
    click.echo(result)

@cli.command()
@click.option('--contact', default=None, help='Contact ID to fetch message threads for.')
@click.option('--limit', default=10, help='Number of recent threads to fetch.')
@click.option('--days', default=7, help='Number of days to look back')
def fetch_recent_threads(contact, limit, days):
    """Fetch recent message threads."""
    agent = MessageAgent()
    if contact:
        threads = agent.tools.get_conversation_history(contact, limit)
        click.echo(f"\nRecent threads for {contact} (last {days} days):")
    else:
        threads = agent.tools.get_pending_messages(days_lookback=days)
        click.echo(f"\nRecent threads from all contacts (last {days} days):")

    if not threads:
        click.echo("No messages found in the specified time range.")
        return

    for thread in threads:
        click.echo("\n---")
        if 'text' in thread:
            click.echo(f"Message: {thread['text']}")
        if 'contact' in thread:
            click.echo(f"From: {thread['contact']}")
        if 'date' in thread:
            click.echo(f"Date: {thread['date']}")
        if 'context' in thread:
            click.echo("Context:")
            for msg in thread['context']:
                click.echo(f"  - {msg['sender']}: {msg['text']}")
        click.echo("---")

@cli.command()
def diagnose():
    """Run diagnostic queries on the messages database"""
    agent = MessageAgent()
    db = agent.message_service.db

    # Check total message count
    query = "SELECT COUNT(*) as count FROM message"
    results = db.execute_query(query)
    click.echo(f"\nTotal messages: {results[0]['count']}")

    # Check message distribution
    query = """
    SELECT 
        date(message.date/1000000000 + 978307200, 'unixepoch', 'localtime') as msg_date,
        COUNT(*) as count
    FROM message
    WHERE msg_date >= date('now', '-7 days')
    GROUP BY msg_date
    ORDER BY msg_date DESC
    """
    results = db.execute_query(query)
    click.echo("\nMessage counts by date (last 7 days):")
    for row in results:
        click.echo(f"{row['msg_date']}: {row['count']} messages")

    # Check contacts
    query = """
    SELECT 
        h.id as contact,
        COUNT(m.ROWID) as message_count,
        MAX(datetime(m.date/1000000000 + 978307200, 'unixepoch', 'localtime')) as last_message
    FROM handle h
    LEFT JOIN message m ON h.ROWID = m.handle_id
    GROUP BY h.id
    ORDER BY last_message DESC
    LIMIT 5
    """
    results = db.execute_query(query)
    click.echo("\nTop 5 contacts by recent activity:")
    for row in results:
        click.echo(f"Contact: {row['contact']}")
        click.echo(f"  Message count: {row['message_count']}")
        click.echo(f"  Last message: {row['last_message']}")

    # Show some recent messages
    query = """
    SELECT 
        datetime(message.date/1000000000 + 978307200, 'unixepoch', 'localtime') as msg_time,
        handle.id as contact,
        message.text,
        message.is_from_me
    FROM message 
    JOIN handle ON message.handle_id = handle.ROWID
    WHERE message.text IS NOT NULL
    ORDER BY message.date DESC
    LIMIT 5
    """
    results = db.execute_query(query)
    click.echo("\nMost recent messages:")
    for row in results:
        direction = "→" if row['is_from_me'] else "←"
        click.echo(f"[{row['msg_time']}] {direction} {row['contact']}: {row['text'][:100]}")

@cli.command()
@click.argument('contact')
def lookup_contact(contact):
    """Look up all messages for a specific contact"""
    agent = MessageAgent()
    db = agent.message_service.db

    # Get all messages for this contact
    query = """
    SELECT 
        datetime(message.date/1000000000 + 978307200, 'unixepoch', 'localtime') as msg_time,
        message.text,
        message.is_from_me,
        message.ROWID as msg_id
    FROM message 
    JOIN handle ON message.handle_id = handle.ROWID
    WHERE handle.id = ?
    AND message.text IS NOT NULL
    ORDER BY message.date ASC
    """
    results = db.execute_query(query, (contact,))
    
    if not results:
        click.echo(f"\nNo messages found for contact: {contact}")
        return

    click.echo(f"\nFound {len(results)} messages for {contact}:")
    for row in results:
        direction = "→" if row['is_from_me'] else "←"
        click.echo(f"[{row['msg_time']}] {direction} {row['text']}")

@cli.command()
def debug_timestamps():
    """Debug database timestamps and recent messages"""
    agent = MessageAgent()
    db = agent.message_service.db

    # Check the most recent message timestamp
    query1 = """
    SELECT 
        MAX(date) as latest_timestamp,
        date/1000000000 + 978307200 as unix_timestamp,
        datetime(date/1000000000 + 978307200, 'unixepoch', 'localtime') as formatted_time
    FROM message
    """
    results = db.execute_query(query1)
    click.echo("\nMost recent message timestamp:")
    click.echo(f"Raw timestamp: {results[0]['latest_timestamp']}")
    click.echo(f"Unix timestamp: {results[0]['unix_timestamp']}")
    click.echo(f"Formatted time: {results[0]['formatted_time']}")

    # Compare with current time
    current_time = int(datetime.now().timestamp())
    click.echo(f"\nCurrent time (Unix): {current_time}")
    click.echo(f"Current time (Formatted): {datetime.fromtimestamp(current_time).strftime('%Y-%m-%d %H:%M:%S')}")

    # Show message distribution over the last 24 hours
    query2 = """
    SELECT 
        strftime('%Y-%m-%d %H:00:00', datetime(date/1000000000 + 978307200, 'unixepoch', 'localtime')) as hour,
        COUNT(*) as count
    FROM message
    WHERE date/1000000000 + 978307200 >= strftime('%s', 'now', '-1 day')
    GROUP BY hour
    ORDER BY hour DESC
    """
    results = db.execute_query(query2)
    click.echo("\nMessage counts by hour (last 24 hours):")
    for row in results:
        click.echo(f"{row['hour']}: {row['count']} messages")

    # Check for any gaps in the data
    query3 = """
    SELECT 
        datetime(date/1000000000 + 978307200, 'unixepoch', 'localtime') as msg_time,
        text,
        is_from_me,
        handle.id as contact
    FROM message
    JOIN handle ON message.handle_id = handle.ROWID
    WHERE date/1000000000 + 978307200 >= strftime('%s', 'now', '-1 day')
    AND text IS NOT NULL
    ORDER BY date DESC
    LIMIT 10
    """
    results = db.execute_query(query3)
    click.echo("\nLast 10 messages with timestamps:")
    for row in results:
        direction = "→" if row['is_from_me'] else "←"
        click.echo(f"[{row['msg_time']}] {direction} {row['contact']}: {row['text'][:100]}")

    # Check database file info
    db_path = db.db_path
    if os.path.exists(db_path):
        stat = os.stat(db_path)
        click.echo(f"\nDatabase file info:")
        click.echo(f"Path: {db_path}")
        click.echo(f"Size: {stat.st_size:,} bytes")
        click.echo(f"Last modified: {datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')}")
    else:
        click.echo(f"\nWarning: Database file not found at {db_path}")

@cli.command()
def check_tables():
    """Check database tables and their contents"""
    agent = MessageAgent()
    db = agent.message_service.db

    # List all tables
    query1 = """
    SELECT name FROM sqlite_master 
    WHERE type='table'
    ORDER BY name;
    """
    results = db.execute_query(query1)
    click.echo("\nTables in database:")
    for row in results:
        click.echo(f"- {row['name']}")

    # Check handle table
    query2 = """
    SELECT COUNT(*) as count, 
           MIN(ROWID) as min_id, 
           MAX(ROWID) as max_id 
    FROM handle;
    """
    results = db.execute_query(query2)
    click.echo("\nHandle table stats:")
    click.echo(f"Count: {results[0]['count']}")
    click.echo(f"ID range: {results[0]['min_id']} to {results[0]['max_id']}")

    # Sample some handles
    query3 = """
    SELECT ROWID, id 
    FROM handle 
    LIMIT 5;
    """
    results = db.execute_query(query3)
    click.echo("\nSample handles:")
    for row in results:
        click.echo(f"ROWID: {row['ROWID']}, ID: {row['id']}")

    # Check message table
    query4 = """
    SELECT COUNT(*) as count,
           MIN(ROWID) as min_id,
           MAX(ROWID) as max_id,
           COUNT(DISTINCT handle_id) as unique_handles,
           SUM(CASE WHEN text IS NOT NULL THEN 1 ELSE 0 END) as messages_with_text
    FROM message;
    """
    results = db.execute_query(query4)
    click.echo("\nMessage table stats:")
    click.echo(f"Total count: {results[0]['count']}")
    click.echo(f"ID range: {results[0]['min_id']} to {results[0]['max_id']}")
    click.echo(f"Unique handles: {results[0]['unique_handles']}")
    click.echo(f"Messages with text: {results[0]['messages_with_text']}")

    # Sample some recent messages directly
    query5 = """
    SELECT m.ROWID, m.handle_id, m.text, m.date, h.id as contact
    FROM message m
    LEFT JOIN handle h ON m.handle_id = h.ROWID
    WHERE m.text IS NOT NULL
    ORDER BY m.ROWID DESC
    LIMIT 5;
    """
    results = db.execute_query(query5)
    click.echo("\nSample recent messages (raw data):")
    for row in results:
        click.echo(f"ROWID: {row['ROWID']}")
        click.echo(f"Handle ID: {row['handle_id']}")
        click.echo(f"Contact: {row['contact']}")
        click.echo(f"Date: {row['date']}")
        click.echo(f"Text: {row['text'][:100]}")
        click.echo("---")

@cli.command()
@click.argument('contact')
@click.option('--hours', default=24, help='Hours to look back')
def verify_messages(contact, hours):
    """Verify recent messages for a contact and check database sync"""
    agent = MessageAgent()
    db = agent.message_service.db

    # Get database info
    db_path = db.db_path
    stat = os.stat(db_path)
    click.echo(f"\nDatabase info:")
    click.echo(f"Path: {db_path}")
    click.echo(f"Size: {stat.st_size:,} bytes")
    click.echo(f"Last modified: {datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')}")

    # Check handle exists
    query1 = "SELECT ROWID, id FROM handle WHERE id = ?"
    results = db.execute_query(query1, (contact,))
    if not results:
        click.echo(f"\nWarning: Contact {contact} not found in handle table")
        return
    handle_id = results[0]['ROWID']
    click.echo(f"\nFound contact in handle table (ROWID: {handle_id})")

    # Get recent messages with detailed info
    query2 = """
    SELECT 
        m.ROWID,
        m.text,
        m.date,
        m.date/1000000000 + 978307200 as unix_timestamp,
        datetime(m.date/1000000000 + 978307200, 'unixepoch', 'localtime') as formatted_time,
        m.is_from_me,
        m.handle_id
    FROM message m
    WHERE m.handle_id = ?
    AND m.date/1000000000 + 978307200 >= strftime('%s', 'now', '-' || ? || ' hours')
    ORDER BY m.date DESC
    """
    results = db.execute_query(query2, (handle_id, hours))
    
    click.echo(f"\nFound {len(results)} messages in the last {hours} hours:")
    for row in results:
        direction = "→" if row['is_from_me'] else "←"
        click.echo("\n---")
        click.echo(f"ROWID: {row['ROWID']}")
        click.echo(f"Time: {row['formatted_time']}")
        click.echo(f"Raw date: {row['date']}")
        click.echo(f"Unix timestamp: {row['unix_timestamp']}")
        click.echo(f"Direction: {direction}")
        click.echo(f"Text: {row['text']}")

    # Check for gaps in message sequence
    query3 = """
    WITH MessageGaps AS (
        SELECT 
            ROWID,
            date,
            LAG(date) OVER (ORDER BY date) as prev_date,
            LEAD(date) OVER (ORDER BY date) as next_date
        FROM message
        WHERE handle_id = ?
        AND date/1000000000 + 978307200 >= strftime('%s', 'now', '-' || ? || ' hours')
    )
    SELECT *
    FROM MessageGaps
    WHERE (next_date - date) > 3600000000000  -- Gap larger than 1 hour
    OR (date - prev_date) > 3600000000000
    """
    results = db.execute_query(query3, (handle_id, hours))
    if results:
        click.echo("\nFound potential gaps in message sequence:")
        for row in results:
            click.echo(f"Message ID {row['ROWID']} has unusual time gap with adjacent messages")

@cli.command()
@click.argument('contact')
@click.option('--limit', default=10, help='Number of messages to show')
def raw_messages(contact, limit):
    """Show raw message data for a contact without timestamp filtering"""
    agent = MessageAgent()
    db = agent.message_service.db

    # First check the handle
    query1 = "SELECT ROWID, id FROM handle WHERE id = ?"
    results = db.execute_query(query1, (contact,))
    if not results:
        click.echo(f"\nWarning: Contact {contact} not found in handle table")
        return
    handle_id = results[0]['ROWID']
    click.echo(f"\nFound contact in handle table (ROWID: {handle_id})")

    # Get raw message data
    query2 = """
    SELECT 
        m.ROWID,
        m.text,
        m.date as raw_date,
        m.is_from_me,
        m.handle_id,
        h.id as contact_id,
        CASE 
            WHEN m.date IS NULL THEN 'NULL'
            WHEN m.date = 0 THEN 'ZERO'
            ELSE 'VALID'
        END as date_status
    FROM message m
    JOIN handle h ON m.handle_id = h.ROWID
    WHERE m.handle_id = ?
    ORDER BY m.ROWID DESC
    LIMIT ?
    """
    results = db.execute_query(query2, (handle_id, limit))
    
    click.echo(f"\nLast {limit} messages (raw data):")
    for row in results:
        click.echo("\n---")
        click.echo(f"ROWID: {row['ROWID']}")
        click.echo(f"Raw date: {row['raw_date']}")
        click.echo(f"Date status: {row['date_status']}")
        click.echo(f"Direction: {'→' if row['is_from_me'] else '←'}")
        click.echo(f"Text: {row['text']}")

    # Get message table stats for this contact
    query3 = """
    SELECT 
        COUNT(*) as total_count,
        COUNT(CASE WHEN date IS NOT NULL THEN 1 END) as with_date,
        COUNT(CASE WHEN text IS NOT NULL THEN 1 END) as with_text,
        MIN(date) as min_date,
        MAX(date) as max_date
    FROM message
    WHERE handle_id = ?
    """
    results = db.execute_query(query3, (handle_id,))
    stats = results[0]
    
    click.echo(f"\nMessage stats for this contact:")
    click.echo(f"Total messages: {stats['total_count']}")
    click.echo(f"Messages with dates: {stats['with_date']}")
    click.echo(f"Messages with text: {stats['with_text']}")
    if stats['min_date'] is not None:
        min_time = stats['min_date']/1000000000 + 978307200
        max_time = stats['max_date']/1000000000 + 978307200
        click.echo(f"Date range: {datetime.fromtimestamp(min_time)} to {datetime.fromtimestamp(max_time)}")

@cli.command()
@click.argument('contact')
@click.option('--limit', default=10, help='Number of messages to show')
def check_group_messages(contact, limit):
    """Check group messages involving a specific contact"""
    agent = MessageAgent()
    db = agent.message_service.db

    # First check the handle
    query1 = "SELECT ROWID, id FROM handle WHERE id = ?"
    results = db.execute_query(query1, (contact,))
    if not results:
        click.echo(f"\nWarning: Contact {contact} not found in handle table")
        return
    handle_id = results[0]['ROWID']
    click.echo(f"\nFound contact in handle table (ROWID: {handle_id})")

    # Get group messages
    query2 = """
    SELECT DISTINCT
        m.ROWID,
        m.text,
        m.date,
        datetime(m.date/1000000000 + 978307200, 'unixepoch', 'localtime') as formatted_time,
        m.is_from_me,
        m.cache_roomnames,
        m.group_title,
        h.id as sender_id
    FROM message m
    JOIN handle h ON m.handle_id = h.ROWID
    WHERE (m.cache_roomnames IS NOT NULL OR m.group_title IS NOT NULL)
    AND (
        m.handle_id = ? 
        OR m.cache_roomnames LIKE '%' || ? || '%'
        OR EXISTS (
            SELECT 1 
            FROM message m2 
            WHERE m2.cache_roomnames = m.cache_roomnames 
            AND m2.handle_id = ?
        )
    )
    ORDER BY m.date DESC
    LIMIT ?
    """
    results = db.execute_query(query2, (handle_id, contact, handle_id, limit))
    
    click.echo(f"\nLast {limit} group messages involving this contact:")
    for row in results:
        click.echo("\n---")
        click.echo(f"Time: {row['formatted_time']}")
        click.echo(f"From: {row['sender_id']}")
        click.echo(f"Group: {row['cache_roomnames'] or row['group_title'] or 'Unknown group'}")
        click.echo(f"Direction: {'→' if row['is_from_me'] else '←'}")
        click.echo(f"Text: {row['text']}")

    # Get group chat stats
    query3 = """
    SELECT DISTINCT
        m.cache_roomnames,
        m.group_title,
        COUNT(*) as message_count,
        MIN(datetime(m.date/1000000000 + 978307200, 'unixepoch', 'localtime')) as first_message,
        MAX(datetime(m.date/1000000000 + 978307200, 'unixepoch', 'localtime')) as last_message
    FROM message m
    WHERE (m.cache_roomnames IS NOT NULL OR m.group_title IS NOT NULL)
    AND (
        m.handle_id = ?
        OR m.cache_roomnames LIKE '%' || ? || '%'
        OR EXISTS (
            SELECT 1 
            FROM message m2 
            WHERE m2.cache_roomnames = m.cache_roomnames 
            AND m2.handle_id = ?
        )
    )
    GROUP BY m.cache_roomnames, m.group_title
    ORDER BY last_message DESC
    """
    results = db.execute_query(query3, (handle_id, contact, handle_id))
    
    click.echo("\nGroup chat statistics:")
    for row in results:
        click.echo("\n---")
        click.echo(f"Group: {row['cache_roomnames'] or row['group_title'] or 'Unknown group'}")
        click.echo(f"Message count: {row['message_count']}")
        click.echo(f"First message: {row['first_message']}")
        click.echo(f"Last message: {row['last_message']}")

@cli.command()
def dump_today():
    """Dump all messages from today with all fields"""
    agent = MessageAgent()
    db = agent.message_service.db

    query = """
    SELECT 
        m.*,
        datetime(m.date/1000000000 + 978307200, 'unixepoch', 'localtime') as formatted_time,
        h.id as contact_id
    FROM message m
    LEFT JOIN handle h ON m.handle_id = h.ROWID
    WHERE date(m.date/1000000000 + 978307200, 'unixepoch', 'localtime') = date('now')
    ORDER BY m.date DESC
    """
    results = db.execute_query(query)
    
    click.echo(f"\nFound {len(results)} messages from today:")
    for row in results:
        click.echo("\n" + "="*50)
        for key, value in row.items():
            if value is not None:  # Only show non-NULL values
                click.echo(f"{key}: {value}")

    # Also check for any messages that might have invalid dates
    query2 = """
    SELECT 
        m.*,
        h.id as contact_id
    FROM message m
    LEFT JOIN handle h ON m.handle_id = h.ROWID
    WHERE m.date IS NULL 
       OR m.date = 0 
       OR m.date > strftime('%s', 'now') * 1000000000 + 978307200000000000
    ORDER BY m.ROWID DESC
    LIMIT 10
    """
    results = db.execute_query(query2)
    
    if results:
        click.echo("\nFound messages with unusual dates:")
        for row in results:
            click.echo("\n" + "="*50)
            for key, value in row.items():
                if value is not None:
                    click.echo(f"{key}: {value}")

@cli.command()
@click.argument('contact')
@click.option('--days', default=1, help='Number of days to look back')
def check_contact_groups(contact, days):
    """Check all group messages involving a specific contact"""
    agent = MessageAgent()
    db = agent.message_service.db

    # First find the specific chat we know exists
    query1 = """
    SELECT 
        c.ROWID as chat_id,
        c.chat_identifier,
        c.display_name,
        COUNT(DISTINCT m.ROWID) as message_count,
        MIN(datetime(m.date/1000000000 + 978307200, 'unixepoch', 'localtime')) as first_message,
        MAX(datetime(m.date/1000000000 + 978307200, 'unixepoch', 'localtime')) as last_message
    FROM chat c
    JOIN chat_message_join cmj ON c.ROWID = cmj.chat_id
    JOIN message m ON cmj.message_id = m.ROWID
    WHERE c.chat_identifier = 'chat363848444324532031'
    GROUP BY c.ROWID, c.chat_identifier, c.display_name
    """
    
    chats = db.execute_query(query1)
    
    if not chats:
        click.echo(f"\nCould not find chat363848444324532031")
        return
        
    chat = chats[0]
    click.echo(f"\nFound chat:")
    click.echo(f"  Identifier: {chat['chat_identifier']}")
    click.echo(f"  Display Name: {chat['display_name'] or 'Not set'}")
    click.echo(f"  Total Messages: {chat['message_count']}")
    click.echo(f"  First Message: {chat['first_message']}")
    click.echo(f"  Last Message: {chat['last_message']}")
    
    # Get recent messages in this chat
    query2 = """
    SELECT 
        m.ROWID,
        m.date as raw_date,
        datetime(m.date/1000000000 + 978307200, 'unixepoch', 'localtime') as time,
        m.text,
        hex(m.attributedBody) as attributed_body_hex,
        h.id as sender,
        m.is_from_me,
        m.service,
        m.account,
        m.cache_has_attachments,
        (
            SELECT GROUP_CONCAT(filename)
            FROM attachment
            JOIN message_attachment_join 
            ON attachment.ROWID = message_attachment_join.attachment_id
            WHERE message_attachment_join.message_id = m.ROWID
        ) as attachments
    FROM message m
    JOIN handle h ON m.handle_id = h.ROWID
    JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
    WHERE cmj.chat_id = ?
    AND m.date >= (
        SELECT MAX(m2.date) - (? * 24 * 60 * 60 * 1000000000)
        FROM message m2
        JOIN chat_message_join cmj2 ON m2.ROWID = cmj2.message_id
        WHERE cmj2.chat_id = cmj.chat_id
    )
    ORDER BY m.date DESC
    """
    
    messages = db.execute_query(query2, (chat['chat_id'], str(days)))
    
    if messages:
        click.echo(f"\nRecent Messages ({len(messages)}):")
        for msg in messages:
            click.echo("\n---")
            click.echo(f"ROWID: {msg['ROWID']}")
            click.echo(f"Raw Date: {msg['raw_date']}")
            click.echo(f"Time: {msg['time']}")
            click.echo(f"From: {msg['sender']}")
            click.echo(f"Service: {msg['service']} ({msg['account'] or 'default account'})")
            click.echo(f"Direction: {'→' if msg['is_from_me'] else '←'}")
            
            # Get text from attributedBody if text is empty
            text = msg['text']
            if not text and msg['attributed_body_hex']:
                try:
                    blob = bytes.fromhex(msg['attributed_body_hex'])
                    text = blob.decode('utf-8', errors='ignore')
                    # Clean up the text
                    if text.startswith('streamtyped@'):
                        text = text[len('streamtyped@'):]
                    if 'NSString+' in text:
                        text = text.split('NSString+')[1]
                    if 'i__kIMMessagePartAttributeName' in text:
                        text = text.split('i__kIMMessagePartAttributeName')[0]
                    text = text.strip()
                except:
                    text = None
                    
            click.echo(f"Text: {text or '(empty)'}")
            
            if msg['cache_has_attachments'] and msg['attachments']:
                click.echo(f"Attachments: {msg['attachments']}")
    else:
        click.echo("\nNo other messages found in the same group")

@cli.command()
@click.option('--days', default=1, help='Number of days to look back')
@click.option('--contact', help='Contact ID to filter by')
def debug_messages(days, contact):
    """Debug message retrieval by showing raw query results"""
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'messages.db')
    db = MessagesDB(db_path)
    
    # Use a reference timestamp and buffer like in the working query
    reference_time = 758751495079831168  # From the working query
    buffer = 1000000000000  # ~16 minutes buffer
    
    query = """
    SELECT 
        message.ROWID,
        message.text,
        message.service,
        message.account,
        message.date,
        message.is_from_me,
        message.cache_has_attachments,
        COALESCE(chat.display_name, message.cache_roomnames) as group_name,
        COALESCE(chat.chat_identifier, message.group_title) as group_id,
        handle.id as contact_id,
        datetime(message.date/1000000000 + 978307200, 'unixepoch', 'localtime') as formatted_time
    FROM message
    JOIN handle ON message.handle_id = handle.ROWID
    LEFT JOIN chat_message_join ON message.ROWID = chat_message_join.message_id
    LEFT JOIN chat ON chat_message_join.chat_id = chat.ROWID
    WHERE message.date BETWEEN ? - ? AND ? + ?
    """
    
    if contact:
        query += " AND handle.id = ?"
        params = (reference_time, buffer, reference_time, buffer, contact)
    else:
        params = (reference_time, buffer, reference_time, buffer)
    
    query += " ORDER BY message.date DESC"
    
    with sqlite3.connect(db.db_path) as conn:
        cursor = conn.execute(query, params)
        
        results = cursor.fetchall()
        click.echo(f"\nFound {len(results)} messages:\n")
        
        for row in results:
            click.echo("=" * 50)
            click.echo(f"ID: {row[0]}")
            click.echo(f"Time: {row[10]}")  # formatted_time
            click.echo(f"From Me: {bool(row[5])}")
            click.echo(f"Service: {row[2]}")
            click.echo(f"Account: {row[3]}")
            click.echo(f"Group: {row[7] or 'N/A'}")
            click.echo(f"Group ID: {row[8] or 'N/A'}")
            click.echo(f"Contact: {row[9]}")
            click.echo(f"Has Attachments: {bool(row[6])}")
            click.echo(f"Text: {row[1] or '(empty)'}")
            click.echo()

@cli.command()
@click.option('--days', default=1, help='Number of days to look back')
def debug_sql(days):
    """Debug SQL query execution for message retrieval"""
    db_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'messages.db')
    
    # Get current timestamp and cutoff
    now = int(datetime.now().timestamp())
    cutoff = int((datetime.now() - timedelta(days=days)).timestamp())
    
    click.echo(f"\nDebug info:")
    click.echo(f"Current time: {datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S')}")
    click.echo(f"Cutoff time: {datetime.fromtimestamp(cutoff).strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Convert to Apple epoch
    now_apple = (now - 978307200) * 1000000000
    cutoff_apple = (cutoff - 978307200) * 1000000000
    
    click.echo(f"\nApple epoch values:")
    click.echo(f"Current time: {now_apple}")
    click.echo(f"Cutoff time: {cutoff_apple}")
    
    # Connect directly to get binary data
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        query = """
        SELECT 
            message.ROWID,
            message.text,
            message.attributedBody,
            message.date,
            message.date/1000000000 + 978307200 as unix_timestamp,
            datetime(message.date/1000000000 + 978307200, 'unixepoch', 'localtime') as formatted_time,
            message.is_from_me,
            message.service,
            message.account,
            message.cache_has_attachments,
            COALESCE(chat.display_name, message.cache_roomnames) as group_name,
            COALESCE(chat.chat_identifier, message.group_title) as group_id,
            handle.id as contact_id,
            (
                SELECT GROUP_CONCAT(filename)
                FROM attachment
                JOIN message_attachment_join 
                ON attachment.ROWID = message_attachment_join.attachment_id
                WHERE message_attachment_join.message_id = message.ROWID
            ) as attachments
        FROM message
        JOIN handle ON message.handle_id = handle.ROWID
        LEFT JOIN chat_message_join ON message.ROWID = chat_message_join.message_id
        LEFT JOIN chat ON chat_message_join.chat_id = chat.ROWID
        WHERE message.date >= ?
        ORDER BY message.date DESC
        LIMIT 10
        """
        
        cursor = conn.execute(query, (cutoff_apple,))
        results = cursor.fetchall()
    
    def extract_text_from_blob(blob):
        if not blob:
            return None
        try:
            # Try different text extraction methods
            text = None
            
            # Method 1: Direct UTF-8 decode
            try:
                text = blob.decode('utf-8', errors='ignore')
            except:
                pass
            
            if text:
                # Remove common prefixes
                prefixes = [
                    'streamtyped@NSObject',
                    'streamtyped@NSMutableAttributedString',
                    'streamtyped@',
                    'NSObject',
                    'NSMutableString',
                    'NSString+',
                ]
                for prefix in prefixes:
                    if text.startswith(prefix):
                        text = text[len(prefix):]
                
                # Remove common suffixes and their variations
                suffixes = [
                    'i__kIMMessagePartAttributeName',
                    'iIi__kIMMessagePartAttributeName',
                    'iI i__kIMMessagePartAttributeName',
                    'iI.i__kIMMessagePartAttributeName',
                    'iI9i__kIMMessagePartAttributeName',
                    'NSNumberNSValue*',
                    'i&__kIMBaseWritingDirectionAttributeName',
                    'i"__kIMFileTransferGUIDAttributeName',
                    'q__kIMMessagePartAttributeName',
                    'Mi__kIMMessagePartAttributeName',
                    '&__kIMDataDetectedAttributeName',
                    '__kIMLinkAttributeName',
                ]
                for suffix in suffixes:
                    if suffix in text:
                        text = text.split(suffix)[0]
                
                # Clean up the text
                text = text.replace('\x00', '')
                text = ''.join(c for c in text if c.isprintable() or c in ['\n', ' '])
                
                # Remove any remaining markers and their variations
                markers = [
                    'NSString+',
                    'NSDictionary',
                    'NSAttributedString',
                    'NSMutableString',
                    'NSObject',
                    'iI',
                    'iIM',
                    'NSData',
                    'NSKeyedArchiver',
                    'bplist00',
                ]
                for marker in markers:
                    text = text.replace(marker, '')
                
                # Remove any remaining control characters and extra whitespace
                text = ' '.join(text.split())
                
                # Remove any remaining single character markers
                text = ' '.join(word for word in text.split() if len(word) > 1 or word.isalnum())
                
                # Clean up URLs
                if 'http' in text:
                    parts = text.split('http')
                    text = parts[0].strip()
                    if len(parts) > 1:
                        url = 'http' + parts[1].split()[0]
                        text = f"{text} {url}"
                
                # Remove any remaining metadata markers
                text = text.replace('at_0_', '')
                text = re.sub(r'[0-9A-F]{8}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{4}-[0-9A-F]{12}', '', text, flags=re.IGNORECASE)
                
                # Clean up any remaining artifacts
                text = text.replace('￼', '')
                text = re.sub(r'^\W+', '', text)  # Remove leading non-word characters
                text = re.sub(r'\W+$', '', text)  # Remove trailing non-word characters
                text = re.sub(r'\s+', ' ', text)  # Normalize whitespace
                
                return text.strip() if text.strip() else None
            
        except Exception as e:
            click.echo(f"Error extracting text: {e}")
        return None
    
    click.echo(f"\nFound {len(results)} messages since {datetime.fromtimestamp(cutoff).strftime('%Y-%m-%d %H:%M:%S')}:")
    for row in results:
        click.echo("\n---")
        click.echo(f"ROWID: {row['ROWID']}")
        click.echo(f"Time: {row['formatted_time']}")
        click.echo(f"Raw date: {row['date']}")
        click.echo(f"Unix timestamp: {row['unix_timestamp']}")
        click.echo(f"Service: {row['service']}")
        click.echo(f"Account: {row['account']}")
        click.echo(f"Contact: {row['contact_id']}")
        click.echo(f"Group: {row['group_name'] or row['group_id'] or 'N/A'}")
        click.echo(f"Direction: {'→' if row['is_from_me'] else '←'}")
        click.echo(f"Has attachments: {bool(row['cache_has_attachments'])}")
        if row['attachments']:
            click.echo(f"Attachments: {row['attachments']}")
        
        # Try to get text content
        text = row['text']
        if not text:
            text = extract_text_from_blob(row['attributedBody'])
        if text:
            click.echo(f"Text: {text}")

@cli.command()
@click.argument('contact')
@click.option('--days', default=1, help='Number of days to look back')
def debug_contact(contact, days):
    """Debug all information about a contact's messages"""
    agent = MessageAgent()
    db = agent.message_service.db
    
    # Get current time and cutoff
    now = int(datetime.now().timestamp())
    cutoff = int((datetime.now() - timedelta(days=days)).timestamp())
    
    # Convert to Apple epoch
    now_apple = (now - 978307200) * 1000000000
    cutoff_apple = (cutoff - 978307200) * 1000000000
    
    click.echo(f"\nDebug info for contact {contact}:")
    click.echo(f"Current time: {datetime.fromtimestamp(now).strftime('%Y-%m-%d %H:%M:%S')}")
    click.echo(f"Cutoff time: {datetime.fromtimestamp(cutoff).strftime('%Y-%m-%d %H:%M:%S')}")
    
    # First check handle table
    query1 = """
    SELECT *
    FROM handle
    WHERE id = ?
    """
    results = db.execute_query(query1, (contact,))
    click.echo(f"\nHandle information:")
    for row in results:
        for key, value in row.items():
            click.echo(f"  {key}: {value}")
    
    # Check for any messages involving this handle
    query2 = """
    SELECT 
        m.ROWID,
        datetime(m.date/1000000000 + 978307200, 'unixepoch', 'localtime') as time,
        m.text,
        m.is_from_me,
        m.service,
        m.account,
        m.cache_roomnames,
        m.group_title,
        c.chat_identifier,
        c.display_name,
        c.ROWID as chat_id
    FROM message m
    JOIN handle h ON m.handle_id = h.ROWID
    LEFT JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
    LEFT JOIN chat c ON cmj.chat_id = c.ROWID
    WHERE h.id = ?
    AND m.date >= ?
    ORDER BY m.date DESC
    """
    results = db.execute_query(query2, (contact, cutoff_apple))
    click.echo(f"\nFound {len(results)} direct messages:")
    for row in results:
        click.echo("\n---")
        for key, value in row.items():
            if value is not None:
                click.echo(f"  {key}: {value}")
    
    # Check for group chats containing this handle
    query3 = """
    SELECT DISTINCT
        c.ROWID as chat_id,
        c.chat_identifier,
        c.display_name,
        m.cache_roomnames,
        m.group_title,
        COUNT(DISTINCT m.ROWID) as message_count,
        MIN(datetime(m.date/1000000000 + 978307200, 'unixepoch', 'localtime')) as first_message,
        MAX(datetime(m.date/1000000000 + 978307200, 'unixepoch', 'localtime')) as last_message
    FROM message m
    JOIN handle h ON m.handle_id = h.ROWID
    LEFT JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
    LEFT JOIN chat c ON cmj.chat_id = c.ROWID
    WHERE h.id = ?
    GROUP BY COALESCE(c.ROWID, m.cache_roomnames, m.group_title)
    HAVING MAX(m.date) >= ?
    """
    results = db.execute_query(query3, (contact, cutoff_apple))
    click.echo(f"\nFound {len(results)} group chats:")
    for row in results:
        click.echo("\n---")
        for key, value in row.items():
            if value is not None:
                click.echo(f"  {key}: {value}")
    
    # Finally check for messages in any group where this handle appears
    query4 = """
    SELECT DISTINCT
        m.ROWID,
        datetime(m.date/1000000000 + 978307200, 'unixepoch', 'localtime') as time,
        m.text,
        h.id as sender,
        m.is_from_me,
        m.service,
        m.account,
        COALESCE(c.display_name, m.cache_roomnames) as group_name,
        COALESCE(c.chat_identifier, m.group_title) as group_id
    FROM message m
    JOIN handle h ON m.handle_id = h.ROWID
    LEFT JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
    LEFT JOIN chat c ON cmj.chat_id = c.ROWID
    WHERE m.date >= ?
    AND (
        h.id = ?
        OR EXISTS (
            SELECT 1
            FROM message m2
            JOIN handle h2 ON m2.handle_id = h2.ROWID
            LEFT JOIN chat_message_join cmj2 ON m2.ROWID = cmj2.message_id
            WHERE h2.id = ?
            AND (
                (cmj2.chat_id = cmj.chat_id AND cmj.chat_id IS NOT NULL)
                OR (m2.cache_roomnames = m.cache_roomnames AND m.cache_roomnames IS NOT NULL)
                OR (m2.group_title = m.group_title AND m.group_title IS NOT NULL)
            )
        )
    )
    ORDER BY m.date DESC
    """
    results = db.execute_query(query4, (cutoff_apple, contact, contact))
    click.echo(f"\nFound {len(results)} messages in groups with this contact:")
    for row in results:
        click.echo("\n---")
        for key, value in row.items():
            if value is not None:
                click.echo(f"  {key}: {value}")

@cli.command()
@click.argument('message_id', type=int)
def debug_message_group(message_id):
    """Debug a specific message and its group chat context"""
    agent = MessageAgent()
    db = agent.message_service.db
    
    # First get the message details
    query1 = """
    SELECT 
        m.ROWID,
        m.text,
        hex(m.attributedBody) as attributed_body_hex,
        m.date,
        datetime(m.date/1000000000 + 978307200, 'unixepoch', 'localtime') as formatted_time,
        m.is_from_me,
        h.id as sender,
        m.service,
        m.account,
        m.cache_roomnames,
        m.group_title,
        c.ROWID as chat_id,
        c.chat_identifier,
        c.display_name,
        m.cache_has_attachments,
        (
            SELECT GROUP_CONCAT(filename)
            FROM attachment
            JOIN message_attachment_join 
            ON attachment.ROWID = message_attachment_join.attachment_id
            WHERE message_attachment_join.message_id = m.ROWID
        ) as attachments
    FROM message m
    JOIN handle h ON m.handle_id = h.ROWID
    LEFT JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
    LEFT JOIN chat c ON cmj.chat_id = c.ROWID
    WHERE m.ROWID = ?
    """
    
    message = db.execute_query(query1, (message_id,))
    if not message:
        click.echo(f"No message found with ID {message_id}")
        return
        
    message = message[0]
    click.echo("\nMessage Details:")
    for key, value in message.items():
        if value is not None:
            click.echo(f"  {key}: {value}")
            
    # Get text from attributedBody if text is empty
    text = message['text']
    if not text and message['attributed_body_hex']:
        try:
            blob = bytes.fromhex(message['attributed_body_hex'])
            text = blob.decode('utf-8', errors='ignore')
            # Clean up the text
            if text.startswith('streamtyped@'):
                text = text[len('streamtyped@'):]
            if 'NSString+' in text:
                text = text.split('NSString+')[1]
            if 'i__kIMMessagePartAttributeName' in text:
                text = text.split('i__kIMMessagePartAttributeName')[0]
            text = text.strip()
            click.echo(f"\nDecoded Text: {text}")
        except Exception as e:
            click.echo(f"Error decoding text: {e}")
            
    # Now get other messages in the same group
    query2 = """
    SELECT 
        m.ROWID,
        datetime(m.date/1000000000 + 978307200, 'unixepoch', 'localtime') as time,
        m.text,
        hex(m.attributedBody) as attributed_body_hex,
        h.id as sender,
        m.is_from_me,
        m.service,
        m.account
    FROM message m
    JOIN handle h ON m.handle_id = h.ROWID
    LEFT JOIN chat_message_join cmj ON m.ROWID = cmj.message_id
    LEFT JOIN chat c ON cmj.chat_id = c.ROWID
    WHERE (
        (c.ROWID = ? AND ? IS NOT NULL)
        OR (m.cache_roomnames = ? AND ? IS NOT NULL)
        OR (m.group_title = ? AND ? IS NOT NULL)
    )
    AND m.ROWID != ?
    AND ABS(m.date - ?) < 86400000000000  -- Messages within 24 hours
    ORDER BY m.date DESC
    """
    
    chat_id = message['chat_id']
    cache_roomnames = message['cache_roomnames']
    group_title = message['group_title']
    
    messages = db.execute_query(query2, (
        chat_id, chat_id,
        cache_roomnames, cache_roomnames,
        group_title, group_title,
        message_id,
        message['date']
    ))
    
    if messages:
        click.echo(f"\nFound {len(messages)} other messages in the same group:")
        for msg in messages:
            click.echo("\n---")
            click.echo(f"Time: {msg['time']}")
            click.echo(f"From: {msg['sender']}")
            click.echo(f"Service: {msg['service']} ({msg['account'] or 'default account'})")
            click.echo(f"Direction: {'→' if msg['is_from_me'] else '←'}")
            
            # Get text from attributedBody if text is empty
            text = msg['text']
            if not text and msg['attributed_body_hex']:
                try:
                    blob = bytes.fromhex(msg['attributed_body_hex'])
                    text = blob.decode('utf-8', errors='ignore')
                    # Clean up the text
                    if text.startswith('streamtyped@'):
                        text = text[len('streamtyped@'):]
                    if 'NSString+' in text:
                        text = text.split('NSString+')[1]
                    if 'i__kIMMessagePartAttributeName' in text:
                        text = text.split('i__kIMMessagePartAttributeName')[0]
                    text = text.strip()
                except:
                    text = None
                    
            click.echo(f"Text: {text or '(empty)'}")
    else:
        click.echo("\nNo other messages found in the same group")

@cli.command()
def sync():
    """Sync messages by copying from ~/Library/Messages/chat.db"""
    source_path = os.path.expanduser("~/Library/Messages/chat.db")
    target_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'messages.db')

    if not os.path.exists(source_path):
        click.echo(f"Error: Source database not found at {source_path}")
        return

    # Get source file info (using sudo)
    try:
        stat_output = subprocess.check_output(['sudo', 'stat', '-f', '%z %m', source_path], text=True)
        size, mtime = map(int, stat_output.strip().split())
        click.echo(f"\nSource database info:")
        click.echo(f"Path: {source_path}")
        click.echo(f"Size: {size:,} bytes")
        click.echo(f"Last modified: {datetime.fromtimestamp(mtime).strftime('%Y-%m-%d %H:%M:%S')}")
    except subprocess.CalledProcessError as e:
        click.echo(f"Error getting source file info: {str(e)}")
        return

    # Check if target exists and compare timestamps
    if os.path.exists(target_path):
        target_stat = os.stat(target_path)
        click.echo(f"\nCurrent database info:")
        click.echo(f"Path: {target_path}")
        click.echo(f"Size: {target_stat.st_size:,} bytes")
        click.echo(f"Last modified: {datetime.fromtimestamp(target_stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')}")

        if target_stat.st_mtime >= mtime:
            click.echo("\nLocal database is already up to date.")
            return

    # Copy the database using sudo
    try:
        subprocess.run(['sudo', 'cp', source_path, target_path], check=True)
        # Fix permissions so we can access it
        subprocess.run(['sudo', 'chown', f"{os.getuid()}:{os.getgid()}", target_path], check=True)
        click.echo("\nSuccessfully synced messages database.")
    except subprocess.CalledProcessError as e:
        click.echo(f"\nError syncing database: {str(e)}")
        return

    # Verify the copy
    if os.path.exists(target_path):
        new_stat = os.stat(target_path)
        click.echo(f"\nNew database info:")
        click.echo(f"Size: {new_stat.st_size:,} bytes")
        click.echo(f"Last modified: {datetime.fromtimestamp(new_stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')}")

@cli.command()
@click.option('--days', default=1, help='Number of days to look back')
def test_messages(days):
    """Simple test to fetch and display messages"""
    agent = MessageAgent()
    messages = agent.message_service.get_recent_messages(days)
    click.echo(f"\nFound {len(messages)} messages from last {days} days:")
    
    for msg in messages:
        click.echo("\n---")
        click.echo(f"From: {msg['contact']}")
        click.echo(f"Message: {msg['text']}")
        click.echo(f"Time: {msg['formatted_time']}")
        click.echo(f"Is from me: {msg['is_from_me']}")
        if msg.get('group_name'):
            click.echo(f"Group: {msg['group_name']}")
        click.echo("---")

@cli.command()
@click.option('--days', default=1, help='Number of days to look back')
def test_agent(days):
    """Test the agent's ability to fetch and respond to messages"""
    agent = MessageAgent()
    
    # Get messages needing responses
    messages = agent.message_service.get_pending_messages(days)
    click.echo(f"\nFound {len(messages)} messages needing responses from last {days} days:")
    
    for msg in messages:
        click.echo("\n=== Processing Message ===")
        click.echo(f"From: {msg['contact']}")
        click.echo(f"Message: {msg['text']}")
        click.echo(f"Time: {msg['formatted_time']}")
        
        # Get conversation history for context
        history = agent.message_service.get_conversation_history(msg['contact'], limit=5)
        click.echo("\nRecent conversation history:")
        for hist_msg in history:
            direction = "→" if hist_msg['is_from_me'] else "←"
            click.echo(f"{direction} {hist_msg.get('text', '')}")
        
        # Have the agent draft a response
        click.echo("\nDrafting response...")
        result = agent.handle_message(msg['contact'], msg['text'])
        click.echo(f"Agent result: {result}")
        click.echo("========================\n")

if __name__ == '__main__':
    cli()