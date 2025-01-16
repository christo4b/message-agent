import asyncio
import click
import os
from datetime import datetime
from .agent import MessageAgent

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

if __name__ == '__main__':
    cli()