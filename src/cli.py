import asyncio
import click
from src.agent import MessageAgent

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

if __name__ == '__main__':
    cli() 