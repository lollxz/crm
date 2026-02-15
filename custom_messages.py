"""
Custom Message Templates Module

Allows users to override default email templates on a per-contact basis.
These custom templates are stored in the database and used instead of
the default template files when sending emails.

This module is imported and used by main.py for message handling.
"""

import logging
import json
from datetime import datetime, UTC
from typing import Optional, Dict, Any, List
import asyncpg

logger = logging.getLogger(__name__)


class CustomMessageManager:
    """Manages custom message templates for individual contacts"""
    
    def __init__(self, db_pool: asyncpg.Pool):
        self.db_pool = db_pool
    
    async def initialize(self):
        """Create custom_contact_messages table if it doesn't exist"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS custom_contact_messages (
                    id SERIAL PRIMARY KEY,
                    contact_id INTEGER NOT NULL REFERENCES campaign_contacts(id) ON DELETE CASCADE,
                    message_type VARCHAR(100) NOT NULL,
                    stage VARCHAR(50),
                    reminder_type VARCHAR(50),
                    subject TEXT NOT NULL,
                    body TEXT NOT NULL,
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW(),
                    created_by VARCHAR(255),
                    UNIQUE(contact_id, message_type, stage, reminder_type)
                )
            """)
            
            # Create index for faster lookups
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_custom_messages_contact
                ON custom_contact_messages(contact_id, message_type)
            """)
            
            logger.info("[CUSTOM MESSAGES] Initialized custom_contact_messages table")
    
    async def get_all_message_templates(self, contact_id: int) -> List[Dict[str, Any]]:
        """Get all custom message templates for a contact"""
        try:
            async with self.db_pool.acquire() as conn:
                templates = await conn.fetch("""
                    SELECT id, contact_id, message_type, stage, reminder_type,
                           subject, body, is_active, created_at, updated_at
                    FROM custom_contact_messages
                    WHERE contact_id = $1
                    ORDER BY message_type, stage, reminder_type
                """, contact_id)
                
                return [dict(t) for t in templates]
        except Exception as e:
            logger.error(f"[CUSTOM MESSAGES] Error fetching templates for contact {contact_id}: {e}")
            return []
    
    async def get_message_template(
        self,
        contact_id: int,
        message_type: str,
        stage: Optional[str] = None,
        reminder_type: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get a specific custom message template"""
        try:
            async with self.db_pool.acquire() as conn:
                template = await conn.fetchrow("""
                    SELECT id, subject, body, is_active, created_at, updated_at
                    FROM custom_contact_messages
                    WHERE contact_id = $1 
                      AND message_type = $2
                      AND (stage = $3 OR ($3 IS NULL AND stage IS NULL))
                      AND (reminder_type = $4 OR ($4 IS NULL AND reminder_type IS NULL))
                      AND is_active = TRUE
                    LIMIT 1
                """, contact_id, message_type, stage, reminder_type)
                
                if template:
                    return dict(template)
                return None
        except Exception as e:
            logger.error(f"[CUSTOM MESSAGES] Error fetching template: {e}")
            return None
    
    async def save_message_template(
        self,
        contact_id: int,
        message_type: str,
        subject: str,
        body: str,
        stage: Optional[str] = None,
        reminder_type: Optional[str] = None,
        username: Optional[str] = None
    ) -> bool:
        """Save or update a custom message template"""
        try:
            async with self.db_pool.acquire() as conn:
                # Use upsert to create or update
                await conn.execute("""
                    INSERT INTO custom_contact_messages (
                        contact_id, message_type, stage, reminder_type,
                        subject, body, is_active, created_by, created_at, updated_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, TRUE, $7, NOW(), NOW())
                    ON CONFLICT (contact_id, message_type, stage, reminder_type)
                    DO UPDATE SET
                        subject = $5,
                        body = $6,
                        is_active = TRUE,
                        updated_at = NOW()
                """, contact_id, message_type, stage, reminder_type, subject, body, username)
                
                logger.info(f"[CUSTOM MESSAGES] Saved template for contact {contact_id}: {message_type}/{stage}/{reminder_type}")
                return True
        except Exception as e:
            logger.error(f"[CUSTOM MESSAGES] Error saving template: {e}")
            return False
    
    async def delete_message_template(
        self,
        contact_id: int,
        message_type: str,
        stage: Optional[str] = None,
        reminder_type: Optional[str] = None
    ) -> bool:
        """Delete a custom message template"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    DELETE FROM custom_contact_messages
                    WHERE contact_id = $1 
                      AND message_type = $2
                      AND (stage = $3 OR ($3 IS NULL AND stage IS NULL))
                      AND (reminder_type = $4 OR ($4 IS NULL AND reminder_type IS NULL))
                """, contact_id, message_type, stage, reminder_type)
                
                logger.info(f"[CUSTOM MESSAGES] Deleted template for contact {contact_id}")
                return True
        except Exception as e:
            logger.error(f"[CUSTOM MESSAGES] Error deleting template: {e}")
            return False
    
    async def get_template_for_sending(
        self,
        contact_id: int,
        message_type: str,
        stage: Optional[str] = None,
        reminder_type: Optional[str] = None
    ) -> Optional[Dict[str, str]]:
        """Get template for sending - returns subject and body"""
        template = await self.get_message_template(
            contact_id, message_type, stage, reminder_type
        )
        
        if template:
            logger.info(f"[CUSTOM MESSAGES] Using custom template for contact {contact_id}")
            return {
                'subject': template['subject'],
                'body': template['body']
            }
        
        return None
    
    async def clear_contact_templates(self, contact_id: int) -> bool:
        """Delete all custom templates for a contact"""
        try:
            async with self.db_pool.acquire() as conn:
                await conn.execute("""
                    DELETE FROM custom_contact_messages
                    WHERE contact_id = $1
                """, contact_id)
                
                logger.info(f"[CUSTOM MESSAGES] Cleared all templates for contact {contact_id}")
                return True
        except Exception as e:
            logger.error(f"[CUSTOM MESSAGES] Error clearing templates: {e}")
            return False


# Create global instance
custom_message_manager: Optional[CustomMessageManager] = None


async def init_custom_messages(db_pool: asyncpg.Pool):
    """Initialize custom messages manager"""
    global custom_message_manager
    custom_message_manager = CustomMessageManager(db_pool)
    await custom_message_manager.initialize()
    logger.info("[CUSTOM MESSAGES] Manager initialized")


def get_custom_message_manager() -> Optional[CustomMessageManager]:
    """Get the custom message manager instance"""
    return custom_message_manager
