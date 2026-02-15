from fastapi import FastAPI, UploadFile, File, HTTPException, Query, status, Body, Depends, Header, Path, BackgroundTasks, Request, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, validator, field_validator, constr
import uvicorn
from typing import Optional, Dict, Any, List, Union, Literal
from datetime import datetime, timedelta, UTC, timezone
import jwt

# Custom flow models
class CustomFlowStep(BaseModel):
    type: Literal['email', 'task', 'notification']
    subject: constr(min_length=1, max_length=200)
    body: constr(min_length=1)
    delay_days: Optional[int] = 0

class CustomFlow(BaseModel):
    contact_id: int
    steps: List[CustomFlowStep]
import pandas as pd
import io
import re
from datetime import datetime, timedelta, UTC, timezone
import bcrypt
import jwt
from uuid import uuid4
import json
import schedule
import time
import threading
import asyncio
from contextlib import contextmanager, asynccontextmanager
import os
import html
import graph_email
import random
import logging
from logging.handlers import RotatingFileHandler
import graph_email
from dotenv import load_dotenv
import requests
from msal import ConfidentialClientApplication
import asyncpg
import httpx
from asyncio import Semaphore
import subprocess
from starlette.responses import Response
from monitoring import init_monitoring_service, update_worker_heartbeat
from monitoring_api import router as monitoring_router
from business_hours import next_allowed_uk_business_time, is_business_hours
from contact_messages import (
    get_contact_message_flows, 
    save_contact_custom_message, 
    delete_contact_custom_message,
    get_message_for_sending,
    create_contact_messages_table
)
import email
from email import policy
from fastapi.responses import StreamingResponse
import csv
from io import StringIO
from fastapi import APIRouter
from fastapi.responses import FileResponse
import pytz
import base64
from datetime import timezone

# Configure root lo
logging.basicConfig(level=logging.DEBUG,
                   format='%(asctime)s %(levelname)s [%(name)s] %(message)s',
                   datefmt='%Y-%m-%d %H:%M:%S')

# Setup file handler
file_handler = RotatingFileHandler('backend.log', maxBytes=10*1024*1024, backupCount=5)
file_handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s [%(name)s] %(message)s'))

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter('\033[92m%(asctime)s\033[0m \033[94m%(levelname)s\033[0m [%(name)s] %(message)s'))


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(file_handler)
logger.addHandler(console_handler)


load_dotenv()


POSTGRES_DSN = os.getenv('POSTGRES_DSN', 'postgresql://postgres@localhost/travel_app')
LOCAL_DB_PATH = os.getenv('LOCAL_DB_PATH', './crm_database')
OUTLOOK_EMAIL = os.getenv('OUTLOOK_EMAIL')
OUTLOOK_PASSWORD = os.getenv('OUTLOOK_PASSWORD')

# External validator service URL
VALIDATOR_URL = os.getenv('VALIDATOR_URL', 'https://62.171.152.239:5000/validate')
# Concurrency limit for validation requests
VALIDATOR_CONCURRENCY = int(os.getenv('VALIDATOR_CONCURRENCY', '8'))
_validator_semaphore = Semaphore(VALIDATOR_CONCURRENCY)
VALIDATOR_MAX_RETRIES = int(os.getenv('VALIDATOR_MAX_RETRIES', '3'))
VALIDATOR_BACKOFF_BASE = float(os.getenv('VALIDATOR_BACKOFF_BASE', '1.0'))


async def call_validator(email: str) -> Dict[str, Any]:
    """Call external validator with retry/backoff and return structured result."""
    last_exc = None
    for attempt in range(1, VALIDATOR_MAX_RETRIES + 1):
        try:
            async with _validator_semaphore:
                timeout = httpx.Timeout(120.0, connect=10.0)
                async with httpx.AsyncClient(verify=False, timeout=timeout) as client:
                    logger.debug(f"Calling validator (attempt {attempt}) for {email} -> {VALIDATOR_URL}")
                    
                    start_ts = datetime.utcnow()
                    resp = await client.post(VALIDATOR_URL, json={"email": email})
                    elapsed = (datetime.utcnow() - start_ts).total_seconds()
                    
                    logger.debug(f"Validator response for {email} received in {elapsed:.2f}s (status={resp.status_code})")

                    # === FIX: Return the actual result directly ===
                    try:
                        data = resp.json()
                    except Exception:
                        data = {}

                    return {
                        'code': resp.status_code,
                        'valid': data.get('valid', False),
                        'validation_result': data,
                        'reason': data.get('reason'),
                        'raw': resp.text
                    }

        except httpx.ReadTimeout as e:
            last_exc = e
            logger.warning(f"Validator read timeout on attempt {attempt} for {email}: {e}")
            if attempt < VALIDATOR_MAX_RETRIES:
                backoff = VALIDATOR_BACKOFF_BASE * (2 ** (attempt - 1))
                await asyncio.sleep(backoff)
                continue
            raise last_exc
        except Exception as e:
            last_exc = e
            logger.warning(f"Validator call exception on attempt {attempt} for {email}: {e}")
            if attempt < VALIDATOR_MAX_RETRIES:
                backoff = VALIDATOR_BACKOFF_BASE * (2 ** (attempt - 1))
                await asyncio.sleep(backoff)
                continue
            raise last_exc

import math

ALLOWED_SENDERS = [
    'accommodations@converiatravel.com',
    'coordination@converiatravel.com',
    'housing@converiatravel.com',
    'logistics@converiatravels.com',
    'reservations@converiatravels.com',
    'lodgings@converiatravels.com',
]

DEFAULT_SENDER_EMAIL = 'accommodations@converiatravel.com'

async def get_auto_assigned_sender(conn, event_size: int = 0) -> str:
    # Batch size config
    CAPACITY_PER_SENDER = 50
    
    # 1. Query Total Load
    rows = await conn.fetch(
        """
        SELECT 
            sender_email, 
            COALESCE(SUM(expected_contact_count), 0) as total_load
        FROM event
        WHERE sender_email = ANY($1::text[])
        GROUP BY sender_email
        """,
        ALLOWED_SENDERS
    )
    
    # 2. Map results
    current_loads = {row['sender_email']: row['total_load'] for row in rows}
    
    # 3. Fill missing senders with 0
    for sender in ALLOWED_SENDERS:
        if sender not in current_loads:
            current_loads[sender] = 0

    # 4. Calculate Batch Tiers
    sender_tiers = {}
    print("\n--- SENDER ROTATION DEBUG ---")
    for sender in ALLOWED_SENDERS:
        load = current_loads[sender]
        tier = load // CAPACITY_PER_SENDER
        sender_tiers[sender] = tier
        print(f"Sender: {sender:<35} | Load: {load:<5} | Tier: {tier}")

    # 5. Find lowest tier
    min_tier = min(sender_tiers.values())
    print(f"TARGET TIER: {min_tier}")

    # 6. Select first sender in lowest tier
    for sender in ALLOWED_SENDERS:
        if sender_tiers[sender] == min_tier:
            print(f"✅ ASSIGNING: {sender}\n")
            return sender

    return ALLOWED_SENDERS[0]


# Define allowed origins for CORS
ALLOWED_ORIGINS = [
    "https://exhibitions-conferences.info",
    "https://staymanagement.org",
    "https://www.conferencecare.org/api",
    "http://localhost:5173",
    "https://conferencecare.org",
    "http://localhost:3000",
    "https://localhost:5173",
    "https://localhost:3000",
    "https://conferencecare.org",
    "https://localhost:9009",
    "http://62.171.152.239",
    "https://accommodationassist.org"
]
app = FastAPI(title="CRM Backend Service", version="1.0.0")
# Template loading functions
def load_template(template_type: str, part: str, reminder_type: str = None, stage: str = None) -> str:
    """
    Load email templates based on type, part, and stage.

    Args:
        template_type: 'campaign', 'reminder', 'forms', or 'payments'
        part: 'subject' or 'body'
        reminder_type: For reminders, can be 'reminder1', 'reminder2', etc.
        stage: Current campaign stage ('forms', 'payments', etc.)

    Returns:
        str: The template content
    """
    # Normalize inputs
    template_type = template_type.lower() if template_type else None
    part = part.lower() if part else None
    stage = stage.lower() if stage else None
    reminder_type = str(reminder_type).lower() if reminder_type else None

    # Backwards-compatibility: callers sometimes pass the reminder stage in the
    # `stage` parameter (e.g. 'reminder2') instead of the `reminder_type`
    # parameter. If we detect that `stage` looks like a reminder name and
    # `reminder_type` isn't provided, treat `stage` as the `reminder_type` and
    # clear `stage` so lookups match the template_files keys.
    if not reminder_type and stage and stage.startswith('reminder'):
        reminder_type = stage
        stage = None
        logger.debug(f"[TEMPLATE] Interpreting stage '{reminder_type}' as reminder_type for lookup")

    # Define template file mappings with fallback hierarchy
    template_files = {
        # Initial campaign message (first email)
        ('campaign', 'subject', None, 'initial'): 'public/templates/emails/campaign_default_subject.txt',
        ('campaign', 'body', None, 'initial'): 'public/templates/emails/campaign_default_body.txt',

        # First reminder (reminder1)
        ('reminder', 'subject', 'reminder1', None): 'public/templates/emails/reminder_default_subject.txt',
        ('reminder', 'body', 'reminder1', None): 'public/templates/emails/reminder_default_body.txt',

        # Second reminder (reminder2)
        ('reminder', 'subject', 'reminder2', None): 'public/templates/emails/reminder2_default_subject.txt',
        ('reminder', 'body', 'reminder2', None): 'public/templates/emails/reminder2_default_body.txt',

        # Forms stage initial message
        ('forms', 'subject', None, 'initial'): 'public/templates/emails/forms_main_subject.txt',
        ('forms', 'body', None, 'initial'): 'public/templates/emails/forms_main_body.txt',

        # Forms stage reminders
        ('forms', 'subject', 'reminder1', None): 'public/templates/emails/forms_reminder1_subject.txt',
        ('forms', 'body', 'reminder1', None): 'public/templates/emails/forms_reminder1_body.txt',
        ('forms', 'subject', 'reminder2', None): 'public/templates/emails/forms_reminder2_subject.txt',
        ('forms', 'body', 'reminder2', None): 'public/templates/emails/forms_reminder2_body.txt',
        ('forms', 'subject', 'reminder3', None): 'public/templates/emails/forms_reminder3_subject.txt',
        ('forms', 'body', 'reminder3', None): 'public/templates/emails/forms_reminder3_body.txt',

        # Payments stage initial message
        ('payments', 'subject', None, 'initial'): 'public/templates/emails/payments_main_subject.txt',
        ('payments', 'body', None, 'initial'): 'public/templates/emails/payments_main_body.txt',

        # Payments stage reminders (up to 6 reminders)
        ('payments', 'subject', 'reminder1', None): 'public/templates/emails/payments_reminder1_subject.txt',
        ('payments', 'body', 'reminder1', None): 'public/templates/emails/payments_reminder1_body.txt',
        ('payments', 'subject', 'reminder2', None): 'public/templates/emails/payments_reminder2_subject.txt',
        ('payments', 'body', 'reminder2', None): 'public/templates/emails/payments_reminder2_body.txt',
        ('payments', 'subject', 'reminder3', None): 'public/templates/emails/payments_reminder3_subject.txt',
        ('payments', 'body', 'reminder3', None): 'public/templates/emails/payments_reminder3_body.txt',
        ('payments', 'subject', 'reminder4', None): 'public/templates/emails/payments_reminder4_subject.txt',
        ('payments', 'body', 'reminder4', None): 'public/templates/emails/payments_reminder4_body.txt',
        ('payments', 'subject', 'reminder5', None): 'public/templates/emails/payments_reminder5_subject.txt',
        ('payments', 'body', 'reminder5', None): 'public/templates/emails/payments_reminder5_body.txt',
        ('payments', 'subject', 'reminder6', None): 'public/templates/emails/payments_reminder6_subject.txt',
        ('payments', 'body', 'reminder6', None): 'public/templates/emails/payments_reminder6_body.txt',

    # SEPA bank transfer payment stage (payment_sepa templates)
    ('sepa', 'subject', None, 'initial'): 'public/templates/emails/payment_sepa_subject.txt',
    ('sepa', 'body', None, 'initial'): 'public/templates/emails/payment_sepa_body.txt',

    # SEPA reminders 1..3 use SEPA-specific templates
    ('sepa', 'subject', 'reminder1', None): 'public/templates/emails/payment_sepa_reminder1_subject.txt',
    ('sepa', 'body', 'reminder1', None): 'public/templates/emails/payment_sepa_reminder1_body.txt',
    ('sepa', 'subject', 'reminder2', None): 'public/templates/emails/payment_sepa_reminder2_subject.txt',
    ('sepa', 'body', 'reminder2', None): 'public/templates/emails/payment_sepa_reminder2_body.txt',
    ('sepa', 'subject', 'reminder3', None): 'public/templates/emails/payment_sepa_reminder3_subject.txt',
    ('sepa', 'body', 'reminder3', None): 'public/templates/emails/payment_sepa_reminder3_body.txt',

    # SEPA reminders 4..6 reuse the payments reminder templates (same copy)
    ('sepa', 'subject', 'reminder4', None): 'public/templates/emails/payments_reminder4_subject.txt',
    ('sepa', 'body', 'reminder4', None): 'public/templates/emails/payments_reminder4_body.txt',
    ('sepa', 'subject', 'reminder5', None): 'public/templates/emails/payments_reminder5_subject.txt',
    ('sepa', 'body', 'reminder5', None): 'public/templates/emails/payments_reminder5_body.txt',
    ('sepa', 'subject', 'reminder6', None): 'public/templates/emails/payments_reminder6_subject.txt',
    ('sepa', 'body', 'reminder6', None): 'public/templates/emails/payments_reminder6_body.txt',

    # RH bank transfer payment stage (payment_rh templates)
    ('rh', 'subject', None, 'initial'): 'public/templates/emails/payment_rh_subject.txt',
    ('rh', 'body', None, 'initial'): 'public/templates/emails/payment_rh_body.txt',

    # RH reminders 1..3 use RH-specific templates
    ('rh', 'subject', 'reminder1', None): 'public/templates/emails/payment_rh_reminder1_subject.txt',
    ('rh', 'body', 'reminder1', None): 'public/templates/emails/payment_rh_reminder1_body.txt',
    ('rh', 'subject', 'reminder2', None): 'public/templates/emails/payment_rh_reminder2_subject.txt',
    ('rh', 'body', 'reminder2', None): 'public/templates/emails/payment_rh_reminder2_body.txt',
    ('rh', 'subject', 'reminder3', None): 'public/templates/emails/payment_rh_reminder3_subject.txt',
    ('rh', 'body', 'reminder3', None): 'public/templates/emails/payment_rh_reminder3_body.txt',

    # RH reminders 4..6 reuse the payments reminder templates (same copy)
    ('rh', 'subject', 'reminder4', None): 'public/templates/emails/payments_reminder4_subject.txt',
    ('rh', 'body', 'reminder4', None): 'public/templates/emails/payments_reminder4_body.txt',
    ('rh', 'subject', 'reminder5', None): 'public/templates/emails/payments_reminder5_subject.txt',
    ('rh', 'body', 'reminder5', None): 'public/templates/emails/payments_reminder5_body.txt',
    ('rh', 'subject', 'reminder6', None): 'public/templates/emails/payments_reminder6_subject.txt',
    ('rh', 'body', 'reminder6', None): 'public/templates/emails/payments_reminder6_body.txt',

        # Fallback for any other reminder types
        ('reminder', 'subject', None, None): 'public/templates/emails/reminder_default_subject.txt',
        ('reminder', 'body', None, None): 'public/templates/emails/reminder_default_body.txt',
    }

    # Define the fallback hierarchy for template lookup
    lookup_keys = []

    # 0. Handle backward compatibility and special cases
    if template_type == 'campaign':
        # For initial campaign message (stage='default' or None)
        if not stage or stage == 'default':
            lookup_keys.append(('campaign', part, None, 'initial'))
        # For campaign with specific stage (e.g., 'forms', 'payments')
        elif stage in ['forms', 'payments']:
            lookup_keys.append((stage, part, reminder_type, 'initial'))
            lookup_keys.append((stage, part, None, 'initial'))

    # 1. Exact match with both stage and reminder_type
    if stage and reminder_type:
        lookup_keys.append((template_type, part, reminder_type, stage))

    # 2. Match with stage only (for initial messages)
    if stage:
        lookup_keys.append((template_type, part, None, stage))

    # 3. Match with reminder_type only (for default stage)
    if reminder_type:
        lookup_keys.append((template_type, part, reminder_type, None))

    # 4. Most generic fallback (no stage or reminder_type)
    lookup_keys.append((template_type, part, None, None))

    # Try each key in order until we find a match
    template_file = None
    matched_key = None
    for key in lookup_keys:
        if key in template_files:
            template_file = template_files[key]
            matched_key = key
            break

    if not template_file:
        raise RuntimeError(f"No template found for type: {template_type}, part: {part}, "
                         f"reminder_type: {reminder_type}, stage: {stage}")

    # Try to load the template file
    try:
        with open(template_file, 'r', encoding='utf-8') as f:
            content = f.read().strip()
            if not content:
                raise ValueError(f"Template file is empty: {template_file}")

            logger.info(f"Successfully loaded template: {template_file}")
            if matched_key:
                # Log at INFO so it appears in normal logs and include a short preview
                preview = (content[:120] + '...') if len(content) > 120 else content
                logger.info(f"[TEMPLATE-LOOKUP] matched_key={matched_key} -> file={template_file} preview={preview!r}")
            return content

    except FileNotFoundError:
        logger.error(f"Template file not found: {template_file}")
        # If this was a specific template, try the next fallback in hierarchy
        if len(lookup_keys) > 1:
            fallback_key = (template_type, part, None)
            fallback_file = template_files.get(fallback_key)
            if fallback_file and fallback_file != template_file:
                try:
                    with open(fallback_file, 'r', encoding='utf-8') as f:
                        content = f.read()
                        logger.warning(f"Using fallback template: {fallback_file} instead of {template_file}")
                        return content
                except FileNotFoundError:
                    pass
        raise RuntimeError(f"Could not load template file: {template_file}")
    except Exception as e:
        raise RuntimeError(f"Error reading template file {template_file}: {e}")

def render_template_strict(template: str, customer: dict) -> str:
    """
    Strict template renderer that fails fast if any required variables are missing.
    No default values are provided - all template variables must be present in the customer data.

    Args:
        template: The template string with {{variable}} placeholders
        customer: Dictionary containing all required variables

    Returns:
        Rendered template with variables replaced

    Raises:
        ValueError: If any required variables are missing or empty
    """
    if not template or not isinstance(template, str):
        raise ValueError("Template must be a non-empty string")

    if not customer or not isinstance(customer, dict):
        raise ValueError("Customer data must be a non-empty dictionary")

    # Normalize common alias keys so templates that use alternate names still work.
    try:
        # make a shallow copy so we don't mutate caller dict
        customer = dict(customer)
        # payment_link <-> payments_link
        if 'payment_link' in customer and 'payments_link' not in customer:
            customer['payments_link'] = customer.get('payment_link')
        if 'payments_link' in customer and 'payment_link' not in customer:
            customer['payment_link'] = customer.get('payments_link')
        # forms_link <-> form_link
        if 'forms_link' in customer and 'form_link' not in customer:
            customer['form_link'] = customer.get('forms_link')
        if 'form_link' in customer and 'forms_link' not in customer:
            customer['forms_link'] = customer.get('form_link')
    except Exception:
        pass

    # Extract all required variables from the template
    required_vars = set(re.findall(r'{{\s*(.*?)\s*}}', template))
    if not required_vars:
        logger.debug("No template variables found in template")
        return template

    # Check for missing variables
    missing = []
    for var in required_vars:
        if var not in customer or customer[var] is None or str(customer[var]).strip() == '':
            missing.append(var)

    if missing:
        error_msg = f"Missing required template variable(s): {', '.join(missing)} for customer ID {customer.get('id', 'unknown')}"
        logger.error(error_msg)
        raise ValueError(error_msg)

    # All variables are present, perform the substitution
    def replacer(match):
        key = match.group(1).strip()
        value = customer.get(key, '')
        if value is None:
            logger.warning(f"Variable '{key}' is None for customer ID {customer.get('id', 'unknown')}")
            return ''
        return str(value)

    try:
        result = re.sub(r'{{\s*(.*?)\s*}}', replacer, template)
        if not result or not result.strip():
            raise ValueError(f"Rendered template is empty for customer ID {customer.get('id', 'unknown')}")

        # Verify no template variables remain unsubstituted
        remaining_vars = set(re.findall(r'{{\s*(.*?)\s*}}', result))
        if remaining_vars:
            raise ValueError(f"Failed to substitute all template variables. Remaining: {', '.join(remaining_vars)}")

        return result

    except Exception as e:
        logger.error(f"Template rendering failed: {str(e)}")
        raise

import re

def extract_name_parts_with_prefix(full_name: str) -> tuple:
    """
    Parses a name string for the email template: 'Dear {{prefix}} {{name}}'
    
    Logic:
    1. If a Prefix is found (e.g., Mr., Dr.), returns (Prefix, Last Name).
    2. If NO Prefix is found, returns (First Name, Last Name).
       This ensures 'Dear {{prefix}} {{name}}' reads as 'Dear Hatem Ayman'.
    """
    if not full_name or not isinstance(full_name, str):
        return "", ""
    
    # Clean whitespace
    full_name = full_name.strip()
    
    # 1. Define common prefixes (Case insensitive)
    # Checks for "Mr.", "Mr", "Dr.", "Dr", etc. at the start of the string
    prefix_pattern = r"^(Mr\.|Mr|Ms\.|Ms|Mrs\.|Mrs|Dr\.|Dr|Prof\.|Prof|Sir|Madam|Eng\.|Eng)\b\.?"
    
    match = re.match(prefix_pattern, full_name, re.IGNORECASE)
    
    if match:
        # --- PATH A: Prefix Found ---
        # Goal: Return ("Mr.", "Ayman")
        
        found_prefix = match.group(0).strip()
        # Remove the prefix from the string to analyze the rest
        remainder = full_name[match.end():].strip()
        
        # Split remaining name by comma OR space to find the Last Name
        if ',' in remainder:
            parts = remainder.split(',')
        else:
            parts = remainder.split()
            
        # We take the LAST part as the surname
        last_name = parts[-1].strip() if parts else remainder
        
        # Ensure prefix has a dot if missing (optional polish)
        if not found_prefix.endswith('.') and len(found_prefix) <= 3:
             found_prefix += "."
             
        return found_prefix.title(), last_name.title()

    else:
        # --- PATH B: No Prefix Found ---
        # Goal: Return ("Hatem", "Ayman") so template reads "Dear Hatem Ayman"
        
        if ',' in full_name:
            
            parts = full_name.split(',')
            first_part = parts[0].strip()
            last_part = parts[-1].strip()
            return first_part.title(), last_part.title()
        else:
            # Handle "Hatem Ayman" (Space separated)
            parts = full_name.split()
            if len(parts) >= 2:
                # First word = Prefix slot, Rest = Name slot
                return parts[0].title(), " ".join(parts[1:]).title()
            else:
                # Single Name (e.g. "Cher")
                # Return empty prefix, Name in name slot
                return "", full_name.title()


# In main.py, add this helper function before generate_quoted_block
def build_outgoing_body(contact: dict, new_body: str) -> str:
    """
    Build the outgoing email body with proper quoting of previous messages.
    Appends either the latest cleaned contact reply or our last sent CRM message.
    
    Args:
        contact: The contact dict containing last_reply_body/last_sent_body
        new_body: The new message body to send
        
    Returns:
        Complete message body with quoted text appended
    """
    quoted_block = ""
    
    if contact.get('last_reply_body') and contact.get('last_reply_at'):
        # Quote the contact's last reply (no arrow prefixes)
        reply_time = contact['last_reply_at'].strftime("%a, %b %-d, %Y at %-I:%M %p")
        quoted_header = f"\nOn {reply_time} {contact['name']} <{contact['email']}> wrote:\n"
        quoted_text = "\n".join(contact['last_reply_body'].splitlines())
        quoted_block = quoted_header + quoted_text
    
    elif contact.get('last_sent_body') and contact.get('last_sent_at'):
        # Quote our last sent message (no arrow prefixes)
        sent_time = contact['last_sent_at'].strftime("%a, %b %-d, %Y at %-I:%M %p")
        quoted_header = f"\nOn {sent_time} {DEFAULT_SENDER_EMAIL} wrote:\n"
        quoted_text = "\n".join(contact['last_sent_body'].splitlines())
        quoted_block = quoted_header + quoted_text
    
    # Combine new body with quoted block
    return f"{new_body}\n{quoted_block}" if quoted_block else new_body

def normalize_email(email: str) -> str:
    """
    Normalize an email address for comparison.
    - Converts to lowercase
    - Removes angle brackets
    - Strips whitespace
    - Extracts just the email part if in "Name <email@example.com>" format
    """
    if not email:
        return ''
    # Convert to string in case it's not already
    email = str(email)
    # Remove angle brackets and strip whitespace
    email = email.strip('<> \t\n\r')
    # Extract just the email part if it's in "Name <email@example.com>" format
    if '<' in email and '>' in email:
        email = email.split('<')[-1].split('>')[0].strip()
    return email.lower()

def is_bounce_email(subject: str, body: str, sender_email: str) -> bool:
    """
    Detect if an email is a delivery failure bounce.
    """
    # Normalize inputs for checking
    subject_lower = subject.lower() if subject else ""
    body_lower = body.lower() if body else ""
    sender_lower = sender_email.lower() if sender_email else ""

    # Common bounce indicators in subject
    bounce_subjects = [
        "delivery status notification", "mail delivery failed", "delivery failure",
        "undelivered mail returned to sender", "message delivery failure", "returned mail",
        "mail system error", "delivery error", "postmaster@", "mailer-daemon@",
        "delivery report", "non-delivery report", "ndr", "bounce", "failure notice"
    ]

    # Common bounce senders
    bounce_senders = [
        "postmaster@", "mailer-daemon@", "noreply@", "no-reply@", "bounce@", "bounces@", "delivery@"
    ]

    # Common bounce body indicators
    bounce_body_indicators = [
        "message could not be delivered", "delivery has failed", "recipient address rejected",
        "mailbox unavailable", "address not found", "user unknown", "mailbox full",
        "quota exceeded", "message rejected", "recipient not found", "smtp error",
        "550", "554", "permanent failure", "bounce message", "delivery failure"
    ]

    # Check subject for bounce indicators
    for indicator in bounce_subjects:
        if indicator in subject_lower:
            return True

    # Check sender for bounce indicators
    for indicator in bounce_senders:
        if indicator in sender_lower:
            return True

    # Check body for bounce indicators
    for indicator in bounce_body_indicators:
        if indicator in body_lower:
            return True

    return False

def extract_bounced_email(body: str) -> str:
    """Extract the original recipient email address from a bounce message."""
    import re

    if not body:
        return ""

    # Common patterns for finding bounced email addresses
    patterns = [
        r'(?:original recipient|recipient address|failed recipient)[:\s]+([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
        r'(?:delivery to the following recipient failed)[:\s]+([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
        r'<([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})>',  # Email in angle brackets
        r'\b([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})\b'  # Any email pattern
    ]

    for pattern in patterns:
        matches = re.findall(pattern, body, re.IGNORECASE)
        if matches:
            return normalize_email(matches[0])

    return ""

def serialize_for_json(obj):
    """Helper function to serialize objects for JSON storage, handling datetime objects"""
    if obj is None:
        return None

    if isinstance(obj, dict):
        return {k: serialize_for_json(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [serialize_for_json(item) for item in obj]
    elif isinstance(obj, datetime):
        return obj.isoformat()
    elif hasattr(obj, 'isoformat'):  # Handle other datetime-like objects
        return obj.isoformat()
    elif hasattr(obj, '__dict__'):  # Handle objects with __dict__
        return serialize_for_json(obj.__dict__)
    else:
        try:
            # Try to serialize, if it fails, convert to string
            json.dumps(obj)
            return obj
        except (TypeError, ValueError):
            return str(obj)

async def log_user_activity(conn, user_info: dict, action_type: str, action_description: str,
                           target_type: str = None, target_id: int = None, target_name: str = None,
                           old_values: dict = None, new_values: dict = None,
                           ip_address: str = None, user_agent: str = None):
    """Log user activity for audit trail"""
    try:
        # Serialize values properly for JSON storage
        serialized_old_values = serialize_for_json(old_values)
        serialized_new_values = serialize_for_json(new_values)

        # Ensure we always provide a non-null username to satisfy the DB constraint.
        username_to_log = None
        try:
            username_to_log = user_info.get('username') if user_info else None
        except Exception:
            username_to_log = None
        if not username_to_log:
            username_to_log = 'system'

        await conn.execute('''
            INSERT INTO user_activity_logs (
                user_id, username, action_type, action_description,
                target_type, target_id, target_name, old_values, new_values,
                ip_address, user_agent, timestamp
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ''',
        user_info.get('id') if user_info else None,
        username_to_log,
        action_type,
        action_description,
        target_type,
        target_id,
        target_name,
        json.dumps(serialized_old_values) if serialized_old_values else None,
        json.dumps(serialized_new_values) if serialized_new_values else None,
        ip_address,
        user_agent,
        datetime.now()
        )
        logger.debug(f"[ACTIVITY] Logged {action_type} by {username_to_log}: {action_description}")
    except Exception as e:
        logger.error(f"[ACTIVITY] Failed to log user activity: {e}")
        # Log more details for debugging
        logger.error(f"[ACTIVITY] Debug info - old_values type: {type(old_values)}, new_values type: {type(new_values)}")
        if old_values:
            logger.error(f"[ACTIVITY] old_values keys: {list(old_values.keys()) if isinstance(old_values, dict) else 'not a dict'}")
        if new_values:
            logger.error(f"[ACTIVITY] new_values keys: {list(new_values.keys()) if isinstance(new_values, dict) else 'not a dict'}")

async def handle_bounce_email(conn, subject: str, body: str, sender_email: str):
    """Handle a detected bounce email by marking the email as bounced."""
    bounced_email = extract_bounced_email(body)

    if not bounced_email:
        logger.warning(f"[BOUNCE] Could not extract bounced email from bounce message")
        return

    logger.info(f"[BOUNCE] Detected bounce for email: {bounced_email}")

    # Determine bounce type and reason
    bounce_type = "hard"
    bounce_reason = "Email delivery failed"

    if "mailbox full" in body.lower() or "quota exceeded" in body.lower():
        bounce_type = "soft"
        bounce_reason = "Mailbox full"
    elif "temporary failure" in body.lower():
        bounce_type = "soft"
        bounce_reason = "Temporary delivery failure"
    elif "user unknown" in body.lower() or "address not found" in body.lower():
        bounce_reason = "Invalid email address"
    elif "mailbox unavailable" in body.lower():
        bounce_reason = "Mailbox unavailable"

    now = datetime.now()

    try:
        # Insert or update bounced email record
        await conn.execute('''
            INSERT INTO bounced_emails (email, bounce_type, bounce_reason, first_bounced_at, last_bounced_at, bounce_count)
            VALUES ($1, $2, $3, $4, $4, 1)
            ON CONFLICT (email) DO UPDATE SET
                bounce_type = EXCLUDED.bounce_type,
                bounce_reason = EXCLUDED.bounce_reason,
                last_bounced_at = EXCLUDED.last_bounced_at,
                bounce_count = bounced_emails.bounce_count + 1
        ''', bounced_email, bounce_type, bounce_reason, now)

        # Update all campaign contacts with this email address
        result = await conn.execute('''
            UPDATE campaign_contacts
            SET email_bounced = TRUE,
                bounce_reason = $1,
                bounced_at = $2,
                campaign_paused = TRUE
            WHERE LOWER(email) = LOWER($3) AND email_bounced = FALSE
        ''', bounce_reason, now, bounced_email)

        logger.info(f"[BOUNCE] Marked contacts as bounced for email: {bounced_email}")

        # Add trigger text to affected contacts
        trigger_text = f"{now.strftime('%Y-%m-%d %H:%M:%S')} - EMAIL_BOUNCED: {bounce_reason} (Type: {bounce_type})"
        await conn.execute('''
            UPDATE campaign_contacts
            SET trigger = COALESCE(trigger || E'\n', '') || $1
            WHERE LOWER(email) = LOWER($2)
        ''', trigger_text, bounced_email)

        # Also set stage to 'mail delivery' (frontend maps this to a gray color) and pause campaigns
        # but avoid overwriting terminal/intentional stages such as 'completed' or 'wrong person'
        try:
            await conn.execute('''
                UPDATE campaign_contacts
                SET stage = 'mail delivery',
                    campaign_paused = TRUE
                WHERE LOWER(email) = LOWER($1)
                  AND (stage IS NULL OR LOWER(stage) NOT IN ('completed','invoice & confirmation','payment due','wrong person'))
            ''', bounced_email)
            logger.info(f"[BOUNCE] Updated campaign_contacts stage to 'mail delivery' and paused campaigns for {bounced_email}")
        except Exception as e:
            logger.error(f"[BOUNCE] Failed to update contact stage/pause for {bounced_email}: {e}")

        # Mark any pending queued emails to this recipient as failed so the send worker won't retry them
        try:
            await conn.execute('''
                UPDATE email_queue
                SET status = 'failed',
                    error_message = $2
                WHERE LOWER(recipient_email) = LOWER($1) AND status = 'pending'
            ''', bounced_email, 'Bounced address - stopping further sends')
            logger.info(f"[BOUNCE] Marked pending email_queue items as failed for {bounced_email}")
        except Exception as e:
            logger.error(f"[BOUNCE] Failed to mark email_queue items failed for {bounced_email}: {e}")

    except Exception as e:
        logger.error(f"[BOUNCE] Error handling bounce for {bounced_email}: {e}")

# In main.py, replace the clean_email_body and generate_quoted_block functions

def clean_email_body(body: str) -> str:
    """
    Cleans an email body by removing signatures, previous history, and HTML.
    """
    if not body:
        return ""

    # Split on common signature/footer markers and take the first part
    separators = ["___", "Confidentiality Notice:", "From:", "-----Original Message-----", "Warm regards,"]
    for sep in separators:
        if sep in body:
            body = body.split(sep, 1)[0]

    body = body.split("PREVIOUS CONVERSATION HISTORY")[0]
    body = re.sub(r'<br\s*/?>', '\n', body, flags=re.IGNORECASE)
    body = re.sub(r'<[^>]+>', '', body)
    body = html.unescape(body)

    lines = [line.strip() for line in body.strip().split('\n')]
    while lines and not lines[0]: lines.pop(0)
    while lines and not lines[-1]: lines.pop()

    return "\n".join(lines).strip()


def generate_quoted_block(messages: List[Dict]) -> str:
    """
    Generates a clean, standard email quote of the conversation history.
    This version uses robust deduplication.
    """
    if not messages:
        return ""

    # 1. Aggressive Deduplication
    unique_messages = {}
    for msg in sorted(messages, key=lambda x: x.get('sent_at') or x.get('received_at') or datetime.min):
        # Create a unique signature for each message
        cleaned_body = clean_email_body(msg.get('body', ''))
        # Ignore empty messages that are just signatures
        if not cleaned_body:
            continue

        # Round the timestamp to the nearest minute to catch duplicates sent close together
        timestamp = msg.get('sent_at') or msg.get('received_at')
        ts_rounded = timestamp.replace(second=0, microsecond=0) if timestamp else None

        signature = (
            msg.get('direction'),
            normalize_email(msg.get('sender_email', '')),
            cleaned_body,
            ts_rounded
        )
        # By using the signature as a key, we automatically overwrite any duplicates
        unique_messages[signature] = msg

    if not unique_messages:
        return ""

    # 2. Reconstruct the last part of the conversation
    # Get the sorted list of unique messages (oldest to newest)
    sorted_unique_messages = sorted(unique_messages.values(), key=lambda x: x.get('sent_at') or x.get('received_at'))

    # The last message in the list is the most recent one
    latest_message = sorted_unique_messages[-1]

    # The message before that is its parent
    parent_message = sorted_unique_messages[-2] if len(sorted_unique_messages) > 1 else None

    latest_body = clean_email_body(latest_message.get('body', ''))

    # If there is no parent, just return the latest message body
    if not parent_message:
        return latest_body

    # Format the parent message as a quote
    parent_time = parent_message.get('sent_at') or parent_message.get('received_at')
    date_str = parent_time.strftime("%a, %b %d, %Y at %I:%M %p") if parent_time else "a previous message"
    sender_str = parent_message.get('sender_email', 'System')

    parent_body = clean_email_body(parent_message.get('body', ''))
    # Do not prefix lines with '>'. Return the parent body verbatim (cleaned).
    # Keep blank lines intact so spacing remains consistent.
    quoted_lines = [line for line in parent_body.split('\n')]
    quoted_text = "\n".join(quoted_lines)

    # Return the final, correctly formatted block
    return f"{latest_body}\n\nOn {date_str}, {sender_str} wrote:\n{quoted_text}"

def get_sender_password(sender_email: str) -> str:
    # Try .env first
    env_key = f'SENDER_PASSWORD_{sender_email.replace("@", "_at_").replace(".", "_")}'.upper()
    password = os.getenv(env_key)
    if password:
        return password
    # Optionally: check a secure DB table here
    if sender_email == OUTLOOK_EMAIL:
        return OUTLOOK_PASSWORD
    raise RuntimeError(f"No password found for sender {sender_email}. Please add to .env as {env_key}")

def is_duplicate_trigger(trigger: str) -> bool:
    if not trigger:
        return False
    if trigger.startswith('sent_at:') or trigger in ('replied', 'reminder_sent'):
        return True
    return False

# Graph API functions
def get_graph_token(client_id, client_secret, tenant_id):
    authority = f"https://login.microsoftonline.com/{tenant_id}"
    app = ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
    scopes = ["https://graph.microsoft.com/.default"]
    result = app.acquire_token_silent(scopes, account=None)
    if not result:
        result = app.acquire_token_for_client(scopes=scopes)
    return result["access_token"]

def fetch_recent_messages(token, mailbox):
    url = f"https://graph.microsoft.com/v1.0/users/{mailbox}/mailFolders/inbox/messages?$top=50"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()["value"]

# Helper functions for stage management
STAGE_FLOW = {
    'forms': 'payments',
    'payments': 'completed',
}

def get_next_stage(current_stage):
    return STAGE_FLOW.get(current_stage, 'completed')

def notify_admin_of_reply(customer_id, customer_email, current_stage, next_stage):
    # This can be extended to send an email, push notification, etc.
    print(f"[ADMIN-NOTIFY] Customer {customer_id} ({customer_email}) replied in stage '{current_stage}'. Moving to '{next_stage}'.")

# --- Database initialization ---
async def init_db() -> bool:
    """Initialize database (migrations, schema setup, etc.)"""
    try:
        # Create connection to run migrations
        conn = await asyncpg.connect(POSTGRES_DSN)
        try:
            # Check if tables exist, if not create schema
            tables_result = await conn.fetch("""
                SELECT tablename FROM pg_tables 
                WHERE schemaname = 'public'
            """)
            
            if not tables_result:
                logger.info("[DB] No tables found, running initialization...")
                # Run any schema initialization if needed
                # This is a placeholder - actual migrations should be in SQL files
                pass
            else:
                logger.info(f"[DB] Found {len(tables_result)} existing tables")
            
            # Initialize contact messages table for custom message feature
            try:
                await create_contact_messages_table(conn)
            except Exception as e:
                logger.warning(f"[DB] Could not create contact_messages table: {e}")
            
            return True
        finally:
            await conn.close()
    except Exception as e:
        logger.error(f"[DB] init_db failed: {e}")
        return False

# --- FastAPI lifespan event handler ---
db_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    try:
        # Set up local database first, with a few retries
        max_init_attempts = 3
        for attempt in range(max_init_attempts):
            try:
                success = await init_db()
                if success:
                    break
                if attempt < max_init_attempts - 1:
                    logger.warning(f"[DB] Database init failed, retrying ({attempt + 1}/{max_init_attempts})")
                    await asyncio.sleep(2)
                else:
                    msg = "[DB] Failed to initialize database after all retries"
                    logger.error(msg)
                    raise RuntimeError(msg)
            except Exception as e:
                if attempt < max_init_attempts - 1:
                    logger.exception(f"[DB] Database init error, retrying ({attempt + 1}/{max_init_attempts})")
                    await asyncio.sleep(2)
                else:
                    logger.exception("[DB] Database init failed with unhandled error")
                    raise

        # Create database connection pool (don't override if already set by tests/mocks)
        if db_pool is None:
            db_pool = await asyncpg.create_pool(dsn=POSTGRES_DSN, min_size=5, max_size=20)
            print("ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¦ Database connection pool created")
        else:
            print("... Using existing db_pool (pre-initialized by tests or external code)")

        # Initialize monitoring service
        await init_monitoring_service(db_pool)
        print("ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¦ Monitoring service initialized")

        # Start background workers
        asyncio.create_task(send_email_worker())
        asyncio.create_task(campaign_worker())
        asyncio.create_task(reply_checker_worker())
        print("ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¦ Background workers started")

        # Expose pool on app.state for other modules / tests that look there
        try:
            app.state.db_pool = db_pool
        except Exception:
            pass

        # Start daily summary loop (sends daily monitoring summary to notification email)
        try:
            from monitoring import monitoring_service

            async def daily_summary_loop():
                while True:
                    try:
                        if monitoring_service:
                            await monitoring_service.send_daily_summary()
                        await asyncio.sleep(24 * 3600)
                    except Exception as e:
                        logger.error(f"Daily summary loop error: {e}")
                        await asyncio.sleep(60)

            asyncio.create_task(daily_summary_loop())
            print("ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¦ Daily summary scheduler started")
        except Exception as e:
            print(f"Failed to start daily summary scheduler: {e}")

        yield
    finally:
        if db_pool:
            try:
                await db_pool.close()
            except Exception:
                pass
            print("ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¾ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¦ Closed database connection pool")
        # Clear state reference if present
        try:
            if hasattr(app, 'state'):
                setattr(app.state, 'db_pool', None)
        except Exception:
            pass


def get_db_pool():
    """Return the active asyncpg pool if available.

    Prefer the pool attached to app.state (the canonical running FastAPI
    instance). Fall back to the module-level `db_pool` variable. Returns
    None if no pool is available yet.
    """
    try:
        # Prefer app.state.db_pool when app has been created
        if 'app' in globals() and getattr(globals()['app'], 'state', None):
            pool = getattr(globals()['app'].state, 'db_pool', None)
            if pool:
                return pool
    except Exception:
        pass

    # Fallback to module-level reference
    return globals().get('db_pool', None)


async def get_db_pool_async(timeout_seconds: float = 5.0, interval: float = 0.1):
    """Asynchronously wait up to timeout_seconds for the DB pool to become available.

    Returns the pool if available, or None if the timeout is reached.
    This is intended for request handlers that may race the app startup.
    """
    try:
        loop = asyncio.get_event_loop()
    except Exception:
        loop = None

    end_time = None
    try:
        if loop:
            end_time = loop.time() + float(timeout_seconds)
    except Exception:
        end_time = None

    logger.debug(f"[DB] get_db_pool_async: waiting up to {timeout_seconds}s for pool")
    while True:
        pool = get_db_pool()
        if pool:
            logger.debug("[DB] get_db_pool_async: pool found")
            return pool
        # If we have an event loop and end_time, check timeout
        if end_time is not None and loop.time() >= end_time:
            return None
        try:
            await asyncio.sleep(interval)
        except Exception:
            # On any sleep error, return what we have (likely None)
            logger.exception("[DB] get_db_pool_async: sleep interrupted")
            return get_db_pool()
    # Shouldn't reach here; return final pool or None
    pool = get_db_pool()
    if not pool:
        logger.warning("[DB] get_db_pool_async: timeout reached without pool")
    return pool

app = FastAPI(lifespan=lifespan)

# Register all routers in a specific order
routers_to_register = []

# 1. Include monitoring router
routers_to_register.append(("monitoring", monitoring_router))

# 2. Include tasks router - import once at module level
try:
    from tasks import router as tasks_router
    routers_to_register.append(("tasks", tasks_router))
except ImportError as e:
    logger.error(f"[MAIN] Failed to import tasks router: {e}", exc_info=True)
    raise RuntimeError(f"Critical module 'tasks' could not be imported: {e}")
except Exception as e:
    logger.error(f"[MAIN] Unexpected error importing tasks router: {e}", exc_info=True)
    raise

# 3. Queue overview router
try:
    from api_queue_overview import router as queue_overview_router
    routers_to_register.append(("queue_overview", queue_overview_router))
except Exception as e:
    logger.warning(f"[MAIN] Failed to import queue_overview router: {e}")

# 4. Excel preview / import router (provides preview and selective import endpoints)
try:
    from excel_preview import router as excel_preview_router
    routers_to_register.append(("excel_preview", excel_preview_router))
except Exception as e:
    logger.warning(f"[MAIN] Failed to import excel_preview router: {e}")

# 5. Contact relations router
try:
    from contact_relations import router as contact_relations_router
    routers_to_register.append(("contact_relations", contact_relations_router))
except ImportError as e:
    logger.warning(f"[MAIN] Failed to import contact_relations router: {e}")
except Exception as e:
    logger.warning(f"[MAIN] Unexpected error importing contact_relations router: {e}")

# 6. Organizations router

# Register all routers and log their routes
for router_name, router in routers_to_register:
    try:
        app.include_router(router)
        routes = [route.path for route in router.routes]
        logger.info(f"[MAIN] Included {router_name} router with routes: {routes}")
    except Exception as e:
        logger.error(f"[MAIN] Failed to register {router_name} router: {e}", exc_info=True)
        if router_name == "tasks":  # tasks router is critical
            raise


    # Temporary debug endpoint: list registered routes
    @app.get("/__debug/routes")
    async def debug_routes():
        """Return a serialized list of registered routes for debugging.

        Safe to call without auth. Remove this endpoint after debugging.
        """
        out = []
        for r in app.routes:
            try:
                methods = sorted(list(r.methods)) if getattr(r, 'methods', None) else None
            except Exception:
                methods = None
            out.append({
                'path': getattr(r, 'path', None),
                'name': getattr(r, 'name', None),
                'methods': methods,
                'repr': repr(r)
            })
        return out
try:
    from api_queue_overview import router as queue_overview_router
    app.include_router(queue_overview_router)
    logger.info('Included api_queue_overview router')
except Exception as e:
    logger.warning(f'Failed to include api_queue_overview router: {e}')

# ========================================
# MONITORING API ENDPOINTS (Direct Implementation)
# ========================================

# Simple in-memory job store for background Excel processing
JOBS: Dict[str, Dict[str, Any]] = {}

async def _process_upload_job(job_id: str, file_bytes: bytes, filename: str, user: dict):
    """Background worker to process uploaded Excel, validate and save results to disk"""
    JOBS[job_id]['status'] = 'running'
    JOBS[job_id]['started_at'] = datetime.now().isoformat()
    JOBS[job_id]['results'] = []  # Store results for streaming
    try:
        # reuse existing upload logic by writing a small wrapper that mimics UploadFile
        import pandas as pd
        from io import BytesIO

        buf = BytesIO(file_bytes)
        df = pd.read_excel(buf)
        # call existing upload-excel logic by invoking function internals
        # For simplicity, re-implement lightweight per-row validation+save here
        results = []
        
        # Get database pool properly
        pool = get_db_pool()
        if not pool:
            pool = await get_db_pool_async(timeout_seconds=3.0)
        if not pool:
            logger.error("[UPLOAD] Database pool not available")
            raise Exception("Database connection not available")

        async with pool.acquire() as conn:
            available_columns = await conn.fetch("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'campaign_contacts' AND table_schema = 'public'
            """)
            available_column_names = {col['column_name'] for col in available_columns}

            total = len(df.index)
            # persist initial job metadata
            await conn.execute("INSERT INTO upload_jobs (id, filename, created_by, status, total_rows, processed_rows) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT (id) DO UPDATE SET status=$4, total_rows=$5",
                               job_id, filename, user.get('id'), 'running', total, 0)

            # OPTIMIZATION: Pre-validate all emails in parallel instead of sequential
            logger.info(f"[UPLOAD] Pre-validating {total} emails in parallel...")
            validation_cache = {}
            
            # Extract all emails and validate in parallel with batching
            emails_to_validate = []
            for idx, row in df.iterrows():
                raw_email = row.get('email')
                if raw_email and not (isinstance(raw_email, float) and pd.isna(raw_email)):
                    emails_list = process_emails(str(raw_email), validate=True)
                    if emails_list:
                        primary_email = emails_list[0].lower()
                        if primary_email not in validation_cache:
                            emails_to_validate.append(primary_email)
            
            # Validate all unique emails in parallel (leverages semaphore)
            if emails_to_validate:
                validation_tasks = [call_validator(email) for email in emails_to_validate]
                validation_results = await asyncio.gather(*validation_tasks, return_exceptions=True)
                for email, result in zip(emails_to_validate, validation_results):
                    if isinstance(result, Exception):
                        validation_cache[email] = {'code': 0, 'reason': f"Validation error: {str(result)}", 'validation_result': None, 'valid': False, 'raw': None}
                    else:
                        validation_cache[email] = result
                logger.info(f"[UPLOAD] Pre-validation complete. Cached {len(validation_cache)} unique emails")
            
            processed = 0
            for idx, row in df.iterrows():
                try:
                    raw_email = row.get('email')
                    if raw_email is None or (isinstance(raw_email, float) and pd.isna(raw_email)):
                        result_row = {'row': idx + 2, 'email': None, 'status': 'skipped', 'reason': 'Missing email'}
                        results.append(result_row)
                        JOBS[job_id]['results'].append(result_row)
                        continue

                    emails_list = process_emails(str(raw_email), validate=True)
                    if not emails_list:
                        result_row = {'row': idx + 2, 'email': str(raw_email), 'status': 'skipped', 'reason': 'No valid emails'}
                        results.append(result_row)
                        JOBS[job_id]['results'].append(result_row)
                        continue

                    primary_email = emails_list[0].lower()
                    # Use cached validation result
                    validator_info = validation_cache.get(primary_email, {'code': 0, 'reason': 'Not validated', 'validation_result': None, 'valid': False, 'raw': None})
                    validation_result_text = validator_info.get('reason') or validator_info.get('validation_result') or ("Valid" if validator_info.get('valid') else "Invalid")

                    # Build fields for insert/update
                    primary_and_others = emails_list
                    # dedupe
                    seen = set(); full_list = []
                    for e in primary_and_others:
                        if e and e not in seen:
                            seen.add(e); full_list.append(e)
                    email_column_value = ','.join(full_list)
                    CCs = []
                    for e in full_list[1:]:
                        if e and e not in CCs:
                            CCs.append(e)
                    cc_store_str = ','.join(CCs) if CCs else None

                    # Also check for existing matches across any event (by email or exact name)
                    # Match by exact email OR email containing the primary (to handle stored comma-separated lists),
                    # or exact name match (case-insensitive).
                    matches = await conn.fetch(
                        "SELECT id, name, email, event_id FROM campaign_contacts WHERE (LOWER(email) = LOWER($1) OR LOWER(email) LIKE '%' || LOWER($1) || '%' OR (name IS NOT NULL AND LOWER(name)=LOWER($2)))",
                        primary_email, (row.get('name') or '')
                    )
                    matches_list = []
                    for m in matches:
                        try:
                            matches_list.append({'id': m['id'], 'name': m.get('name'), 'email': m.get('email'), 'event_id': m.get('event_id')})
                        except Exception:
                            continue

                    existing = await conn.fetchrow(
                        "SELECT id, email, cc_store FROM campaign_contacts WHERE LOWER(email) = LOWER($1) AND event_id = $2",
                        primary_email, row.get('event_id')
                    )

                    if existing:
                        # update
                        await conn.execute(
                            "UPDATE campaign_contacts SET name=$1, email=$2, cc_store=$3, validation_result=$4 WHERE id=$5",
                            row.get('name'), email_column_value, cc_store_str, validation_result_text, existing['id']
                        )
                        result_row = {'row': idx+2, 'email': primary_email, 'status': 'updated', 'validation_result': validation_result_text, 'validator_code': validator_info.get('code'), 'matches': json.dumps(matches_list) if matches_list else None}
                        results.append(result_row)
                        JOBS[job_id]['results'].append(result_row)
                    else:
                        # prepare dynamic insert
                        dynamic_fields = ['name','email','cc_store','event_id','status','stage','campaign_paused','validation_result']
                        dynamic_values = [row.get('name'), email_column_value, cc_store_str, row.get('event_id'), 'pending','initial', True, validation_result_text]
                        # include other available columns
                        for col in df.columns:
                            if col.lower() in available_column_names and col.lower() not in [f.lower() for f in dynamic_fields]:
                                val = row.get(col)
                                if pd.notna(val):
                                    dynamic_fields.append(col.lower()); dynamic_values.append(val)

                        placeholders = ', '.join([f'${i+1}' for i in range(len(dynamic_values))])
                        fields_str = ', '.join(dynamic_fields)
                        insert_sql = f"INSERT INTO campaign_contacts ({fields_str}) VALUES ({placeholders}) RETURNING id"
                        await conn.fetchrow(insert_sql, *dynamic_values)

                        result_row = {'row': idx+2, 'name': row.get('name'), 'email': primary_email, 'status': 'added', 'validation_result': validation_result_text, 'validator_code': validator_info.get('code'), 'matches': json.dumps(matches_list) if matches_list else None}
                        results.append(result_row)
                        JOBS[job_id]['results'].append(result_row)

                    processed += 1
                    # update processed_rows in upload_jobs
                    await conn.execute("UPDATE upload_jobs SET processed_rows = $1 WHERE id = $2", processed, job_id)
                except Exception as e:
                    logger.error(f"Error processing row {idx+2} in job {job_id}: {e}")
                    result_row = {'row': idx+2, 'name': row.get('name'), 'email': row.get('email'), 'status': 'skipped', 'reason': str(e)}
                    results.append(result_row)
                    JOBS[job_id]['results'].append(result_row)
                    continue

        # Mark job as finished IMMEDIATELY - no Excel file generation needed
        JOBS[job_id]['status'] = 'finished'
        JOBS[job_id]['finished_at'] = datetime.now().isoformat()
        
        # Update job status to finished NOW (skip Excel generation entirely)
        try:
            async with pool.acquire() as conn:
                await conn.execute("UPDATE upload_jobs SET status=$1, finished_at=$2, processed_rows=$3 WHERE id=$4",
                                   'finished', datetime.now().strftime('%Y-%m-%d %H:%M:%S'), processed, job_id)
            logger.info(f"[UPLOAD] Job {job_id} completed. {processed} rows processed. Excel generation skipped.")
        except Exception as e:
            logger.error(f"Error updating job status for {job_id}: {e}")
    except Exception as e:
        # persist failure
        try:
            async with pool.acquire() as conn:
                await conn.execute("UPDATE upload_jobs SET status=$1, error=$2 WHERE id=$3", 'failed', str(e), job_id)
        except Exception:
            pass
        JOBS[job_id]['status'] = 'failed'
        JOBS[job_id]['error'] = str(e)
        logger.error(f"Upload job {job_id} failed: {e}")


# Lightweight early auth dependency so endpoints defined earlier can use Depends(get_current_user).
# This is intentionally defined before the larger auth block later in the file to avoid
# NameError at import/definition time for endpoints that reference the dependency.
SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-here')
security = HTTPBearer()

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired"
        )
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )

# Organizations endpoints (inlined after get_current_user so dependency is available)
from pydantic import BaseModel

from typing import List, Optional


class OrgCreate(BaseModel):
    name: str


class OrgOut(BaseModel):
    id: int
    name: str
    created_by: Optional[str]
    created_at: str
    attachment_url: Optional[str] = None
    note: Optional[str] = None


async def require_admin(user = Depends(get_current_user)):
    is_admin = bool(user.get('is_admin'))
    if not is_admin:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required")
    return user

# Remove the 'current_user' argument and the 'Depends' check
@app.get("/admin/sender-capacities")
async def get_sender_capacities():
    """
    Get current capacity and load for all email senders.
    ACCESSIBLE BY: All users (Public/Standard Auth)
    """
    try:
        # Debug: Print to terminal to track requests
        print("[DEBUG] Fetching sender capacities (Public Access)...")
        
        async with db_pool.acquire() as conn:
            CAPACITY_PER_SENDER = 50
            
            # 1. Query: Sum of ONLY expected_contact_count
            rows = await conn.fetch(
                """
                SELECT 
                    sender_email, 
                    COALESCE(SUM(expected_contact_count), 0) as total_load
                FROM event
                WHERE sender_email = ANY($1::text[])
                GROUP BY sender_email
                """,
                ALLOWED_SENDERS
            )
            
            # 2. Map DB results
            current_loads = {row['sender_email']: row['total_load'] for row in rows}
            
            # 3. Ensure all senders exist
            for sender in ALLOWED_SENDERS:
                if sender not in current_loads:
                    current_loads[sender] = 0
            
            # 4. Calculate Active Batch
            sender_tiers = {email: load // CAPACITY_PER_SENDER for email, load in current_loads.items()}
            min_tier = min(sender_tiers.values()) if sender_tiers else 0

            # 5. Build response
            senders_data = []
            for sender in ALLOWED_SENDERS:
                load = current_loads[sender]
                tier = sender_tiers[sender]
                
                load_in_current_batch = load % CAPACITY_PER_SENDER
                capacity_remaining = CAPACITY_PER_SENDER - load_in_current_batch
                is_active_turn = (tier == min_tier)
                
                senders_data.append({
                    "sender_email": sender,
                    "total_load": load,
                    "current_batch_number": tier + 1,
                    "load_in_current_batch": load_in_current_batch,
                    "capacity_per_sender": CAPACITY_PER_SENDER,
                    "capacity_remaining": capacity_remaining,
                    "is_active_turn": is_active_turn,
                    "status": "Active" if is_active_turn else "Waiting"
                })
            
            return {
                "senders": senders_data,
                "total_senders": len(ALLOWED_SENDERS),
                "capacity_per_sender": CAPACITY_PER_SENDER,
                "current_active_batch": min_tier + 1
            }
    
    except Exception as e:
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("ERROR IN SENDER CAPACITIES:")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Server Error: {str(e)}")


@app.get("/organizations/", response_model=List[OrgOut])
async def list_orgs():
    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async()
    if not pool:
        raise HTTPException(status_code=503, detail="Database connection not available")

    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, created_by, created_at, attachment_url, note FROM organizations WHERE COALESCE(archived, FALSE) = FALSE ORDER BY name")
        out = []
        for r in rows:
            out.append({
                'id': r['id'],
                'name': r['name'],
                'created_by': r['created_by'],
                'created_at': r['created_at'].isoformat() if r['created_at'] else None,
                'attachment_url': r.get('attachment_url'),
                'note': r.get('note') if 'note' in r.keys() else None
            })
        return out


@app.get("/organizations/event_counts")
async def organizations_event_counts():
    """Return event counts per organization in a single call.

    Returns a list of objects: { id, name, event_count }
    """
    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async()
    if not pool:
        raise HTTPException(status_code=503, detail="Database connection not available")

    async with pool.acquire() as conn:
        try:
            rows = await conn.fetch(
                """
                SELECT o.id, o.name, COUNT(e.id) AS event_count
                FROM organizations o
                LEFT JOIN event e ON e.org_id = o.id
                WHERE COALESCE(o.archived, FALSE) = FALSE
                GROUP BY o.id, o.name
                ORDER BY o.name
                """
            )
            return [ {'id': r['id'], 'name': r['name'], 'event_count': int(r['event_count'])} for r in rows ]
        except Exception as e:
            logger.exception(f"Failed to fetch organization event counts: {e}")
            raise HTTPException(status_code=500, detail='Failed to fetch organization event counts')


@app.post("/organizations/add", response_model=OrgOut, status_code=status.HTTP_201_CREATED)
async def add_org(payload: OrgCreate, user = Depends(require_admin)):
    name = (payload.name or '').strip()
    if not name:
        raise HTTPException(status_code=400, detail='Name required')

    # Debug: log caller and whether a DB pool is already available
    try:
        logger.debug(f"[ORG] add_org called by user={user.get('username')} id={user.get('user_id')} is_admin={user.get('is_admin')} payload_name='{name}'")
    except Exception:
        try:
            logger.debug("[ORG] add_org called (failed to read user info)")
        except Exception:
            pass

    pool = get_db_pool()
    try:
        logger.debug(f"[ORG] initial db_pool present: {bool(pool)}")
    except Exception:
        pass
    if not pool:
        try:
            pool = await get_db_pool_async(timeout_seconds=10.0)
        except Exception as e:
            logger.exception(f"[ORG] get_db_pool_async failed: {e}")
            raise HTTPException(status_code=503, detail="Database connection not available (startup in progress)")
    if not pool:
        logger.error("[ORG] No DB pool available when handling add_org")
        raise HTTPException(status_code=503, detail="Database connection not available")

    async with pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT id FROM organizations WHERE LOWER(name)=LOWER($1)", name)
        if existing:
            raise HTTPException(status_code=409, detail='Organization already exists')

        created_by = user.get('user_id') or user.get('id') or user.get('username')
        row = await conn.fetchrow(
            "INSERT INTO organizations (name, created_by, created_at) VALUES ($1,$2,CURRENT_TIMESTAMP) RETURNING id, name, created_by, created_at",
            name, created_by
        )
        return { 'id': row['id'], 'name': row['name'], 'created_by': row['created_by'], 'created_at': row['created_at'].isoformat() }


@app.patch('/organizations/{org_id}')
async def patch_organization(org_id: int, payload: dict = Body(...), current_user: dict = Depends(require_admin)):
    """Patch organization fields (e.g., note, attachment_url, name). Requires admin access."""
    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async()
    if not pool:
        raise HTTPException(status_code=503, detail='Database connection not available')

    allowed = {'note', 'attachment_url', 'name'}
    updates = {k: v for k, v in payload.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail='No updatable fields provided')

    set_parts = []
    vals = []
    i = 1
    for k, v in updates.items():
        set_parts.append(f"{k} = ${i}")
        vals.append(v)
        i += 1
    vals.append(org_id)
    sql = f"UPDATE organizations SET {', '.join(set_parts)} WHERE id = ${i} RETURNING id, name, created_by, created_at, attachment_url, note"

    async with pool.acquire() as conn:
        try:
            row = await conn.fetchrow(sql, *vals)
        except Exception as e:
            logger.exception(f"Failed to patch organization {org_id}: {e}")
            raise HTTPException(status_code=500, detail='Failed to update organization')

        if not row:
            raise HTTPException(status_code=404, detail='Organization not found')

        return {
            'id': row['id'],
            'name': row['name'],
            'created_by': row['created_by'],
            'created_at': row['created_at'].isoformat() if row['created_at'] else None,
            'attachment_url': row.get('attachment_url'),
            'note': row.get('note')
        }


@app.delete('/organizations/{org_id}', status_code=status.HTTP_204_NO_CONTENT)
async def delete_organization(org_id: int, current_user: dict = Depends(require_admin)):
    """Permanently delete an organization (hard delete). Requires admin access.

    Note: events referencing this organization have a foreign key with
    ON DELETE SET NULL, so deleting an organization will leave related
    events intact but with org_id set to NULL.
    """
    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async()
    if not pool:
        raise HTTPException(status_code=503, detail='Database connection not available')

    async with pool.acquire() as conn:
        try:
            # Hard-delete the organization row
            row = await conn.fetchrow('DELETE FROM organizations WHERE id = $1 RETURNING id', org_id)
        except Exception as e:
            logger.exception(f"Failed to delete organization {org_id}: {e}")
            raise HTTPException(status_code=500, detail='Failed to delete organization')

        if not row:
            raise HTTPException(status_code=404, detail='Organization not found')

        # Optionally log activity
        try:
            await log_user_activity(conn, current_user, 'delete_organization', f"Deleted organization {org_id}", target_type='organization', target_id=org_id)
        except Exception:
            pass

        return Response(status_code=status.HTTP_204_NO_CONTENT)

    logger.info('[MAIN] Organizations endpoints registered inline')
@app.get('/search')
async def search_all(q: str = Query(..., min_length=1), current_user: dict = Depends(get_current_user)):
    """Search organizations, events and contacts by a free-text query.

    Returns JSON with keys: organizations, events, contacts.
    """
    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async()
    if not pool:
        raise HTTPException(status_code=503, detail='Database connection not available')

    pattern = f"%{q}%"
    async with pool.acquire() as conn:
        try:
            org_rows = await conn.fetch(
                "SELECT id, name, note, attachment_url, created_at FROM organizations WHERE name ILIKE $1 OR COALESCE(note,'') ILIKE $1 ORDER BY name LIMIT 200",
                pattern
            )
            event_rows = await conn.fetch(
                """
                SELECT e.id, e.event_name, e.date2, e.sender_email, e.city, e.venue, e.org_id, COALESCE(e.org_name, o.name) AS org_name
                FROM event e
                LEFT JOIN organizations o ON e.org_id = o.id
                WHERE e.event_name ILIKE $1 OR COALESCE(e.city,'') ILIKE $1
                ORDER BY e.date2 NULLS LAST
                LIMIT 500
                """,
                pattern
            )
            contact_rows = await conn.fetch(
                """
                SELECT c.id, c.name, c.email, c.event_id, e.event_name
                FROM campaign_contacts c
                LEFT JOIN event e ON c.event_id = e.id
                WHERE c.name ILIKE $1 OR c.email ILIKE $1
                LIMIT 1000
                """,
                pattern
            )
            return {
                'organizations': [ { 'id': r['id'], 'name': r['name'], 'note': r.get('note'), 'attachment_url': r.get('attachment_url'), 'created_at': r['created_at'].isoformat() if r['created_at'] else None } for r in org_rows ],
                'events': [ dict(r) for r in event_rows ],
                'contacts': [ dict(r) for r in contact_rows ]
            }
        except Exception as e:
            logger.exception(f"Search failed for query '{q}': {e}")
            raise HTTPException(status_code=500, detail='Search failed')


@app.post('/search/advanced')
async def search_advanced(
    query: str = Query(..., min_length=1),
    columns: List[str] = Query(['all']),
    current_user: dict = Depends(get_current_user)
):
    """Advanced search with the same logic as the frontend MainApp.tsx search.
    
    This implements:
    - Normalization of search queries
    - '+' token support for AND matching
    - Column filtering
    - Intelligent fallback search across event and contact fields
    - Event name matching with full contact listing
    
    Args:
        query: Normalized search query (trimmed, lowercase)
        columns: List of columns to search in (currently used for validation)
    
    Returns:
        {
            'events': [...filtered events with contacts...],
            'event_match_map': {event_id: bool},  # whether event name matched search
            'filtered_count': int
        }
    """
    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async()
    if not pool:
        raise HTTPException(status_code=503, detail='Database connection not available')
    
    # Normalize query exactly like frontend does
    search_normalized = (query or '').strip().lower()
    
    try:
        async with pool.acquire() as conn:
            # Get all events with their full event data
            all_events = await conn.fetch(
                """
                SELECT e.id, e.event_name, e.org_name, e.month, e.sender_email, 
                       e.city, e.venue, e.date2, e.org_id, e.note, e.event_url,
                       o.name as org_name_from_org, o.attachment_url
                FROM event e
                LEFT JOIN organizations o ON e.org_id = o.id
                ORDER BY e.id DESC
                """
            )
            
            # Get all contacts grouped by event
            contacts_by_event: Dict[int, List[Dict[str, Any]]] = {}
            all_contacts = await conn.fetch(
                """
                SELECT id, event_id, name, phone_number, email, stage, status, trigger, notes, 
                       organizer, hotel_name, supplier, nationality, workplace, 
                       payment_method, forms_link, payment_link, validation_result,
                       source, date, booking_id
                FROM campaign_contacts
                ORDER BY id
                """
            )
            
            for contact_row in all_contacts:
                contact_dict = dict(contact_row)
                event_id = contact_dict.get('event_id')
                if event_id not in contacts_by_event:
                    contacts_by_event[event_id] = []
                contacts_by_event[event_id].append(contact_dict)
            
            # If no search, return all events with all their contacts
            if not search_normalized:
                result_events = []
                for event_row in all_events:
                    event_dict = dict(event_row)
                    event_id = event_dict['id']
                    event_dict['contacts'] = contacts_by_event.get(event_id, [])
                    result_events.append(event_dict)
                
                return {
                    'events': result_events,
                    'event_match_map': {},
                    'filtered_count': len(result_events)
                }
            
            # Apply search filtering logic
            event_match_map: Dict[int, bool] = {}
            event_name_match_map: Dict[int, bool] = {}
            filtered_events = []
            
            for event_row in all_events:
                event_dict = dict(event_row)
                event_id = event_dict['id']
                contacts = contacts_by_event.get(event_id, [])
                
                # Check if event matches the search
                event_matches = False
                
                # Handler for '+' token (AND) search
                if '+' in search_normalized:
                    parts = [p.strip() for p in search_normalized.split('+') if p.strip()]
                    
                    # Check if any part contains ':' for key:value pairs
                    has_kv = any(':' in p for p in parts)
                    if has_kv:
                        # Build key:value map
                        kv_map: Dict[str, str] = {}
                        for p in parts:
                            if ':' in p:
                                k, _, v = p.partition(':')
                                kv_map[k.strip()] = v.strip()
                        
                        # Check if any contact matches all key:value pairs
                        for contact in contacts:
                            if all(
                                str(contact.get(k, '')).lower().find(v.lower()) >= 0
                                for k, v in kv_map.items()
                            ):
                                event_matches = True
                                break
                    elif len(parts) > 1:
                        # Positional mapping: stage, status, trigger
                        cols = ['stage', 'status', 'trigger']
                        for contact in contacts:
                            if all(
                                str(contact.get(cols[idx], '')).lower().find(part.lower()) >= 0
                                for idx, part in enumerate(parts) if idx < len(cols)
                            ):
                                event_matches = True
                                break
                    else:
                        # Single token with '+': prefer whole-word stage/status match
                        token = parts[0].lower() if parts else ''
                        for contact in contacts:
                            stage = str(contact.get('stage', '')).lower()
                            status = str(contact.get('status', '')).lower()
                            
                            # Whole word match
                            if stage == token or status == token:
                                event_matches = True
                                break
                            
                            # Check searchable fields
                            searchable_fields = [
                                'name', 'email', 'phone_number',  'notes', 'organizer', 'hotel_name', 'supplier', 'searchable_fields',
                                'nationality', 'workplace', 'payment_method', 'trigger', 'forms_link', 'payment_link', 'booking_id'
                            ]
                            if any(token in str(contact.get(f, '')).lower() for f in searchable_fields):
                                event_matches = True
                                break
                else:
                    # Regular search (no '+')
                    # First check if event name matches
                    event_name = (event_dict.get('event_name') or '').lower()
                    if search_normalized in event_name:
                        event_matches = True
                        event_name_match_map[event_id] = True
                    else:
                        # Fallback: search across all contact and event fields
                        # Contact full-text
                        contact_text_list = []
                        for contact in contacts:
                            contact_fields = [
                                contact.get('name', ''),
                                str(contact.get('booking_id', '')).strip() if contact.get('booking_id') else '',
                                contact.get('email', ''),
                                contact.get('stage', ''),
                                contact.get('status', ''),
                                contact.get('notes', ''),
                                contact.get('organizer', ''),
                                contact.get('hotel_name', ''),
                                contact.get('supplier', ''),
                                contact.get('nationality', ''),
                                contact.get('workplace', ''),
                                contact.get('payment_method', ''),
                                contact.get('trigger', ''),
                                contact.get('forms_link', ''),
                                contact.get('payment_link', ''),
                            ]
                            contact_text_list.append(' '.join(str(f or '') for f in contact_fields))
                        
                        contact_text = ' '.join(contact_text_list).lower()
                        
                        # Event full-text
                        event_fields = [
                            str(event_dict.get('id') or ''),
                            event_dict.get('event_name', ''),
                            event_dict.get('org_name', ''),
                            event_dict.get('month', ''),
                            event_dict.get('sender_email', ''),
                            event_dict.get('city', ''),
                            event_dict.get('venue', ''),
                            event_dict.get('date2', ''),
                            str(len(contacts)),  # contacts count
                            contact_text,
                        ]
                        event_text = ' '.join(str(f or '') for f in event_fields).lower()
                        
                        if search_normalized in event_text:
                            event_matches = True
                
                if event_matches:
                    event_dict['contacts'] = contacts
                    filtered_events.append(event_dict)
                    event_match_map[event_id] = bool(event_id in event_name_match_map)
            
            return {
                'events': filtered_events,
                'event_match_map': event_match_map,
                'event_name_match_map': event_name_match_map,
                'filtered_count': len(filtered_events)
            }
            
    except Exception as e:
        logger.exception(f"Advanced search failed for query '{query}': {e}")
        raise HTTPException(status_code=500, detail=f'Advanced search failed: {str(e)}')


@app.get("/organizations/{org_id}/events")
async def events_by_org(org_id: int):
    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async()
    if not pool:
        raise HTTPException(status_code=503, detail="Database connection not available")
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT e.id, e.event_name, e.month, e.sender_email, e.city, e.venue, e.date2,
                   COALESCE(e.org_name, o.name) AS org_name
                FROM event e
                LEFT JOIN organizations o ON e.org_id = o.id
                WHERE e.org_id = $1
                ORDER BY e.date2 NULLS LAST
                """,
                org_id
        )
        return [dict(r) for r in rows]
    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async()
    if not pool:
        raise HTTPException(status_code=503, detail="Database connection not available")
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, event_name, month, sender_email, city, venue, date2 FROM event WHERE org_id = $1 ORDER BY date2 NULLS LAST", org_id)
        return [ dict(r) for r in rows ]


@app.get("/organizations/events/{event_id}/contacts")
async def contacts_by_event(event_id: int):
    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async()
    if not pool:
        raise HTTPException(status_code=503, detail="Database connection not available")
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, email, stage, status, last_triggered_at FROM campaign_contacts WHERE event_id = $1 ORDER BY id", event_id)
        out = []
        for r in rows:
            rec = dict(r)
            if rec.get('last_triggered_at'):
                rec['last_triggered_at'] = rec['last_triggered_at'].isoformat()
            out.append(rec)
        return out


@app.get("/organizations/_health")
async def organizations_health():
    """Lightweight health check for organizations router (DB connectivity)."""
    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async(timeout_seconds=2.0)
    if not pool:
        logger.warning("[ORG-HEALTH] no db pool available")
        return {"ok": False, "db_pool": False}
    try:
        async with pool.acquire() as conn:
            await conn.execute('SELECT 1')
        return {"ok": True, "db_pool": True}
    except Exception as e:
        logger.exception(f"[ORG-HEALTH] DB check failed: {e}")
        return {"ok": False, "db_pool": True, "error": str(e)}

logger.info('[MAIN] Organizations endpoints registered inline')


# Organization attachment upload endpoint
@app.post('/organizations/{org_id}/attachment')
async def upload_organization_attachment(org_id: int, request: Request, file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    """Accept a single file upload (image or PDF) for an organization, save to ./uploads and return attachment_url.

    This endpoint requires authentication but does not require admin privileges.
    """
    # Ensure organization exists
    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async()
    if not pool:
        raise HTTPException(status_code=503, detail="Database connection not available")

    async with pool.acquire() as conn:
        org_row = await conn.fetchrow("SELECT id FROM organizations WHERE id = $1", org_id)
        if not org_row:
            raise HTTPException(status_code=404, detail="Organization not found")

    # Prepare uploads dir
    upload_dir = os.path.abspath('./uploads')
    os.makedirs(upload_dir, exist_ok=True)

    # Sanitize filename and create unique name
    filename = (file.filename or 'upload').replace('..', '_').replace('/', '_')
    unique = f"org_{org_id}_{uuid4().hex}_{filename}"
    dest_path = os.path.join(upload_dir, unique)

    try:
        contents = await file.read()
        with open(dest_path, 'wb') as f:
            f.write(contents)
    except Exception as e:
        logger.exception(f"Failed to save uploaded file for org {org_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to save uploaded file")

    # Persist a relative path so stored values do not accidentally embed the
    # frontend host. The frontend will prefix the API base when building links.
    relative_path = f"/uploads/{unique}"

    try:
        async with pool.acquire() as conn:
            try:
                await conn.execute("UPDATE organizations SET attachment_url = $1 WHERE id = $2", relative_path, org_id)
            except Exception:
                # ignore DB schema differences
                logger.debug(f"Could not persist attachment_url to organizations table for org {org_id}")
    except Exception:
        logger.exception(f"Failed to persist attachment_url for org {org_id}")

    # Return relative path; frontend should resolve against the API base.
    return { 'attachment_url': relative_path }


@app.post('/events/{event_id}/attachment')
async def upload_event_attachment(event_id: int, request: Request, file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    """Accept a single file upload (image or PDF) for an event, save to ./uploads and return attachment_url.

    This endpoint requires authentication but does not require admin privileges.
    """
    # Ensure event exists
    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async()
    if not pool:
        raise HTTPException(status_code=503, detail="Database connection not available")

    async with pool.acquire() as conn:
        ev = await conn.fetchrow("SELECT id FROM event WHERE id = $1", event_id)
        if not ev:
            raise HTTPException(status_code=404, detail="Event not found")

    # Prepare uploads dir
    upload_dir = os.path.abspath('./uploads')
    os.makedirs(upload_dir, exist_ok=True)

    # Sanitize filename and create unique name
    filename = (file.filename or 'upload').replace('..', '_').replace('/', '_')
    unique = f"event_{event_id}_{uuid4().hex}_{filename}"
    dest_path = os.path.join(upload_dir, unique)

    try:
        contents = await file.read()
        with open(dest_path, 'wb') as f:
            f.write(contents)
    except Exception as e:
        logger.exception(f"Failed to save uploaded file for event {event_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to save uploaded file")

    relative_path = f"/uploads/{unique}"

    try:
        async with pool.acquire() as conn:
            try:
                await conn.execute("UPDATE event SET attachment_url = $1 WHERE id = $2", relative_path, event_id)
            except Exception:
                # ignore DB schema differences
                logger.debug(f"Could not persist attachment_url to event table for event {event_id}")
    except Exception:
        logger.exception(f"Failed to persist attachment_url for event {event_id}")

    return { 'attachment_url': relative_path }


@app.get('/uploads/{filename}')
async def serve_uploaded_file(filename: str):
    # serve files from ./uploads directory
    file_path = os.path.abspath(os.path.join('./uploads', filename))
    uploads_dir = os.path.abspath('./uploads')
    if not file_path.startswith(uploads_dir):
        raise HTTPException(status_code=400, detail='Invalid filename')
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail='File not found')
    return FileResponse(file_path, filename=filename)


@app.post('/campaign_contacts/upload-excel-job')
async def upload_excel_campaign_contacts_job(file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    # create job id and schedule background task
    contents = await file.read()
    job_id = uuid4().hex
    # determine user identifier from token payload
    user_identifier = current_user.get('user_id') or current_user.get('id') or current_user.get('username')
    JOBS[job_id] = {'status': 'pending', 'created_at': datetime.now().isoformat(), 'filename': file.filename, 'created_by': user_identifier}
    # persist job record (pending)
    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async(timeout_seconds=3.0)
    if not pool:
        raise HTTPException(status_code=503, detail="Database connection not available")

    async with pool.acquire() as conn:
        await conn.execute("INSERT INTO upload_jobs (id, filename, created_by, status, total_rows, processed_rows) VALUES ($1,$2,$3,$4,0,0) ON CONFLICT (id) DO NOTHING",
                           job_id, file.filename, user_identifier, 'pending')
    # schedule background processing
    asyncio.create_task(_process_upload_job(job_id, contents, file.filename, current_user))
    return {'job_id': job_id, 'status': 'pending'}


@app.post('/campaign_contacts/check-duplicates')
async def check_duplicates(payload: dict = Body(...), current_user: dict = Depends(get_current_user)):
    """Bulk check duplicates for a list of rows provided by the client.

    Expects payload: { rows: [ { name: str, email: str, event_id: optional int }, ... ] }
    Returns: { results: [ { index: int, email: str, name: str, matches: [ { id, name, email, event_id } ] }, ... ] }
    """
    rows = payload.get('rows') or []
    if not isinstance(rows, list):
        raise HTTPException(status_code=400, detail='Invalid payload')

    # collect unique emails and names (lowercased)
    emails = []
    names = []
    for r in rows:
        e = (r.get('email') or '')
        if isinstance(e, str):
            el = e.strip().lower()
            if el and el not in emails:
                emails.append(el)
        n = (r.get('name') or '')
        if isinstance(n, str):
            nl = n.strip().lower()
            if nl and nl not in names:
                names.append(nl)

    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async(timeout_seconds=3.0)
    if not pool:
        raise HTTPException(status_code=503, detail='Database connection not available')

    results = []
    try:
        async with pool.acquire() as conn:
            # Build arrays for query parameters. For substring matching of comma-separated stored emails
            # we use LOWER(email) LIKE ANY(array_of_patterns)
            email_eq_array = emails if emails else ['']
            email_like_array = [f'%{e}%' for e in emails] if emails else ['']
            name_eq_array = names if names else ['']

            # Fetch candidate matches in a single query
            sql = """
                SELECT id, name, email, event_id
                FROM campaign_contacts
                WHERE (LOWER(email) = ANY($1) OR LOWER(email) LIKE ANY($2) OR (name IS NOT NULL AND LOWER(name) = ANY($3)))
            """
            fetched = await conn.fetch(sql, email_eq_array, email_like_array, name_eq_array)

            # organize fetched rows for quick matching
            fetched_list = [dict(r) for r in fetched]

            for idx, r in enumerate(rows):
                primary_email = (r.get('email') or '').strip().lower()
                name = (r.get('name') or '').strip().lower()
                matches = []
                if primary_email or name:
                    for f in fetched_list:
                        try:
                            f_email = (f.get('email') or '').lower()
                            f_name = (f.get('name') or '') and f.get('name').lower()
                            # match if stored email equals primary, or contains it, or name equals
                            if primary_email and (f_email == primary_email or primary_email in f_email):
                                matches.append({'id': f.get('id'), 'name': f.get('name'), 'email': f.get('email'), 'event_id': f.get('event_id')})
                                continue
                            if name and f_name and f_name == name:
                                matches.append({'id': f.get('id'), 'name': f.get('name'), 'email': f.get('email'), 'event_id': f.get('event_id')})
                        except Exception:
                            continue

                results.append({'index': idx, 'email': r.get('email'), 'name': r.get('name'), 'matches': matches})

    except Exception as e:
        logger.exception(f"Failed to check duplicates: {e}")
        raise HTTPException(status_code=500, detail='Failed to check duplicates')

    return {'results': results}


@app.post('/campaign_contacts/validate-excel')
async def validate_excel(file: UploadFile = File(...), current_user: dict = Depends(get_current_user)):
    """Validate an uploaded Excel file and perform duplicate checks WITHOUT persisting any rows.

    Returns a JSON object with per-row validation results and detected matches so the
    frontend can present the findings and ask for confirmation before saving.
    """
    try:
        contents = await file.read()
        import pandas as pd
        from io import BytesIO
        buf = BytesIO(contents)
        df = pd.read_excel(buf)
        df = df.where(pd.notna(df), None)
    except Exception as e:
        logger.exception(f"Failed to read uploaded excel for validation: {e}")
        raise HTTPException(status_code=400, detail='Failed to parse Excel file')

    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async(timeout_seconds=3.0)
    if not pool:
        raise HTTPException(status_code=503, detail='Database connection not available')

    rows = df.to_dict(orient='records')
    results = []
    try:
        async with pool.acquire() as conn:
            # Prefetch candidate rows to reduce per-row queries when possible
            # Collect emails/names from the sheet
            emails = []
            names = []
            for r in rows:
                e = (r.get('email') or '')
                if isinstance(e, str):
                    el = e.strip().lower()
                    if el and el not in emails:
                        emails.append(el)
                n = (r.get('name') or '')
                if isinstance(n, str):
                    nl = n.strip().lower()
                    if nl and nl not in names:
                        names.append(nl)

            email_eq_array = emails if emails else ['']
            email_like_array = [f'%{e}%' for e in emails] if emails else ['']
            name_eq_array = names if names else ['']

            sql = """
                SELECT id, name, email, event_id
                FROM campaign_contacts
                WHERE (LOWER(email) = ANY($1) OR LOWER(email) LIKE ANY($2) OR (name IS NOT NULL AND LOWER(name) = ANY($3)))
            """
            fetched = await conn.fetch(sql, email_eq_array, email_like_array, name_eq_array)
            fetched_list = [dict(r) for r in fetched]

            # Per-row validation similar to background job but no inserts/updates
            for idx, r in enumerate(rows):
                row_info: Dict[str, Any] = {'row': idx + 2, 'email': r.get('email'), 'name': r.get('name')}
                raw_email = r.get('email')
                if raw_email is None:
                    row_info.update({'status': 'skipped', 'reason': 'Missing email'})
                    results.append(row_info)
                    continue

                emails_list = process_emails(str(raw_email), validate=True)
                if not emails_list:
                    row_info.update({'status': 'skipped', 'reason': 'No valid emails'})
                    results.append(row_info)
                    continue

                primary_email = emails_list[0]
                # call validator but tolerate failures
                validator_info = {'code': None, 'reason': None, 'valid': False, 'raw': None}
                validation_result_text = None
                try:
                    validator_info = await call_validator(primary_email)
                    validation_result_text = validator_info.get('reason') or validator_info.get('validation_result') or ("Valid" if validator_info.get('valid') else "Invalid")
                except Exception as e:
                    validation_result_text = f"Validator error: {str(e)}"

                # find matches from prefetched candidates
                matches = []
                try:
                    name_val = (r.get('name') or '').strip().lower()
                    p_email = (primary_email or '').lower()
                    for f in fetched_list:
                        try:
                            f_email = (f.get('email') or '').lower()
                            f_name = (f.get('name') or '') and f.get('name').lower()
                            if p_email and (f_email == p_email or p_email in f_email):
                                matches.append({'id': f.get('id'), 'name': f.get('name'), 'email': f.get('email'), 'event_id': f.get('event_id')})
                                continue
                            if name_val and f_name and f_name == name_val:
                                matches.append({'id': f.get('id'), 'name': f.get('name'), 'email': f.get('email'), 'event_id': f.get('event_id')})
                        except Exception:
                            continue
                except Exception:
                    matches = []

                row_info.update({'status': 'validated', 'validation_result': validation_result_text, 'validator_code': validator_info.get('code'), 'matches': matches})
                results.append(row_info)

    except Exception as e:
        logger.exception(f"Failed to validate uploaded Excel: {e}")
        raise HTTPException(status_code=500, detail='Failed to validate Excel')

    # counts for convenience
    summary = {'total_rows': len(rows), 'matched_rows': sum(1 for r in results if r.get('matches'))}
    try:
        # Log a short debug summary so administrators can trace validation runs
        sample_matches = [r for r in results if r.get('matches')][:5]
        logger.debug(f"Excel validation: total_rows={summary['total_rows']} matched_rows={summary['matched_rows']} sample_matches={sample_matches}")
    except Exception:
        pass
    return {'results': results, 'summary': summary}


@app.get('/campaign_contacts/upload-job/{job_id}/status')
async def upload_job_status(job_id: str, current_user: dict = Depends(get_current_user)):
    # Prefer database-backed job status and enforce owner/admin access
    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async(timeout_seconds=3.0)
    if not pool:
        raise HTTPException(status_code=503, detail="Database connection not available")

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id, filename, created_by, status, created_at, started_at, finished_at, result_file, error, total_rows, processed_rows FROM upload_jobs WHERE id = $1", job_id)
        user_identifier = current_user.get('user_id') or current_user.get('id') or current_user.get('username')
        is_admin = bool(current_user.get('is_admin'))
        if not row:
            job = JOBS.get(job_id)
            if not job:
                raise HTTPException(status_code=404, detail='Job not found')
            # if job has created_by, enforce ownership
            if job.get('created_by') and str(job.get('created_by')) != str(user_identifier) and not is_admin:
                raise HTTPException(status_code=403, detail='forbidden')
            return job
        # enforce owner/admin
        if str(row['created_by']) != str(user_identifier) and not is_admin:
            raise HTTPException(status_code=403, detail='forbidden')
        return dict(row)


@app.get('/campaign_contacts/upload-job/{job_id}/results')
async def upload_job_results(job_id: str, current_user: dict = Depends(get_current_user)):
    """Stream validation results as they are processed (real-time feedback)"""
    job = JOBS.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail='Job not found')
    
    user_identifier = current_user.get('user_id') or current_user.get('id') or current_user.get('username')
    is_admin = bool(current_user.get('is_admin'))
    
    # Enforce owner/admin access
    if job.get('created_by') and str(job.get('created_by')) != str(user_identifier) and not is_admin:
        raise HTTPException(status_code=403, detail='forbidden')
    
    # Return all results collected so far
    results = job.get('results', [])
    return {'results': results, 'job_id': job_id}


@app.get('/campaign_contacts/upload-job/{job_id}/download')
async def upload_job_download(job_id: str, current_user: dict = Depends(get_current_user)):
    # Check DB for job and owner
    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async(timeout_seconds=3.0)
    if not pool:
        raise HTTPException(status_code=503, detail="Database connection not available")

    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT result_file, status, created_by FROM upload_jobs WHERE id=$1", job_id)
        user_identifier = current_user.get('user_id') or current_user.get('id') or current_user.get('username')
        is_admin = bool(current_user.get('is_admin'))
        if not row:
            raise HTTPException(status_code=404, detail='Job not found')
        if str(row['created_by']) != str(user_identifier) and not is_admin:
            raise HTTPException(status_code=403, detail='forbidden')
        if row['status'] != 'finished' or not row['result_file']:
            raise HTTPException(status_code=400, detail='Result not ready')
        return FileResponse(os.path.join('./uploads', row['result_file']), filename=row['result_file'], media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

# Import monitoring utilities
from monitoring_api import create_access_token

# Monitoring helper functions
async def verify_monitoring_token(request: Request):
    """Verify JWT token for monitoring endpoints"""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(status_code=401, detail="Missing authorization")

    token = auth_header.split(' ')[1]
    try:
        SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-here-change-in-production')
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        username = payload.get("sub")
        if username != 'hatem':
            raise HTTPException(status_code=403, detail="Access denied")
        return {"username": username}
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:  # Changed from jwt.JWTError
        raise HTTPException(status_code=401, detail="Invalid token")

@app.get("/api/monitoring/test")
async def monitoring_test():
    """Simple test endpoint to verify router is working"""
    return {"status": "ok", "message": "Monitoring router is working", "timestamp": datetime.now().isoformat()}

@app.get("/api/monitoring/health")
async def monitoring_health_check():
    """Health check endpoint for monitoring system"""
    try:
        # Check if monitoring service is available
        from monitoring import monitoring_service
        if not monitoring_service:
            return {"status": "error", "message": "Monitoring service not initialized"}

        # Quick database connectivity check
        async with monitoring_service.db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")

        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "message": "Monitoring system is operational"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": f"Database connectivity issue: {str(e)}"
        }

@app.post("/api/monitoring/login")
async def monitoring_login(request: Request):
    """Login endpoint for monitoring dashboard - restricted to user 'hatem'"""
    try:
        data = await request.json()
        username = data.get('username')
        password = data.get('password')

        # Only allow user 'hatem'
        if username != 'hatem':
            raise HTTPException(status_code=401, detail="Access denied. Monitoring dashboard is restricted to user 'hatem'.")

        # Check password against database
        async with db_pool.acquire() as conn:
            user = await conn.fetchrow(
                "SELECT id, username, password_hash, is_admin FROM users WHERE username = $1",
                username
            )

            if not user:
                raise HTTPException(status_code=401, detail="User not found")

            # Verify password
            if not bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
                raise HTTPException(status_code=401, detail="Invalid password")

        # Create JWT token
        access_token = create_access_token(data={"sub": username})

        return {
            "access_token": access_token,
            "token_type": "bearer",
            "user": {
                "id": user['id'],
                "username": user['username'],
                "is_admin": user['is_admin']
            }
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Login error: {str(e)}")

@app.get("/api/monitoring/worker-status")
async def get_worker_status(request: Request):
    """Get current status of all background workers"""
    await verify_monitoring_token(request)

    try:
        from monitoring import monitoring_service
        if not monitoring_service:
            raise HTTPException(status_code=500, detail="Monitoring service not available")

        worker_status = await monitoring_service.get_worker_status()
        return {"workers": worker_status}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching worker status: {str(e)}")

@app.get("/api/monitoring/email-queue-status")
async def get_email_queue_status(request: Request):
    """Get detailed email queue status"""
    await verify_monitoring_token(request)

    try:
        from monitoring import monitoring_service
        if not monitoring_service:
            raise HTTPException(status_code=500, detail="Monitoring service not available")

        queue_status = await monitoring_service.get_email_queue_status()
        return queue_status
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching email queue status: {str(e)}")

@app.get("/api/monitoring/campaign-flow")
async def get_campaign_flow(request: Request):
    """Get campaign flow statistics and recent activities"""
    await verify_monitoring_token(request)

    try:
        from monitoring import monitoring_service
        if not monitoring_service:
            raise HTTPException(status_code=500, detail="Monitoring service not available")

        campaign_flow = await monitoring_service.get_campaign_flow()
        return campaign_flow
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching campaign flow: {str(e)}")

@app.get("/api/monitoring/system-errors")
async def get_system_errors(request: Request, limit: int = 50000):
    """Get recent system errors"""
    await verify_monitoring_token(request)

    try:
        from monitoring import monitoring_service
        if not monitoring_service:
            raise HTTPException(status_code=500, detail="Monitoring service not available")

        errors = await monitoring_service.get_system_errors(limit)
        return {"errors": errors}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching system errors: {str(e)}")

@app.get("/api/monitoring/dashboard-overview")
async def get_dashboard_overview(request: Request):
    """Get comprehensive dashboard overview"""
    await verify_monitoring_token(request)

    try:
        from monitoring import monitoring_service
        if not monitoring_service:
            raise HTTPException(status_code=500, detail="Monitoring service not available")

        overview = await monitoring_service.get_dashboard_overview()
        return overview
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching dashboard overview: {str(e)}")

@app.post("/api/monitoring/test-notification")
async def send_test_notification(request: Request):
    """Send test notification to verify email setup"""
    await verify_monitoring_token(request)

    try:
        from monitoring import monitoring_service
        if not monitoring_service:
            raise HTTPException(status_code=500, detail="Monitoring service not available")

        result = await monitoring_service.send_test_notification()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error sending test notification: {str(e)}")

@app.get("/api/contacts/{contact_id}/custom_flow")
async def get_custom_flow(contact_id: int):
    """Get the custom follow-up flow for a contact"""
    async with db_pool.acquire() as conn:
        # Get the custom flow steps for this contact
        steps = await conn.fetch("""
            SELECT cf.id as flow_id, cfs.*
            FROM custom_flows cf
            JOIN custom_flow_steps cfs ON cfs.flow_id = cf.id
            WHERE cf.contact_id = $1
            ORDER BY cfs.step_order
        """, contact_id)

        return {"flow_steps": [dict(step) for step in steps]}

@app.post("/api/contacts/{contact_id}/custom_flow")
async def create_custom_flow(
    contact_id: int,
    request: Request,
    steps: Any = Body(...),
):
    """Create a new custom follow-up flow for a contact.

    Accepts either a JSON array of step objects or a JSON object with a
    `steps` field (legacy clients sometimes POST `{contact_id:..., steps:[...]}`).
    """
    async with db_pool.acquire() as conn:
        try:
            # Normalize incoming payload to a list of steps
            if isinstance(steps, dict):
                if 'steps' in steps and isinstance(steps['steps'], list):
                    steps_list = steps['steps']
                elif 'steps' in steps and isinstance(steps['steps'], dict):
                    # Single-step object encoded incorrectly
                    steps_list = [steps['steps']]
                else:
                    raise HTTPException(status_code=422, detail="Request body must be a list of steps or an object with a 'steps' list")
            elif isinstance(steps, list):
                steps_list = steps
            else:
                raise HTTPException(status_code=422, detail="Request body must be a list of steps or an object with a 'steps' list")
            # Get user info from token for logging
            token = request.headers.get('Authorization', '').split(' ')[1]
            user_info = {'id': None, 'username': None}  # Default values
            try:
                payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
                user_info = {'id': payload.get('id'), 'username': payload.get('sub') or 'system'}
            except Exception:
                # If token is invalid or missing, fallback to system user to avoid null username
                user_info = {'id': None, 'username': 'system'}

            # Start transaction
            async with conn.transaction():
                # Create the custom flow
                flow_id = await conn.fetchval("""
                    INSERT INTO custom_flows (contact_id, created_at, updated_at, active)
                    VALUES ($1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE)
                    RETURNING id
                """, contact_id)

                # Insert all steps using 1-based step_order
                for idx, step in enumerate(steps_list):
                    step_order = idx + 1
                    await conn.execute("""
                        INSERT INTO custom_flow_steps
                        (flow_id, step_order, type, subject, body, delay_days)
                        VALUES ($1, $2, $3, $4, $5, $6)
                    """, flow_id, step_order, step.get('type', 'email'), step.get('subject'),
                         step.get('body'), step.get('delay_days', 0))

                # Log the creation
                await log_user_activity(
                    conn=conn,
                    user_info=user_info,
                    action_type='create_custom_flow',
                    action_description=f'Created custom follow-up flow for contact {contact_id}',
                    target_type='contact',
                    target_id=contact_id,
                    new_values={'steps': steps_list}
                )

                # Mark the contact as having a custom flow so the campaign worker
                # (or our immediate task) will process custom flow steps.
                try:
                    now = datetime.now()
                    # Set last_triggered_at slightly in the past so immediate processing
                    # isn't blocked by the 'recent action (<60s)' guard.
                    from datetime import timedelta as _td
                    last_triggered_value = now - _td(seconds=65)
                    await conn.execute("""
                        UPDATE campaign_contacts
                        SET flow_type = 'custom', last_triggered_at = $1
                        WHERE id = $2
                    """, last_triggered_value, contact_id)
                    # Read back the flow_type to verify the update persisted
                    try:
                        current_flow_type = await conn.fetchval("SELECT flow_type FROM campaign_contacts WHERE id = $1", contact_id)
                        logger.info(f"[CUSTOM FLOW] Updated campaign_contacts.flow_type for contact {contact_id} -> {current_flow_type}")
                    except Exception as _inner:
                        logger.debug(f"[CUSTOM FLOW] Could not read back flow_type for contact {contact_id}: {_inner}")
                except Exception as _:
                    # Non-fatal: continue even if we couldn't update contact metadata
                    logger.debug(f"[CUSTOM FLOW] Could not update contact {contact_id} flow_type metadata")

                # Directly insert the first step into email_queue if the first step
                # is an email and due now. This ensures deterministic immediate behavior
                # for newly-created custom flows.
                try:
                    # Load contact row for fields used below
                    contact = await conn.fetchrow('SELECT * FROM campaign_contacts WHERE id = $1', contact_id)
                    if steps_list and isinstance(steps_list, list) and len(steps_list) > 0:
                        first = steps_list[0]
                        if (first.get('type') or '').lower() == 'email':
                            subject = first.get('subject') or 'Follow-up'
                            body = first.get('body') or ''

                            # Prevent duplicates: check pending/sent for custom-step-1
                            existing = await conn.fetchval("SELECT 1 FROM email_queue WHERE contact_id = $1 AND last_message_type = $2 AND status IN ('pending','sent') LIMIT 1", contact_id, 'custom-step-1')
                            if existing:
                                logger.debug(f"[CUSTOM FLOW] First step already exists in queue for contact {contact_id}")
                            else:
                                cc_recipients = None
                                try:
                                    if contact.get('cc_store'):
                                        parts = [p.strip() for p in re.split(r'[;,\s]+', contact.get('cc_store') or '') if p.strip()]
                                        cc_recipients = ';'.join(parts) if parts else None
                                    else:
                                        parsed = process_emails(contact.get('email') or '', validate=True)
                                        if parsed and len(parsed) > 1:
                                            cc_recipients = ';'.join([p for p in parsed[1:]])
                                except Exception:
                                    cc_recipients = None

                                # Ensure we have a sender_email: prefer contact.sender_email, then event.sender_email, then global SENDER_EMAIL
                                sender_email_val = contact.get('sender_email') if contact else None
                                sender_email_val = sender_email_val.strip() if sender_email_val and isinstance(sender_email_val, str) else None
                                if not sender_email_val:
                                    try:
                                        event_sender = await conn.fetchval('SELECT sender_email FROM event WHERE id = $1', contact.get('event_id') if contact else None)
                                        sender_email_val = event_sender.strip() if event_sender and isinstance(event_sender, str) else None   
                                        if event_sender and '@' not in event_sender:
                                            sender_email_val = event_sender 
                                    except Exception as e:
                                        logger.debug(f"[sENDER EMAIL] Could not fetch event sender_email for contact {contact_id}: {e} ")
                                        
                                if not sender_email_val or '@' not in str(sender_email_val):
                                    sender_email_val = DEFAULT_SENDER_EMAIL

                                recipient_email_val = contact.get('email') if contact else None

                                # Calculate scheduled_at with UK business hours enforcement
                                try:
                                    scheduled_at_val = next_allowed_uk_business_time(now)
                                except Exception as e:
                                    logger.warning(f"[CUSTOM FLOW] Error calculating business hours for contact {contact_id}: {e}. Using now as fallback.")
                                    scheduled_at_val = now

                                await conn.execute('''
                                    INSERT INTO email_queue (contact_id, event_id, sender_email, recipient_email, cc_recipients, subject, message, last_message_type, status, created_at, due_at, scheduled_at, type, attachment, attachment_filename, attachment_mimetype)
                                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', $9, $10, $11, $12, $13, $14, $15)
                                ''', contact_id, contact.get('event_id'), sender_email_val, recipient_email_val, cc_recipients, subject, body, 'custom-step-1', now, now, scheduled_at_val, 'custom', None, None, None)

                                await conn.execute("UPDATE campaign_contacts SET last_triggered_at = $1, last_message_type = $2, status = $3, stage = 'custom' WHERE id = $4", now, 'custom-step-1', 'pending', contact_id)
                                logger.info(f"[CUSTOM FLOW] Inserted first custom step into email_queue for contact {contact_id}, scheduled_at={scheduled_at_val}")
                except Exception as _e:
                    logger.error(f"[CUSTOM FLOW] Failed to insert first step for contact {contact_id}: {_e}", exc_info=True)

                # Do not schedule immediate processing inside the DB transaction ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¾Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€šÃ‚Â¦ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â‚¬Å¾Ã‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã‚Â¦ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬
                # schedule it after the transaction commits so the processing task
                # sees the committed flow_type and new rows.
                created_flow_id = flow_id

            # Transaction block ends here; schedule processing now that the DB state is committed
            try:
                import asyncio as _asyncio
                _asyncio.create_task(process_single_contact_campaign(contact_id))
            except Exception:
                logger.debug(f"[CUSTOM FLOW] Could not schedule immediate processing for contact {contact_id}")

            return {"flow_id": created_flow_id}

        except Exception as e:
            logger.error(f"Error creating custom flow: {e}")
            raise HTTPException(status_code=500, detail=str(e))


# Backwards-compatible aliases for legacy non-/api routes
@app.post("/contacts/{contact_id}/custom_flow")
async def create_custom_flow_alias(contact_id: int, request: Request, steps: Any = Body(None)):
    """Alias for legacy clients that POST to /contacts/{id}/custom_flow.

    This alias is tolerant of legacy clients that may send form-encoded data or
    raw JSON in the body. It will attempt to parse the incoming request body
    into the expected `steps` list and then delegate to the canonical handler.
    """
    # If FastAPI already parsed something into `steps`, try to normalize it into a list
    parsed_steps = None
    if steps:
        try:
            # If it's already a list of dicts, accept it
            if isinstance(steps, list):
                parsed_steps = steps
            # If it's an object like {"steps": [...]}
            elif isinstance(steps, dict) and 'steps' in steps and isinstance(steps['steps'], list):
                parsed_steps = steps['steps']
            # If it's a JSON string passed directly (some clients send steps as a JSON string)
            elif isinstance(steps, str):
                try:
                    obj = json.loads(steps)
                    if isinstance(obj, list):
                        parsed_steps = obj
                    elif isinstance(obj, dict) and 'steps' in obj and isinstance(obj['steps'], list):
                        parsed_steps = obj['steps']
                except Exception:
                    parsed_steps = None
        except Exception:
            parsed_steps = None

    # If we were able to normalize, delegate to canonical handler
    if parsed_steps:
        return await create_custom_flow(contact_id=contact_id, request=request, steps=parsed_steps)

    # Otherwise, read the raw body and try to parse common formats
    raw = await request.body()
    parsed_steps = None
    if raw:
        text = raw.decode('utf-8', errors='ignore').strip()
        # Try JSON first
        try:
            obj = json.loads(text)
            # Accept either {'steps': [...]} or direct [...]
            if isinstance(obj, dict) and 'steps' in obj and isinstance(obj['steps'], list):
                parsed_steps = obj['steps']
            elif isinstance(obj, list):
                parsed_steps = obj
        except Exception:
            parsed_steps = None

        # Try common form-encoded: steps=[{...},...] or steps=<json>
        if parsed_steps is None:
            # Parse key=value pairs
            try:
                from urllib.parse import parse_qs
                qs = parse_qs(text)
                if 'steps' in qs and qs['steps']:
                    # steps may be a JSON string inside the form
                    try:
                        candidate = qs['steps'][0]
                        obj = json.loads(candidate)
                        if isinstance(obj, list):
                            parsed_steps = obj
                    except Exception:
                        # ignore and fall through
                        parsed_steps = None
            except Exception:
                parsed_steps = None

    if not parsed_steps:
        # Let the original handler produce a helpful 422/400 via normal validation
        # but raise a clearer HTTPException for legacy clients
        raise HTTPException(status_code=422, detail="Invalid or missing 'steps' payload; expected JSON list of step objects")

    return await create_custom_flow(contact_id=contact_id, request=request, steps=parsed_steps)


@app.get("/contacts/{contact_id}/custom_flow")
async def get_custom_flow_alias(contact_id: int):
    """Alias for legacy clients that GET /contacts/{id}/custom_flow."""
    return await get_custom_flow(contact_id)


@app.put("/contacts/{contact_id}/custom_flow/{flow_id}")
async def update_custom_flow_alias(contact_id: int, flow_id: int, request: Request, steps: List[Dict[str, Any]] = Body(...)):
    """Alias for legacy clients that PUT /contacts/{id}/custom_flow/{flow_id}."""
    return await update_custom_flow(contact_id=contact_id, flow_id=flow_id, request=request, steps=steps)


@app.delete("/contacts/{contact_id}/custom_flow/{flow_id}")
async def delete_custom_flow_alias(contact_id: int, flow_id: int, request: Request):
    """Alias for legacy clients that DELETE /contacts/{id}/custom_flow/{flow_id}."""
    return await delete_custom_flow(contact_id=contact_id, flow_id=flow_id, request=request)


@app.post("/contacts/{contact_id}/custom_flow/{flow_id}/pause")
async def pause_custom_flow_alias(contact_id: int, flow_id: int, request: Request):
    """Alias for legacy clients that POST pause on /contacts/..."""
    return await pause_custom_flow(contact_id=contact_id, flow_id=flow_id, request=request)


@app.post("/contacts/{contact_id}/custom_flow/{flow_id}/resume")
async def resume_custom_flow_alias(contact_id: int, flow_id: int, request: Request):
    """Alias for legacy clients that POST resume on /contacts/..."""
    return await resume_custom_flow(contact_id=contact_id, flow_id=flow_id, request=request)

@app.put("/api/contacts/{contact_id}/custom_flow/{flow_id}")
async def update_custom_flow(
    contact_id: int,
    flow_id: int,
    request: Request,
    steps: List[Dict[str, Any]] = Body(...),
):
    """Update an existing custom follow-up flow"""
    async with db_pool.acquire() as conn:
        try:
            # Get user info from token for logging
            token = request.headers.get('Authorization', '').split(' ')[1]
            user_info = {'id': None, 'username': None}
            try:
                payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
                user_info = {'id': payload.get('id'), 'username': payload.get('sub')}
            except:
                pass

            # First verify this flow belongs to the contact
            flow = await conn.fetchrow("""
                SELECT * FROM custom_flows
                WHERE id = $1 AND contact_id = $2
            """, flow_id, contact_id)

            if not flow:
                raise HTTPException(status_code=404, detail="Flow not found")

            # Start transaction
            async with conn.transaction():
                # Get existing steps for logging
                old_steps = await conn.fetch("""
                    SELECT * FROM custom_flow_steps
                    WHERE flow_id = $1
                    ORDER BY step_order
                """, flow_id)

                # Delete existing steps
                await conn.execute("""
                    DELETE FROM custom_flow_steps
                    WHERE flow_id = $1
                """, flow_id)

                # Insert new steps
                for i, step in enumerate(steps):
                    await conn.execute("""
                        INSERT INTO custom_flow_steps
                        (flow_id, step_order, type, subject, body, delay_days)
                        VALUES ($1, $2, $3, $4, $5, $6)
                    """, flow_id, i, step['type'], step['subject'],
                         step['body'], step.get('delay_days', 0))

                # Update flow updated_at timestamp
                await conn.execute("""
                    UPDATE custom_flows
                    SET updated_at = CURRENT_TIMESTAMP
                    WHERE id = $1
                """, flow_id)

                # Log the update
                await log_user_activity(
                    conn=conn,
                    user_info=user_info,
                    action_type='update_custom_flow',
                    action_description=f'Updated custom follow-up flow for contact {contact_id}',
                    target_type='contact',
                    target_id=contact_id,
                    old_values={'steps': [dict(step) for step in old_steps]},
                    new_values={'steps': steps}
                )

                return {"success": True}

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating custom flow: {e}")
            raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/contacts/{contact_id}/custom_flow/{flow_id}")
async def delete_custom_flow(
    contact_id: int,
    flow_id: int,
    request: Request
):
    """Delete a custom follow-up flow"""
    async with db_pool.acquire() as conn:
        try:
            # Get user info from token for logging
            token = request.headers.get('Authorization', '').split(' ')[1]
            user_info = {'id': None, 'username': None}
            try:
                payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
                user_info = {'id': payload.get('id'), 'username': payload.get('sub')}
            except:
                pass

            # First verify this flow belongs to the contact
            flow = await conn.fetchrow("""
                SELECT * FROM custom_flows
                WHERE id = $1 AND contact_id = $2
            """, flow_id, contact_id)

            if not flow:
                raise HTTPException(status_code=404, detail="Flow not found")

            # Get existing steps for logging
            old_steps = await conn.fetch("""
                SELECT * FROM custom_flow_steps
                WHERE flow_id = $1
                ORDER BY step_order
            """, flow_id)

            # Start transaction
            async with conn.transaction():
                # Delete steps first (cascade should handle this but being explicit)
                await conn.execute("""
                    DELETE FROM custom_flow_steps
                    WHERE flow_id = $1
                """, flow_id)

                # Delete the flow
                await conn.execute("""
                    DELETE FROM custom_flows
                    WHERE id = $1
                """, flow_id)

                # Log the deletion
                await log_user_activity(
                    conn=conn,
                    user_info=user_info,
                    action_type='delete_custom_flow',
                    action_description=f'Deleted custom follow-up flow for contact {contact_id}',
                    target_type='contact',
                    target_id=contact_id,
                    old_values={'steps': [dict(step) for step in old_steps]}
                )

                return {"success": True}

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting custom flow: {e}")
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/contacts/{contact_id}/custom_flow/{flow_id}/pause")
async def pause_custom_flow(
    contact_id: int,
    flow_id: int,
    request: Request
):
    """Pause a custom follow-up flow"""
    async with db_pool.acquire() as conn:
        try:
            # Get user info from token for logging
            token = request.headers.get('Authorization', '').split(' ')[1]
            user_info = {'id': None, 'username': None}
            try:
                payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
                user_info = {'id': payload.get('id'), 'username': payload.get('sub')}
            except:
                pass

            # First verify this flow belongs to the contact
            flow = await conn.fetchrow("""
                SELECT * FROM custom_flows
                WHERE id = $1 AND contact_id = $2
            """, flow_id, contact_id)

            if not flow:
                raise HTTPException(status_code=404, detail="Flow not found")

            # Update flow status to paused
            await conn.execute("""
                UPDATE custom_flows
                SET active = FALSE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = $1
            """, flow_id)

            # Log the pause
            await log_user_activity(
                conn=conn,
                user_info=user_info,
                action_type='pause_custom_flow',
                action_description=f'Paused custom follow-up flow for contact {contact_id}',
                target_type='contact',
                target_id=contact_id
            )

            return {"success": True}

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error pausing custom flow: {e}")
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/api/contacts/{contact_id}/custom_flow/{flow_id}/resume")
async def resume_custom_flow(
    contact_id: int,
    flow_id: int,
    request: Request
):
    """Resume a paused custom follow-up flow"""
    async with db_pool.acquire() as conn:
        try:
            # Get user info from token for logging
            token = request.headers.get('Authorization', '').split(' ')[1]
            user_info = {'id': None, 'username': None}
            try:
                payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
                user_info = {'id': payload.get('id'), 'username': payload.get('sub')}
            except:
                pass

            # First verify this flow belongs to the contact
            flow = await conn.fetchrow("""
                SELECT * FROM custom_flows
                WHERE id = $1 AND contact_id = $2
            """, flow_id, contact_id)

            if not flow:
                raise HTTPException(status_code=404, detail="Flow not found")

            # Update flow status to active
            await conn.execute("""
                UPDATE custom_flows
                SET active = TRUE,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = $1
            """, flow_id)

            # Log the resume
            await log_user_activity(
                conn=conn,
                user_info=user_info,
                action_type='resume_custom_flow',
                action_description=f'Resumed custom follow-up flow for contact {contact_id}',
                target_type='contact',
                target_id=contact_id
            )

            return {"success": True}

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error resuming custom flow: {e}")
            raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/monitoring/worker-monitoring")
async def get_worker_monitoring(request: Request):
    """Get comprehensive worker monitoring data with system metrics"""
    await verify_monitoring_token(request)

    try:
        from monitoring import monitoring_service
        if not monitoring_service:
            # Return fallback data if monitoring service not available
            return {
                'workers': [
                    {
                        'name': 'send_email_worker',
                        'status': 'unknown',
                        'last_heartbeat': datetime.now().isoformat(),
                        'error_count': 0,
                        'last_error': None,
                        'cpu_percent': 0,
                        'memory_percent': 0,
                        'memory_mb': 0,
                        'uptime': 0
                    },
                    {
                        'name': 'campaign_worker',
                        'status': 'unknown',
                        'last_heartbeat': datetime.now().isoformat(),
                        'error_count': 0,
                        'last_error': None,
                        'cpu_percent': 0,
                        'memory_percent': 0,
                        'memory_mb': 0,
                        'uptime': 0
                    },
                    {
                        'name': 'reply_checker_worker',
                        'status': 'unknown',
                        'last_heartbeat': datetime.now().isoformat(),
                        'error_count': 0,
                        'last_error': None,
                        'cpu_percent': 0,
                        'memory_percent': 0,
                        'memory_mb': 0,
                        'uptime': 0
                    }
                ],
                'system_overview': {
                    'total_cpu': 0,
                    'total_memory': 0,
                    'system_uptime': 0,
                    'active_workers': 0
                }
            }

        worker_data = await monitoring_service.get_worker_monitoring()

        # Ensure data structure is correct
        if not isinstance(worker_data, dict):
            worker_data = {'workers': [], 'system_overview': {}}

        if 'workers' not in worker_data:
            worker_data['workers'] = []

        if 'system_overview' not in worker_data:
            worker_data['system_overview'] = {
                'total_cpu': 0,
                'total_memory': 0,
                'system_uptime': 0,
                'active_workers': 0
            }

        return worker_data
    except Exception as e:
        # Return fallback data on error
        return {
            'workers': [],
            'system_overview': {
                'total_cpu': 0,
                'total_memory': 0,
                'system_uptime': 0,
                'active_workers': 0
            },
            'error': str(e)
        }

@app.get("/api/monitoring/email-dashboard")
async def get_email_dashboard(request: Request):
    """Get comprehensive email sending dashboard with real-time metrics"""
    await verify_monitoring_token(request)

    try:
        from monitoring import monitoring_service
        if not monitoring_service:
            # Return fallback data if monitoring service not available
            return {
                'queue_stats': [
                    {'status': 'sent', 'count': 0, 'avg_age_seconds': 0},
                    {'status': 'pending', 'count': 0, 'avg_age_seconds': 0},
                    {'status': 'failed', 'count': 0, 'avg_age_seconds': 0}
                ],
                'hourly_stats': [],
                'success_rate': 0,
                'failure_rate': 0,
                'real_time_metrics': {
                    'sent_today': 0,
                    'failed_today': 0,
                    'queued_now': 0,
                    'avg_delivery_time': 0
                }
            }

        email_data = await monitoring_service.get_email_dashboard()

        # Ensure data structure is correct
        if not isinstance(email_data, dict):
            email_data = {
                'queue_stats': [],
                'hourly_stats': [],
                'success_rate': 0,
                'failure_rate': 0,
                'real_time_metrics': {}
            }

        # Ensure required fields exist
        if 'real_time_metrics' not in email_data:
            email_data['real_time_metrics'] = {
                'sent_today': 0,
                'failed_today': 0,
                'queued_now': 0,
                'avg_delivery_time': 0
            }

        if 'queue_stats' not in email_data:
            email_data['queue_stats'] = []

        return email_data
    except Exception as e:
        # Return fallback data on error
        return {
            'queue_stats': [],
            'hourly_stats': [],
            'success_rate': 0,
            'failure_rate': 0,
            'real_time_metrics': {
                'sent_today': 0,
                'failed_today': 0,
                'queued_now': 0,
                'avg_delivery_time': 0
            },
            'error': str(e)
        }

@app.get("/api/monitoring/email-history")
async def get_email_history(
    request: Request,
    days: int = Query(30, description="Number of days of history to return")
):
    """Get historical email metrics and recent email records"""
    await verify_monitoring_token(request)

    try:
        async with db_pool.acquire() as conn:
            # Get historical email stats by day
            historical = await conn.fetch('''
                SELECT
                    DATE(created_at) as date,
                    COUNT(*) FILTER (WHERE status = 'sent') as sent,
                    COUNT(*) FILTER (WHERE status = 'delivered') as received,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed,
                    json_agg(
                        DISTINCT jsonb_build_object(
                            'email', sender_email,
                            'count', COUNT(*) FILTER (WHERE status = 'sent')
                        )
                        ORDER BY COUNT(*) FILTER (WHERE status = 'sent') DESC
                        LIMIT 5
                    ) as from_addresses,
                    json_agg(
                        DISTINCT jsonb_build_object(
                            'email', recipient_email,
                            'count', COUNT(*)
                        )
                        ORDER BY COUNT(*) DESC
                        LIMIT 5
                    ) as to_addresses
                FROM email_queue
                WHERE created_at >= NOW() - $1 * INTERVAL '1 day'
                GROUP BY DATE(created_at)
                ORDER BY DATE(created_at) DESC
            ''', days)

            # Get recent email records
            recent_emails = await conn.fetch('''
                SELECT
                    id::text,
                    subject,
                    sender_email as "from",
                    recipient_email as "to",
                    status,
                    created_at as timestamp,
                    error_message
                FROM email_queue
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                ORDER BY created_at DESC
                LIMIT 10000
            ''')

            return {
                "historical": [
                    {
                        "date": row["date"].isoformat(),
                        "sent": row["sent"],
                        "received": row["received"],
                        "failed": row["failed"],
                        "from_addresses": row["from_addresses"],
                        "to_addresses": row["to_addresses"]
                    }
                    for row in historical
                ],
                "recent_emails": [
                    {
                        "id": str(row["id"]),
                        "subject": row["subject"] or "(No subject)",
                        "from": row["from"],
                        "to": row["to"],
                        "status": "sent" if row["status"] == "delivered" else row["status"],
                        "timestamp": row["timestamp"].isoformat(),
                        "error_message": row["error_message"]
                    }
                    for row in recent_emails
                ]
            }

    except Exception as e:
        logger.error(f"Error getting email history: {e}")
        return {
            "historical": [],
            "recent_emails": [],
            "error": str(e)
        }

@app.get("/api/monitoring/schedule-management")
async def get_schedule_management(request: Request):
    """Get schedule management and task tracking data"""
    await verify_monitoring_token(request)

    try:
        from monitoring import monitoring_service
        if not monitoring_service:
            # Return fallback data if monitoring service not available
            return {
                'scheduled_tasks': [
                    {
                        'name': 'Email Campaign Worker',
                        'next_run': (datetime.now() + timedelta(minutes=5)).isoformat(),
                        'last_run': (datetime.now() - timedelta(minutes=55)).isoformat(),
                        'status': 'scheduled',
                        'duration_seconds': 45,
                        'success_rate': 0
                    }
                ],
                'execution_history': []
            }

        schedule_data = await monitoring_service.get_schedule_management()

        # Ensure data structure is correct
        if not isinstance(schedule_data, dict):
            schedule_data = {'scheduled_tasks': [], 'execution_history': []}

        if 'scheduled_tasks' not in schedule_data:
            schedule_data['scheduled_tasks'] = []

        if 'execution_history' not in schedule_data:
            schedule_data['execution_history'] = []

        return schedule_data
    except Exception as e:
        # Return fallback data on error
        return {
            'scheduled_tasks': [],
            'execution_history': [],
            'error': str(e)
        }

@app.get("/api/monitoring/error-tracking")
async def get_error_tracking(request: Request):
    """Get comprehensive error tracking and analytics"""
    await verify_monitoring_token(request)

    try:
        from monitoring import monitoring_service
        if not monitoring_service:
            # Return fallback data if monitoring service not available
            return {
                'error_types': [],
                'failed_emails': [],
                'analytics': {
                    'total_errors_24h': 0,
                    'rate_limit_warnings': 0,
                    'auth_failures': 0,
                    'error_trend': 'stable'
                }
            }

        error_data = await monitoring_service.get_error_tracking()

        # Ensure data structure is correct
        if not isinstance(error_data, dict):
            error_data = {
                'error_types': [],
                'failed_emails': [],
                'analytics': {}
            }

        if 'analytics' not in error_data:
            error_data['analytics'] = {
                'total_errors_24h': 0,
                'rate_limit_warnings': 0,
                'auth_failures': 0,
                'error_trend': 'stable'
            }

        if 'error_types' not in error_data:
            error_data['error_types'] = []

        if 'failed_emails' not in error_data:
            error_data['failed_emails'] = []

        return error_data
    except Exception as e:
        # Return fallback data on error
        return {
            'error_types': [],
            'failed_emails': [],
            'analytics': {
                'total_errors_24h': 0,
                'rate_limit_warnings': 0,
                'auth_failures': 0,
                'error_trend': 'stable'
            },
            'error': str(e)
        }

@app.get("/api/monitoring/campaign-analytics")
async def get_campaign_analytics(request: Request):
    """Get comprehensive campaign analytics and performance metrics"""
    await verify_monitoring_token(request)

    try:
        from monitoring import monitoring_service
        if not monitoring_service:
            raise HTTPException(status_code=500, detail="Monitoring service not available")

        campaign_data = await monitoring_service.get_campaign_analytics()
        return campaign_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching campaign analytics: {str(e)}")

# Security
SECRET_KEY = "your-secret-key-here"  # In production, use a secure secret key
security = HTTPBearer()

def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)):
    try:
        token = credentials.credentials
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token has expired"
        )
    except jwt.PyJWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token"
        )

# CORS configuration with more restrictive settings
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type", "Accept", "Origin", "X-Requested-With"],
    expose_headers=["Content-Type", "Authorization"],
    max_age=3600,
)

# Add security headers middleware with CORS headers
@app.middleware("http")
async def add_security_headers(request, call_next):
    response = await call_next(request)
    # Add CORS headers
    origin = request.headers.get("origin")
    if origin in ALLOWED_ORIGINS:
        response.headers["Access-Control-Allow-Origin"] = origin
    else:
        response.headers["Access-Control-Allow-Origin"] = ALLOWED_ORIGINS[0]
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type, Accept, Origin, X-Requested-With"
    response.headers["Access-Control-Allow-Credentials"] = "true"
    response.headers["Access-Control-Max-Age"] = "3600"

    # Other security headers
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    response.headers["Content-Security-Policy"] = "default-src 'self' https://exhibitions-conferences.info https://staymanagement.org https://conferencecare.org https://accommodationassist.org" 
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    return response


# Middleware to catch and respond to OPTIONS preflight for campaign_contacts paths
@app.middleware("http")
async def handle_options_campaign_contacts(request: Request, call_next):
    try:
        if request.method == 'OPTIONS' and (request.url.path or '').startswith('/campaign_contacts'):
            headers = request.headers
            logger.debug(f"[OPTIONS-DEBUG] path={request.url.path} origin={headers.get('origin')} acr_method={headers.get('access-control-request-method')} acr_headers={headers.get('access-control-request-headers')}")
            # Build a response that includes the required CORS headers so the browser accepts the preflight
            origin = headers.get('origin')
            resp = Response(status_code=200)
            if origin in ALLOWED_ORIGINS:
                resp.headers['Access-Control-Allow-Origin'] = origin
            else:
                resp.headers['Access-Control-Allow-Origin'] = ALLOWED_ORIGINS[0] if ALLOWED_ORIGINS else '*'
            resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, PATCH, DELETE, OPTIONS'
            # Mirror requested headers or fall back to defaults
            acrh = headers.get('access-control-request-headers')
            if acrh:
                resp.headers['Access-Control-Allow-Headers'] = acrh
            else:
                resp.headers['Access-Control-Allow-Headers'] = 'Authorization, Content-Type, Accept, Origin, X-Requested-With'
            resp.headers['Access-Control-Allow-Credentials'] = 'true'
            resp.headers['Access-Control-Max-Age'] = '3600'
            return resp
    except Exception as e:
        logger.error(f"[OPTIONS-DEBUG] error logging preflight: {e}")
    return await call_next(request)


# Middleware to catch and respond to OPTIONS preflight for organizations paths
@app.middleware("http")
async def handle_options_organizations(request: Request, call_next):
    try:
        if request.method == 'OPTIONS' and (request.url.path or '').startswith('/organizations'):
            headers = request.headers
            logger.debug(f"[OPTIONS-ORG] path={request.url.path} origin={headers.get('origin')} acr_method={headers.get('access-control-request-method')} acr_headers={headers.get('access-control-request-headers')}")
            origin = headers.get('origin')
            resp = Response(status_code=200)
            if origin in ALLOWED_ORIGINS:
                resp.headers['Access-Control-Allow-Origin'] = origin
            else:
                resp.headers['Access-Control-Allow-Origin'] = ALLOWED_ORIGINS[0] if ALLOWED_ORIGINS else '*'
            # Allow POST for organization creation, plus typical methods
            resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, PATCH, DELETE, OPTIONS'
            acrh = headers.get('access-control-request-headers')
            if acrh:
                resp.headers['Access-Control-Allow-Headers'] = acrh
            else:
                resp.headers['Access-Control-Allow-Headers'] = 'Authorization, Content-Type, Accept, Origin, X-Requested-With'
            resp.headers['Access-Control-Allow-Credentials'] = 'true'
            resp.headers['Access-Control-Max-Age'] = '3600'
            return resp
    except Exception as e:
        logger.error(f"[OPTIONS-ORG] error handling preflight: {e}")
    return await call_next(request)


# Middleware to catch and respond to OPTIONS preflight for events paths
@app.middleware("http")
async def handle_options_events(request: Request, call_next):
    try:
        if request.method == 'OPTIONS' and (request.url.path or '').startswith('/events'):
            headers = request.headers
            logger.debug(f"[OPTIONS-EVENTS] path={request.url.path} origin={headers.get('origin')} acr_method={headers.get('access-control-request-method')} acr_headers={headers.get('access-control-request-headers')}")
            origin = headers.get('origin')
            resp = Response(status_code=200)
            if origin in ALLOWED_ORIGINS:
                resp.headers['Access-Control-Allow-Origin'] = origin
            else:
                resp.headers['Access-Control-Allow-Origin'] = ALLOWED_ORIGINS[0] if ALLOWED_ORIGINS else '*'
            resp.headers['Access-Control-Allow-Methods'] = 'GET, POST, PUT, PATCH, DELETE, OPTIONS'
            acrh = headers.get('access-control-request-headers')
            if acrh:
                resp.headers['Access-Control-Allow-Headers'] = acrh
            else:
                resp.headers['Access-Control-Allow-Headers'] = 'Authorization, Content-Type, Accept, Origin, X-Requested-With'
            resp.headers['Access-Control-Allow-Credentials'] = 'true'
            resp.headers['Access-Control-Max-Age'] = '3600'
            return resp
    except Exception as e:
        logger.error(f"[OPTIONS-EVENTS] error handling preflight: {e}")
    return await call_next(request)


# Temporary debug middleware: log PUT/PATCH payloads for campaign_contacts endpoints
# Remove or disable this in production after debugging
@app.middleware("http")
async def log_campaign_contact_updates(request: Request, call_next):
    try:
        path = request.url.path or ''
        method = request.method or ''
        if path.startswith('/campaign_contacts') and method in ('PUT', 'PATCH'):
            # Read the body for logging and then replace the receive function so downstream can still read it
            body_bytes = await request.body()
            ct = request.headers.get('content-type', '')
            auth = 'Authorization' in request.headers
            try:
                body_preview = body_bytes.decode('utf-8')
            except Exception:
                body_preview = str(body_bytes[:1000])
            logger.debug(f"[DEBUG-REQUEST] {method} {path} content-type={ct} has_auth={auth} body_preview={body_preview[:2000]}")

            # Recreate the receive function so downstream handlers get the body
            async def receive():
                return {"type": "http.request", "body": body_bytes, "more_body": False}

            request._receive = receive  # type: ignore[attr-defined]

    except Exception as e:
        logger.error(f"[DEBUG-REQUEST] Failed to log request: {e}")

    response = await call_next(request)
    return response

# ---------------------------
# Helper Functions
# ---------------------------
def validate_email(email: str):
    if not re.match(r"[^@]+@[^@]+\.[^@]+", email):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid email format"
        )

def validate_date(date_str: str):
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format. Use YYYY-MM-DD"
        )

def process_emails(email_str: str, validate: bool = True) -> List[str]:
    """Process a string containing multiple email addresses.
    Args:
        email_str: String containing one or more emails separated by commas or semicolons
        validate: Whether to validate email format
    Returns:
        List of cleaned and optionally validated email addresses
    """
    if not email_str:
        return []

    # Split by comma or semicolon and clean each email
    emails = re.split(r'[,;]', email_str)
    cleaned = [normalize_email(email) for email in emails if email.strip()]

    if validate:
        valid_emails = []
        for email in cleaned:
            if re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
                valid_emails.append(email)
            else:
                logger.warning(f"Invalid email format skipped: {email}")
        return valid_emails

    return cleaned

def text_to_html(text: str) -> str:
    """
    Convert plain text to HTML format with proper line breaks and basic email-safe HTML structure.

    Args:
        text: The plain text to convert

    Returns:
        str: HTML-formatted text with line breaks converted to <br> tags
        and wrapped in a basic HTML structure for better email client compatibility.
    """
    if not text:
        return ""

    # Normalize line endings to \n
    normalized_text = text.replace('\r\n', '\n').replace('\r', '\n')

    # Split into lines and wrap each line in a paragraph
    lines = normalized_text.split('\n')

    # Convert empty lines to <br> and escape HTML in non-empty lines
    html_lines = []
    for line in lines:
        if not line.strip():
            html_lines.append('<br>')
        else:
            # Escape HTML special characters
            escaped_line = line.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            html_lines.append(escaped_line + '<br>')

    # Join lines and wrap in a basic HTML structure
    html_content = '\n'.join(html_lines)

    # Basic HTML email structure with inline styles for better compatibility
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <meta http-equiv="Content-Type" content="text/html; charset=utf-8">
        <style type="text/css">
            body, p, div {{
                font-family: Arial, sans-serif;
                font-size: 14px;
                line-height: 1.5;
                color: #333333;
                margin: 0;
                padding: 0;
            }}
            p {{
                margin: 0 0 1em 0;
                padding: 0;
            }}
        </style>
    </head>
    <body>
        <div style="max-width: 600px; margin: 0; padding: 20px;">
            {html_content}
        </div>
    </body>
    </html>
    """

    # Clean up any double <br> tags that might have been created
    return html.replace('<br><br>', '<br>').strip()

    return html

# ---------------------------
# Auth Models
# ---------------------------
class LoginRequest(BaseModel):
    username: str
    password: str

class UserCreate(BaseModel):
    username: str
    password: str
    is_admin: bool = False

class PasswordChange(BaseModel):
    password: str

class MatchRequest(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    org_name: Optional[str] = None
    current_excel_data: List[Dict[str, Any]] = []

class TaskCreate(BaseModel):
    title: str
    description: Optional[str] = None
    assigned_to: str
    due_date: Optional[str] = None

class TaskUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None
    due_date: Optional[str] = None
    customer_status: Optional[str] = None
    sender_email: Optional[str] = None
    customer_date: Optional[str] = None

    @field_validator('status')
    @classmethod
    def validate_status(cls, v):
        if v and v not in ['pending', 'in progress', 'completed']:
            raise ValueError('Status must be one of: pending, in progress, completed')
        return v

class NotificationRequest(BaseModel):
    user_id: str  # 'all' or a user id
    message: str
    type: str = 'admin'

class NotificationReplyRequest(BaseModel):
    reply_text: str

class StartCampaignRequest(BaseModel):
    sender_emails: Optional[List[str]] = None
    subject_template: str
    message_template: str
    customer_ids: List[int]

# ---------------------------
# Auth Functions
# ---------------------------
def create_token(user_id: str, username: str, is_admin: bool) -> str:
    return jwt.encode(
        {
            "user_id": str(user_id),  # Ensure user_id is a string for JSON serialization
            "username": username,
            "is_admin": is_admin,
            "exp": datetime.utcnow() + timedelta(days=7)
        },
        SECRET_KEY,
        algorithm="HS256"
    )

# ---------------------------
# Auth Endpoints
# ---------------------------
@app.post("/auth/login")
async def login(
    request: LoginRequest,
    http_request: Request,
    user_agent: Optional[str] = Header(None),
    x_forwarded_for: Optional[str] = Header(None)
):
    async with db_pool.acquire() as conn:
        user = await conn.fetchrow("SELECT * FROM users WHERE username = $1", request.username)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials"
            )
        # Verify password
        if not bcrypt.checkpw(request.password.encode('utf-8'), user['password_hash'].encode('utf-8')):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid credentials"
            )
        # Update login times
        now = datetime.now(UTC)

        # Get client IP address
        client_ip = x_forwarded_for or http_request.client.host if http_request.client else "unknown"

        # Log login activity
        await log_user_activity(
            conn,
            dict(user),
            "LOGIN",
            f"User logged in successfully from {client_ip}",
            ip_address=client_ip,
            user_agent=user_agent
        )
        now_naive = now.replace(tzinfo=None)  # Convert to timezone-naive for database
        if not user['first_login']:
            await conn.execute(
                "UPDATE users SET first_login = $1, last_login = $2 WHERE id = $3",
                now_naive, now_naive, user['id']
            )
        else:
            await conn.execute(
                "UPDATE users SET last_login = $1 WHERE id = $2",
                now_naive, user['id']
            )
        # Log activity (refactor this function async later)
        # await log_activity(user['id'], "login", "/auth/login", ip_address=x_forwarded_for, user_agent=user_agent)
        token = create_token(user['id'], user['username'], bool(user['is_admin']))
        return {
            "token": token,
            "user": {
                "id": user['id'],
                "username": user['username'],
                "is_admin": bool(user['is_admin'])
            }
        }

@app.post("/auth/users")
async def create_user(
    user: UserCreate,
    current_user: dict = Depends(get_current_user),
    user_agent: Optional[str] = Header(None),
    x_forwarded_for: Optional[str] = Header(None)
):
    if not current_user["is_admin"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can create users"
        )
    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT * FROM users WHERE username = $1", user.username)
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Username already exists"
            )
        user_id = str(uuid4())
        salt = bcrypt.gensalt()
        hashed = bcrypt.hashpw(user.password.encode('utf-8'), salt)
        await conn.execute(
            "INSERT INTO users (id, username, password_hash, is_admin) VALUES ($1, $2, $3, $4)",
            user_id, user.username, hashed.decode('utf-8'), user.is_admin
        )

        # Log user creation activity
        await log_user_activity(
            conn,
            current_user,
            "CREATE_USER",
            f"Created new user: {user.username}",
            target_type="user",
            target_id=user_id,
            target_name=user.username,
            new_values={"username": user.username, "is_admin": user.is_admin}
        )

        return {"message": "User created successfully"}

@app.get("/auth/users")
async def list_users(current_user: dict = Depends(get_current_user)):
    if not current_user["is_admin"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can list users"
        )
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, username, is_admin, created_at FROM users ORDER BY created_at DESC")
        return {"users": [dict(row) for row in rows]}

@app.delete("/auth/users/{user_id}")
async def delete_user(
    user_id: str,
    current_user: dict = Depends(get_current_user),
    user_agent: Optional[str] = Header(None),
    x_forwarded_for: Optional[str] = Header(None)
):
    if not current_user["is_admin"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can delete users"
        )
    async with db_pool.acquire() as conn:
        # Get user info before deletion for logging
        user_info = await conn.fetchrow("SELECT username FROM users WHERE id = $1", user_id)
        # Delete dependent rows first to avoid foreign key violations.
        # Wrap in a transaction so either all dependent rows and the user are removed, or nothing is changed.
        async with conn.transaction():
            # Delete activity logs referencing this user
            await conn.execute("DELETE FROM user_activity_logs WHERE user_id = $1", user_id)
            # Delete notifications owned by this user
            await conn.execute("DELETE FROM notifications WHERE user_id = $1", user_id)
            # Delete notification replies authored by this user
            await conn.execute("DELETE FROM notification_replies WHERE user_id = $1", user_id)

            # Finally delete the user
            result = await conn.execute("DELETE FROM users WHERE id = $1", user_id)

        # Log user deletion activity
        if user_info:
            await log_user_activity(
                conn,
                current_user,
                "DELETE_USER",
                f"Deleted user: {user_info['username']}",
                target_type="user",
                target_id=user_id,
                target_name=user_info['username'],
                old_values={"username": user_info['username']}
            )

        return {"message": "User deleted successfully"}

@app.put("/auth/users/{user_id}/password")
async def change_user_password(
    user_id: str,
    password_change: PasswordChange,
    current_user: dict = Depends(get_current_user),
    user_agent: Optional[str] = Header(None),
    x_forwarded_for: Optional[str] = Header(None)
):
    if not current_user["is_admin"] and current_user["user_id"] != user_id:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Not authorized to change this password"
        )
    salt = bcrypt.gensalt()
    hashed = bcrypt.hashpw(password_change.password.encode('utf-8'), salt)
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET password_hash = $1 WHERE id = $2",
            hashed.decode('utf-8'), user_id
        )

        # Log password change activity
        await log_user_activity(
            conn,
            current_user,
            "CHANGE_PASSWORD",
            f"Changed password for user ID: {user_id}",
            target_type="user",
            target_id=user_id,
            target_name=current_user.get("username", "system")
        )

        return {"message": "Password updated successfully"}

@app.get("/auth/activity-logs")
async def get_activity_logs(
    current_user: dict = Depends(get_current_user),
    user_id: Optional[str] = None,
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    action: Optional[str] = None,
    page: Optional[str] = None
):
    if not current_user["is_admin"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can view activity logs"
        )

    async with db_pool.acquire() as conn:
        query = """
            SELECT l.*, u.username
            FROM user_activity_logs l
            JOIN users u ON l.user_id = u.id
            WHERE 1=1
        """
        params = []
        param_count = 0

        if user_id:
            param_count += 1
            query += f" AND l.user_id = ${param_count}"
            params.append(user_id)
        if from_date:
            param_count += 1
            query += f" AND l.timestamp >= ${param_count}"
            params.append(from_date)
        if to_date:
            param_count += 1
            query += f" AND l.timestamp <= ${param_count}"
            params.append(to_date)
        if action:
            param_count += 1
            query += f" AND l.action_type = ${param_count}"
            params.append(action)

        query += " ORDER BY l.timestamp DESC LIMIT 100000"

        rows = await conn.fetch(query, *params)

        return {
            "logs": [
                {
                    "id": log['id'],
                    "user_id": str(log['user_id']) if log['user_id'] else None,
                    "username": log['username'],
                    "action_type": log['action_type'],
                    "action_description": log['action_description'],
                    "target_type": log['target_type'],
                    "target_id": log['target_id'],
                    "target_name": log['target_name'],
                    "old_values": log['old_values'],
                    "new_values": log['new_values'],
                    "ip_address": log['ip_address'],
                    "user_agent": log['user_agent'],
                    "timestamp": log['timestamp'].isoformat() if log['timestamp'] else None
                }
                for log in rows
            ]
        }

# ---------------------------
# Health Check
# ---------------------------

@app.get("/health")
async def health_check():
    """Simple health check endpoint"""
    try:
        async with db_pool.acquire() as conn:
            # Test database connection
            result = await conn.fetchval("SELECT 1")
            if result == 1:
                return {"status": "healthy", "database": "connected", "timestamp": datetime.now().isoformat()}
            else:
                return {"status": "unhealthy", "database": "error", "timestamp": datetime.now().isoformat()}
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {"status": "unhealthy", "database": "disconnected", "error": str(e), "timestamp": datetime.now().isoformat()}

# ---------------------------
# Dynamic Column Management
# ---------------------------

@app.get("/admin/dynamic-columns")
async def get_dynamic_columns(
    current_user: dict = Depends(verify_monitoring_token)
):
    """Get all dynamic columns for the contacts table"""
    try:
        async with db_pool.acquire() as conn:
            # First, check if dynamic_columns table exists
            table_exists = await conn.fetchval("""
                SELECT 1 FROM information_schema.tables
                WHERE table_name = 'dynamic_columns' AND table_schema = 'public'
            """)

            if not table_exists:
                logger.warning("dynamic_columns table does not exist, creating it now")
                await conn.execute('''
                    CREATE TABLE IF NOT EXISTS dynamic_columns (
                        id SERIAL PRIMARY KEY,
                        table_name TEXT NOT NULL,
                        column_name TEXT NOT NULL,
                        data_type TEXT NOT NULL,
                        is_nullable BOOLEAN DEFAULT TRUE,
                        created_by TEXT NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(table_name, column_name)
                    )
                ''')
                logger.info("dynamic_columns table created successfully")

            # Get all columns from campaign_contacts table
            columns = await conn.fetch("""
                SELECT column_name, data_type, is_nullable,
                       COALESCE(column_default, '') as column_default
                FROM information_schema.columns
                WHERE table_name = 'campaign_contacts'
                AND table_schema = 'public'
                ORDER BY ordinal_position
            """)

            # Get dynamic columns (custom columns added by admin)
            dynamic_columns = await conn.fetch("""
                SELECT column_name, data_type, is_nullable, created_at, created_by
                FROM dynamic_columns
                WHERE table_name = 'campaign_contacts'
                ORDER BY created_at DESC
            """)

            logger.info(f"Successfully fetched {len(columns)} standard columns and {len(dynamic_columns)} dynamic columns")

            return {
                "standard_columns": [dict(col) for col in columns],
                "dynamic_columns": [dict(col) for col in dynamic_columns]
            }

    except Exception as e:
        logger.error(f"Error fetching dynamic columns: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.post("/admin/dynamic-columns")
async def add_dynamic_column(
    column_data: dict = Body(...),
    current_user: dict = Depends(verify_monitoring_token)
):
    """Add a new dynamic column to the contacts table"""
    try:
        column_name = column_data.get("column_name", "").strip().lower()
        data_type = column_data.get("data_type", "TEXT").upper()
        is_nullable = column_data.get("is_nullable", True)

        # Validate column name (only letters, numbers, underscores)
        if not re.match(r'^[a-z][a-z0-9_]*$', column_name):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Column name must start with a letter and contain only lowercase letters, numbers, and underscores"
            )

        # Validate data type
        allowed_types = ['TEXT', 'VARCHAR', 'INTEGER', 'BOOLEAN', 'DATE', 'TIMESTAMP', 'NUMERIC']
        if data_type not in allowed_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Data type must be one of: {', '.join(allowed_types)}"
            )

        async with db_pool.acquire() as conn:
            # Check if column already exists
            existing = await conn.fetchval("""
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'campaign_contacts'
                AND column_name = $1
            """, column_name)

            if existing:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Column '{column_name}' already exists"
                )

            # Add the column to the table
            nullable_clause = "" if is_nullable else " NOT NULL"
            alter_sql = f"ALTER TABLE campaign_contacts ADD COLUMN {column_name} {data_type}{nullable_clause}"

            await conn.execute(alter_sql)

            # Record the dynamic column
            await conn.execute("""
                INSERT INTO dynamic_columns (table_name, column_name, data_type, is_nullable, created_by, created_at)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, 'campaign_contacts', column_name, data_type, is_nullable, current_user['username'], datetime.now())

            # Log the activity
            await log_user_activity(
                conn,
                current_user,
                "ADD_DYNAMIC_COLUMN",
                f"Added dynamic column '{column_name}' of type {data_type} to contacts table",
                target_type="table_schema",
                target_name=column_name,
                new_values={"column_name": column_name, "data_type": data_type, "is_nullable": is_nullable}
            )

            logger.info(f"[DYNAMIC COLUMN] Added column '{column_name}' to campaign_contacts table")

            return {
                "success": True,
                "message": f"Column '{column_name}' added successfully",
                "column_name": column_name,
                "data_type": data_type
            }

    except Exception as e:
        logger.error(f"Error adding dynamic column: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/admin/dynamic-columns/{column_name}")
async def delete_dynamic_column(
    column_name: str,
    current_user: dict = Depends(verify_monitoring_token)
):
    """Delete a dynamic column from the contacts table"""
    try:
        async with db_pool.acquire() as conn:
            # Check if it's a dynamic column (not a standard one)
            dynamic_col = await conn.fetchrow("""
                SELECT * FROM dynamic_columns
                WHERE table_name = 'campaign_contacts' AND column_name = $1
            """, column_name)

            if not dynamic_col:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Column '{column_name}' is not a dynamic column or doesn't exist"
                )

            # Drop the column
            await conn.execute(f"ALTER TABLE campaign_contacts DROP COLUMN {column_name}")

            # Remove from dynamic_columns table
            await conn.execute("""
                DELETE FROM dynamic_columns
                WHERE table_name = 'campaign_contacts' AND column_name = $1
            """, column_name)

            # Log the activity
            await log_user_activity(
                conn,
                current_user,
                "DELETE_DYNAMIC_COLUMN",
                f"Deleted dynamic column '{column_name}' from contacts table",
                target_type="table_schema",
                target_name=column_name,
                old_values={"column_name": column_name, "data_type": dynamic_col['data_type']}
            )

            logger.info(f"[DYNAMIC COLUMN] Deleted column '{column_name}' from campaign_contacts table")

            return {
                "success": True,
                "message": f"Column '{column_name}' deleted successfully"
            }

    except Exception as e:
        logger.error(f"Error deleting dynamic column: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# ---------------------------
# Protected Endpoints
# ---------------------------
@app.get("/admin/sender-capacities")
async def get_sender_capacities():
    try:
        async with db_pool.acquire() as conn:
            CAPACITY_PER_SENDER = 50
            
            # 1. Query: Sum of ONLY expected_contact_count
            rows = await conn.fetch(
                """
                SELECT 
                    sender_email, 
                    COALESCE(SUM(expected_contact_count), 0) as total_load
                FROM event
                WHERE sender_email = ANY($1::text[])
                GROUP BY sender_email
                """,
                ALLOWED_SENDERS
            )
            
            # 2. Map DB results
            current_loads = {row['sender_email']: row['total_load'] for row in rows}
            
            # 3. Ensure all senders exist
            for sender in ALLOWED_SENDERS:
                if sender not in current_loads:
                    current_loads[sender] = 0
            
            # 4. Calculate Active Batch
            sender_tiers = {email: load // CAPACITY_PER_SENDER for email, load in current_loads.items()}
            min_tier = min(sender_tiers.values()) if sender_tiers else 0

            # 5. Build response
            senders_data = []
            for sender in ALLOWED_SENDERS:
                load = current_loads[sender]
                tier = sender_tiers[sender]
                
                load_in_current_batch = load % CAPACITY_PER_SENDER
                capacity_remaining = CAPACITY_PER_SENDER - load_in_current_batch
                is_active_turn = (tier == min_tier)
                
                senders_data.append({
                    "sender_email": sender,
                    "total_load": load,
                    "current_batch_number": tier + 1,
                    "load_in_current_batch": load_in_current_batch,
                    "capacity_per_sender": CAPACITY_PER_SENDER,
                    "capacity_remaining": capacity_remaining,
                    "is_active_turn": is_active_turn,
                    "status": "Active" if is_active_turn else "Waiting"
                })
            
            return {
                "senders": senders_data,
                "total_senders": len(ALLOWED_SENDERS),
                "capacity_per_sender": CAPACITY_PER_SENDER,
                "current_active_batch": min_tier + 1
            }
    
    except Exception as e:
        logger.error(f"Error fetching sender capacities: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
@app.get("/customers")
async def get_all_customers(current_user: dict = Depends(get_current_user)):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM customers ORDER BY id DESC LIMIT 500")
        def hide_links(row):
            d = dict(row)
            d.pop('form_link', None)
            d.pop('payment_link', None)
            return d
        return {"customers": [hide_links(row) for row in rows]}

@app.post("/customers/matches")
async def find_matches(
    request: MatchRequest,
    current_user: dict = Depends(get_current_user)
):
    matches = []
    async with db_pool.acquire() as conn:
        if request.email:
            row = await conn.fetchrow("SELECT * FROM customers WHERE LOWER(email) = LOWER($1) LIMIT 1", request.email)
            if row:
                matches.append({
                    "type": "email",
                    "role": "Email",
                    "email": row['email'],
                    "event_name": row['event_name'],
                    "match_details": "Email match",
                    "stage": row['stage'],
                    "status": row['status']
                })
        if request.name:
            row = await conn.fetchrow("SELECT * FROM customers WHERE LOWER(name) = LOWER($1) LIMIT 1", request.name)
            if row:
                matches.append({
                    "type": "name",
                    "role": "Name",
                    "name": row['name'],
                    "event_name": row['event_name'],
                    "match_details": "Name match",
                    "stage": row['stage'],
                    "status": row['status']
                })
        if request.org_name:
            rows = await conn.fetch("SELECT COUNT(*) as count, org_name, stage, status FROM customers WHERE LOWER(org_name) = LOWER($1) GROUP BY org_name, stage, status", request.org_name)
            for row in rows:
                matches.append({
                    "type": "organization",
                    "role": "Organization",
                    "org_name": row['org_name'],
                    "event_name": "Multiple Events",
                    "match_details": f"Found in {row['count']} event(s)",
                    "stage": row['stage'],
                    "status": row['status']
                })
    return {"matches": matches}

@app.get("/api/customers/search")
async def search_customers(
    term: str = Query(None),
    columns: List[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    async with db_pool.acquire() as conn:
        if not term or not columns:
            rows = await conn.fetch("SELECT * FROM customers ORDER BY id DESC LIMIT 500")
        else:
            if 'all' in columns or not columns:
                search_terms = term.split('+')
                conditions = []
                params = []
                for search_term in search_terms:
                    search_term = search_term.strip()
                    if search_term:
                        if re.match(r'^\d{2}/\d{2}/\d{4}$', search_term):
                            conditions.append("LOWER(date) = LOWER($%d)" % (len(params)+1))
                            params.append(search_term)
                        else:
                            or_fields = [
                                "event_name", "org_name", "month", "name", "email", "source", "validation_result", "organizer", "sender_email", "stage", "status", "hotel_name", "notes", "nationality", "workplace", "payment_method", "speaker_type", "supplier"
                            ]
                            or_condition = " OR ".join([f"LOWER({col}) LIKE LOWER($%d)" % (len(params)+i+1) for i, col in enumerate(or_fields)])
                            conditions.append(f"({or_condition})")
                            params.extend([f"%{search_term}%"] * len(or_fields))
                if conditions:
                    query = f"SELECT * FROM customers WHERE {' AND '.join(conditions)} ORDER BY id DESC LIMIT 500"
                    rows = await conn.fetch(query, *params)
                else:
                    rows = await conn.fetch("SELECT * FROM customers ORDER BY id DESC LIMIT 500")
            else:
                conditions = []
                params = []
                for column in columns:
                    if column == 'payment_method':
                        column = 'payment_method'
                    if column == 'speaker_type':
                        column = 'speaker_type'
                    conditions.append(f"LOWER({column}) LIKE LOWER($%d)" % (len(params)+1))
                    params.append(f"%{term}%")
                query = f"SELECT * FROM customers WHERE {' OR '.join(conditions)} ORDER BY id DESC LIMIT 500"
                rows = await conn.fetch(query, *params)
        def hide_links(row):
            d = dict(row)
            d.pop('form_link', None)
            d.pop('payment_link', None)
            return d
        return {"customers": [hide_links(row) for row in rows]}

@app.post("/customers")
async def add_customers(
    customers: Union[List[Dict], Dict] = Body(...),
    current_user: dict = Depends(get_current_user),
    user_agent: Optional[str] = Header(None),
    x_forwarded_for: Optional[str] = Header(None)
):
    if isinstance(customers, dict):
        customers = [customers]
    added = []
    skipped = []
    async with db_pool.acquire() as conn:
        for idx, customer in enumerate(customers):
            try:
                # Insert customer (assume all columns exist in PostgreSQL schema)
                columns = list(customer.keys())
                values = [customer[k] for k in columns]
                if 'progress' not in columns:
                    columns.append('progress')
                    values.append('continue')
                col_str = ', '.join(columns)
                val_str = ', '.join([f"${i+1}" for i in range(len(values))])
                query = f"INSERT INTO customers ({col_str}) VALUES ({val_str}) RETURNING id"
                row = await conn.fetchrow(query, *values)
                added.append({"row": idx+1, "id": row['id']})
            except Exception as e:
                skipped.append({"row": idx+1, "reason": str(e)})
    return {"added": added, "skipped": skipped, "added_count": len(added), "skipped_count": len(skipped)}

@app.put("/customers/{customer_id}")
async def update_customer(
    customer_id: int,
    updates: Dict[str, Any],
    current_user: dict = Depends(get_current_user),
    user_agent: Optional[str] = Header(None),
    x_forwarded_for: Optional[str] = Header(None)
):
    update_data = dict(updates)
    # Ensure any incoming string fields are trimmed of accidental whitespace (tabs/newlines)
    if 'date' in update_data and isinstance(update_data['date'], str):
        raw_date = update_data['date'].strip()
        update_data['date'] = raw_date

        # Try to parse a variety of common formats and normalize to dd/mm/YYYY
        parsed = None
        date_formats_try = ["%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y", "%Y/%m/%d"]
        for fmt in date_formats_try:
            try:
                parsed = datetime.strptime(raw_date, fmt).date()
                break
            except Exception:
                continue

        if not parsed:
            try:
                # ISO fallback
                parsed = datetime.fromisoformat(raw_date).date()
            except Exception:
                parsed = None

        if parsed:
            # Store consistently as dd/mm/YYYY (frontend expects this format)
            update_data['date'] = parsed.strftime('%d/%m/%Y')

    # Parse sending_time (trim and try ISO)
    if 'sending_time' in update_data and isinstance(update_data['sending_time'], str):
        try:
            update_data['sending_time'] = datetime.fromisoformat(update_data['sending_time'])
        except Exception:
            update_data['sending_time'] = None

    # Assign the contact to the user performing the update
    try:
        update_data['assigned_to'] = current_user.get('user_id')
        logger.info(f"[CONTACT ASSIGN] Assigning contact {contact_id} to user {current_user.get('user_id')}")
    except Exception:
        pass
    valid_columns = [
        'id', 'event_name', 'org_name', 'month', 'name', 'email', 'source', 'validation_result', 'organizer',
        'date', 'sender_email', 'stage', 'status', 'hotel_name', 'supplier', 'notes', 'nationality',
        'payment_method', 'workplace', 'speaker_type', 'city', 'venue', 'form_link', 'payment_link',
        'progress', 'date2', 'assigned_to', 'created_by', 'customer_id', 'customer_name', 'customer_email',
        'customer_date', 'customer_stage', 'customer_status', 'customer_notes', 'customer_sender_email',
        'sending_time', 'message_id', 'conversation_id'
    ]
    valid_columns_lower = [col.lower() for col in valid_columns]
    corrected_updates = {}
    for key, value in update_data.items():
        lower_key = key.lower()
        if lower_key not in valid_columns_lower:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid column name: {key}. Valid columns are: {valid_columns}"
            )
        actual_column = next((col for col in valid_columns if col.lower() == lower_key), None)
        if actual_column:
            corrected_updates[actual_column] = value
    update_data = corrected_updates
    # Ensure the contact is assigned to the user performing the update
    try:
        update_data['assigned_to'] = current_user.get('user_id')
        logger.info(f"[CONTACT ASSIGN] Assigning contact {contact_id} to user {current_user.get('user_id')}")
    except Exception:
        pass
    update_fields = []
    update_values = []
    for i, (key, value) in enumerate(update_data.items()):
        update_fields.append(f"{key} = ${i+1}")
        update_values.append(value)
    if not update_fields:
        raise HTTPException(status_code=400, detail="No valid fields to update")
    update_query = f"UPDATE customers SET {', '.join(update_fields)} WHERE id = ${len(update_values)+1}"
    update_values.append(customer_id)
    async with db_pool.acquire() as conn:
        await conn.execute(update_query, *update_values)
    return {"message": "Customer updated successfully"}


@app.put("/contacts/{contact_id}")
async def update_contact(
    contact_id: int,
    updates: Dict[str, Any],
    current_user: dict = Depends(get_current_user),
    user_agent: Optional[str] = Header(None),
    x_forwarded_for: Optional[str] = Header(None)
):
    """Update a contact in campaign_contacts. Mirrors /customers/{id} behavior but targets campaign_contacts table."""
    update_data = dict(updates)
    # Parse sending_time
    if 'sending_time' in update_data and isinstance(update_data['sending_time'], str):
        try:
            update_data['sending_time'] = datetime.fromisoformat(update_data['sending_time'])
        except Exception:
            update_data['sending_time'] = None
    # Parse date
    if 'date' in update_data and isinstance(update_data['date'], str):
        try:
            try:
                update_data['date'] = datetime.fromisoformat(update_data['date'])
            except Exception:
                update_data['date'] = datetime.strptime(update_data['date'], '%d/%m/%Y')
        except Exception:
            update_data['date'] = None

    # Allow a set of commonly editable columns on contacts
    valid_columns = [
        'id', 'event_id', 'name', 'email', 'source', 'organizer', 'date', 'sender_email', 'stage', 'status',
        'hotel_name', 'supplier', 'notes', 'nationality', 'payment_method', 'workplace', 'speaker_type',
        'sending_time', 'progress', 'date2', 'assigned_to', 'customer_id', 'customer_name', 'customer_email',
        'customer_date', 'customer_stage', 'customer_status', 'customer_notes', 'customer_sender_email'
    ]
    valid_columns_lower = [col.lower() for col in valid_columns]
    corrected_updates = {}
    for key, value in update_data.items():
        lower_key = key.lower()
        if lower_key not in valid_columns_lower:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid column name: {key}. Valid columns are: {valid_columns}"
            )
        actual_column = next((col for col in valid_columns if col.lower() == lower_key), None)
        if actual_column:
            corrected_updates[actual_column] = value
    update_data = corrected_updates
    update_fields = []
    update_values = []
    for i, (key, value) in enumerate(update_data.items()):
        update_fields.append(f"{key} = ${i+1}")
        update_values.append(value)
    if not update_fields:
        raise HTTPException(status_code=400, detail="No valid fields to update")
    update_query = f"UPDATE campaign_contacts SET {', '.join(update_fields)} WHERE id = ${len(update_values)+1}"
    update_values.append(contact_id)
    async with db_pool.acquire() as conn:
        await conn.execute(update_query, *update_values)
    return {"message": "Contact updated successfully"}




@app.get('/api/queue/contacts')
async def get_queue_contacts(q: str = '', dedupe: bool = Query(False), current_user: dict = Depends(get_current_user)):
    """Return contacts currently in the email queue with status, stage, next action, last message and audit info.
    
    IMPORTANT: Returns EXACTLY 1000 newest contacts per request (no pagination).
    Pagination parameters (page, per_page) are NOT supported - always returns single page with max 1000 contacts.

    Fields returned:
    - contact_id, name, email, sender_email
    - current_stage, current_status
    - next_action, next_action_at
    - last_message_preview, last_message_sent_at
    - last_updated_by
    - send_failed (boolean)
    """
    if not current_user.get('is_admin'):
        # Allow any authenticated user to view their assigned contacts, admins see all
        # We'll return only contacts assigned to the user if not admin
        user_filter = f"AND c.assigned_to = '{current_user.get('user_id')}'"
    else:
        user_filter = ""

    try:
        async with db_pool.acquire() as conn:
            # Build filters for search and user assignment
            # `dedupe` controls whether we return one row per contact (True) or all pending email_queue rows (False)
            # For dedupe=true: use q (LATERAL subquery alias)
            # For dedupe=false: use eq (email_queue table alias in JOIN)
            filters = ["c.status != 'finalized'"]
            params = []
            idx = 1
            if not current_user.get('is_admin'):
                filters.append(f"c.assigned_to = ${idx}")
                params.append(current_user.get('user_id'))
                idx += 1
            if q and q.strip():
                # Search across name, email, stage, and queue_type/campaign_stage
                if dedupe:
                    filters.append(f"(LOWER(c.name) ILIKE ${idx} OR LOWER(c.email) ILIKE ${idx} OR LOWER(c.stage) ILIKE ${idx} OR LOWER(q.campaign_stage) ILIKE ${idx} OR LOWER(q.queue_type) ILIKE ${idx})")
                else:
                    filters.append(f"(LOWER(c.name) ILIKE ${idx} OR LOWER(c.email) ILIKE ${idx} OR LOWER(c.stage) ILIKE ${idx} OR LOWER(eq.campaign_stage) ILIKE ${idx} OR LOWER(eq.type) ILIKE ${idx})")
                params.append(f"%{q.strip().lower()}%")
                idx += 1

            where_clause = ' AND '.join(filters)

            # Total count
            if dedupe:
                # use LATERAL to pick one pending queue entry per contact
                count_query = f"""
                    SELECT COUNT(*)
                    FROM campaign_contacts c
                    LEFT JOIN LATERAL (
                        SELECT contact_id, type as queue_type, campaign_stage, due_at, created_at
                        FROM email_queue eq
                        WHERE eq.contact_id = c.id AND eq.status = 'pending'
                        ORDER BY eq.created_at DESC
                        LIMIT 1
                    ) q ON TRUE
                    WHERE {where_clause}
                """
                total = await conn.fetchval(count_query, *params) if params else await conn.fetchval(count_query)
            else:
                # count all email_queue rows regardless of status (not joined to contacts, to include orphaned items)
                # But if user is non-admin, filter by assigned_to
                if not current_user.get('is_admin'):
                    count_query = f"""
                        SELECT COUNT(*)
                        FROM email_queue eq
                        LEFT JOIN campaign_contacts c ON c.id = eq.contact_id
                        {('WHERE ' + where_clause) if where_clause else ''}
                    """
                    total = await conn.fetchval(count_query, *params) if params else await conn.fetchval(count_query)
                else:
                    count_query = f"""
                        SELECT COUNT(*)
                        FROM email_queue
                    """
                    total = await conn.fetchval(count_query)
                

            # Aggregated counts by category (initial, forms, payments, reminders)
            if dedupe:
                counts_query = f"""
                    SELECT
                        COALESCE(SUM(CASE WHEN LOWER(COALESCE(q.campaign_stage, '')) LIKE 'initial%' THEN 1 ELSE 0 END),0) AS initial_count,
                        COALESCE(SUM(CASE WHEN LOWER(COALESCE(q.campaign_stage, '')) LIKE 'forms%' THEN 1 ELSE 0 END),0) AS forms_count,
                        COALESCE(SUM(CASE WHEN LOWER(COALESCE(q.campaign_stage, '')) LIKE 'payments%' THEN 1 ELSE 0 END),0) AS payments_count,
                        COALESCE(SUM(CASE WHEN COALESCE(q.campaign_stage,'') ILIKE '%reminder%' OR COALESCE(q.queue_type,'') ILIKE '%reminder%' THEN 1 ELSE 0 END),0) AS reminders_count
                    FROM campaign_contacts c
                    LEFT JOIN LATERAL (
                        SELECT contact_id, type as queue_type, campaign_stage, due_at, created_at
                        FROM email_queue eq
                        WHERE eq.contact_id = c.id AND eq.status = 'pending'
                        ORDER BY eq.created_at DESC
                        LIMIT 1
                    ) q ON TRUE
                    WHERE {where_clause}
                """
                counts_row = await conn.fetchrow(counts_query, *params) if params else await conn.fetchrow(counts_query)
            else:
                # For dedupe=false: count all emails, but filter by assigned_to if non-admin
                if not current_user.get('is_admin'):
                    counts_query = f"""
                        SELECT
                            COALESCE(SUM(CASE WHEN LOWER(COALESCE(eq.campaign_stage, '')) LIKE 'initial%' THEN 1 ELSE 0 END),0) AS initial_count,
                            COALESCE(SUM(CASE WHEN LOWER(COALESCE(eq.campaign_stage, '')) LIKE 'forms%' THEN 1 ELSE 0 END),0) AS forms_count,
                            COALESCE(SUM(CASE WHEN LOWER(COALESCE(eq.campaign_stage, '')) LIKE 'payments%' THEN 1 ELSE 0 END),0) AS payments_count,
                            COALESCE(SUM(CASE WHEN COALESCE(eq.campaign_stage,'') ILIKE '%reminder%' OR COALESCE(eq.type,'') ILIKE '%reminder%' THEN 1 ELSE 0 END),0) AS reminders_count
                        FROM email_queue eq
                        LEFT JOIN campaign_contacts c ON c.id = eq.contact_id
                        WHERE c.assigned_to = $1
                    """
                else:
                    counts_query = f"""
                        SELECT
                            COALESCE(SUM(CASE WHEN LOWER(COALESCE(eq.campaign_stage, '')) LIKE 'initial%' THEN 1 ELSE 0 END),0) AS initial_count,
                            COALESCE(SUM(CASE WHEN LOWER(COALESCE(eq.campaign_stage, '')) LIKE 'forms%' THEN 1 ELSE 0 END),0) AS forms_count,
                            COALESCE(SUM(CASE WHEN LOWER(COALESCE(eq.campaign_stage, '')) LIKE 'payments%' THEN 1 ELSE 0 END),0) AS payments_count,
                            COALESCE(SUM(CASE WHEN COALESCE(eq.campaign_stage,'') ILIKE '%reminder%' OR COALESCE(eq.type,'') ILIKE '%reminder%' THEN 1 ELSE 0 END),0) AS reminders_count
                        FROM email_queue eq
                        LEFT JOIN campaign_contacts c ON c.id = eq.contact_id
                        {('WHERE ' + where_clause) if where_clause else ''}
                    """
                counts_row = await conn.fetchrow(counts_query, *params) if params else await conn.fetchrow(counts_query)
                
            counts = {
                'initial': counts_row['initial_count'] if counts_row else 0,
                'forms': counts_row['forms_count'] if counts_row else 0,
                'payments': counts_row['payments_count'] if counts_row else 0,
                'reminders': counts_row['reminders_count'] if counts_row else 0,
            }

            # Get newest 1000 contacts (no pagination - always return top 1000)
            if dedupe:
                data_query = f"""
                SELECT
                    c.id as contact_id,
                    c.event_id,
                    c.name,
                    c.email,
                    c.stage as current_stage,
                    c.status as current_status,
                    q.campaign_stage as next_action,
                    q.due_at as next_action_at,
                    q.created_at as queue_created_at,
                    q.queue_type as queue_type,
                    c.created_at as contact_created_at,
                    m.body as last_message_body,
                    m.sent_at as last_message_sent_at,
                    ul.username as last_updated_by,
                    c.email_bounced as send_failed,
                    ev.sender_email
                FROM campaign_contacts c
                LEFT JOIN event ev ON c.event_id = ev.id
                LEFT JOIN LATERAL (
                    SELECT contact_id, type as queue_type, campaign_stage, due_at, created_at
                    FROM email_queue eq
                    WHERE eq.contact_id = c.id AND eq.status = 'pending'
                    ORDER BY eq.created_at DESC
                    LIMIT 1
                ) q ON TRUE
                LEFT JOIN LATERAL (
                    SELECT body, sent_at
                    FROM messages
                    WHERE messages.contact_id = c.id
                    ORDER BY sent_at DESC
                    LIMIT 1
                ) m ON TRUE
                LEFT JOIN LATERAL (
                    SELECT ua.username
                    FROM user_activity_logs ua
                    WHERE ua.target_type = 'campaign_contact' AND ua.target_id = c.id
                    ORDER BY ua.timestamp DESC
                    LIMIT 1
                ) ul ON TRUE
                WHERE {where_clause}
                    ORDER BY q.created_at DESC NULLS LAST
                    LIMIT 1000
                """
            else:
                # For dedupe=false: Show ALL emails from email_queue table (any status)
                # For non-admin users: filter by assigned_to contact
                # Join to campaign_contacts only for contact info (LEFT JOIN to allow orphaned queue items)
                if not current_user.get('is_admin'):
                    data_query = f"""
                        SELECT
                            eq.id as queue_id,
                            eq.contact_id,
                            c.event_id,
                            COALESCE(c.name, 'Unknown Contact') as name,
                            COALESCE(c.email, eq.recipient_email) as email,
                            eq.type as current_stage,
                            eq.status as current_status,
                            eq.campaign_stage as next_action,
                            eq.due_at as next_action_at,
                            eq.created_at as queue_created_at,
                            eq.type as queue_type,
                            COALESCE(c.created_at, eq.created_at) as contact_created_at,
                            eq.subject as last_message_body,
                            eq.sent_at as last_message_sent_at,
                            ul.username as last_updated_by,
                            (eq.status = 'failed') as send_failed,
                            eq.status as queue_status,
                            ev.sender_email
                        FROM email_queue eq
                        LEFT JOIN campaign_contacts c ON c.id = eq.contact_id
                        LEFT JOIN event ev ON c.event_id = ev.id
                        LEFT JOIN LATERAL (
                            SELECT ua.username
                            FROM user_activity_logs ua
                            WHERE ua.target_type = 'campaign_contact' AND ua.target_id = c.id
                            ORDER BY ua.timestamp DESC
                            LIMIT 1
                        ) ul ON TRUE
                        WHERE c.assigned_to = $1
                        ORDER BY eq.created_at DESC
                        LIMIT 1000
                    """
                else:
                    data_query = f"""
                        SELECT
                            eq.id as queue_id,
                            eq.contact_id,
                            c.event_id,
                            COALESCE(c.name, 'Unknown Contact') as name,
                            COALESCE(c.email, eq.recipient_email) as email,
                            eq.type as current_stage,
                            eq.status as current_status,
                            eq.campaign_stage as next_action,
                            eq.due_at as next_action_at,
                            eq.created_at as queue_created_at,
                            eq.type as queue_type,
                            COALESCE(c.created_at, eq.created_at) as contact_created_at,
                            eq.subject as last_message_body,
                            eq.sent_at as last_message_sent_at,
                            ul.username as last_updated_by,
                            (eq.status = 'failed') as send_failed,
                            eq.status as queue_status,
                            ev.sender_email
                        FROM email_queue eq
                        LEFT JOIN campaign_contacts c ON c.id = eq.contact_id
                        LEFT JOIN event ev ON c.event_id = ev.id
                        LEFT JOIN LATERAL (
                            SELECT ua.username
                            FROM user_activity_logs ua
                            WHERE ua.target_type = 'campaign_contact' AND ua.target_id = c.id
                            ORDER BY ua.timestamp DESC
                            LIMIT 1
                        ) ul ON TRUE
                        {('WHERE ' + where_clause) if where_clause else ''}
                        ORDER BY eq.created_at DESC
                        LIMIT 1000
                    """

            if params:
                rows = await conn.fetch(data_query, *params)
            else:
                rows = await conn.fetch(data_query) 

            result = []
            for r in rows:
                preview = None
                if r['last_message_body']:
                    preview = (r['last_message_body'][:200] + '...') if len(r['last_message_body']) > 200 else r['last_message_body']

                result.append({
                    'contact_id': r['contact_id'],
                    'event_id': r['event_id'],
                    'name': r['name'],
                    'email': r['email'],
                    'current_stage': r['current_stage'],
                    'current_status': r['current_status'],
                    'next_action': r['next_action'] or r['queue_type'],
                    'next_action_at': r['next_action_at'].isoformat() if r['next_action_at'] else None,
                    'queue_created_at': r['queue_created_at'].isoformat() if r['queue_created_at'] else None,
                    'contact_created_at': r['contact_created_at'].isoformat() if r['contact_created_at'] else None,
                    'last_message_preview': preview,
                    'last_message_sent_at': r['last_message_sent_at'].isoformat() if r['last_message_sent_at'] else None,
                    'last_updated_by': r['last_updated_by'],
                    'send_failed': bool(r['send_failed']),
                    'sender_email': r['sender_email']
                })

            return { 'contacts': result, 'total': total, 'page': 1, 'per_page': 1000, 'counts': counts }
    except Exception as e:
        logger.exception("Failed to fetch queue contacts")
        raise HTTPException(status_code=500, detail=f"Failed to fetch queue contacts: {e}")
# Notification helper functions
async def create_notification(
    user_id: str,
    notification_type: str,
    message: str,
    task_id: Optional[str] = None,
    customer_id: Optional[int] = None
):
    async with db_pool.acquire() as conn:
        notification_id = str(uuid4())
        await conn.execute("""
            INSERT INTO notifications (id, user_id, task_id, customer_id, type, message)
            VALUES ($1, $2, $3, $4, $5, $6)
        """, notification_id, user_id, task_id, customer_id, notification_type, message)

async def send_task_notification(task_id: str, assigned_to: str, notification_type: str):
    async with db_pool.acquire() as conn:
        # Get task details
        task = await conn.fetchrow("""
            SELECT t.title, t.due_date, u.username
            FROM tasks t
            JOIN users u ON t.created_by = u.id
            WHERE t.id = $1
        """, task_id)

        if task:
            title, due_date, creator = task['title'], task['due_date'], task['username']
            message = ""
            if notification_type == "assigned":
                message = f"New task assigned: {title} (Due: {due_date})"
            elif notification_type == "due_soon":
                message = f"Task due soon: {title} (Due: {due_date})"
            elif notification_type == "completed":
                message = f"Task completed: {title}"

            await create_notification(
                user_id=assigned_to,
                notification_type=notification_type,
                message=message,
                task_id=task_id
            )

async def check_due_tasks():
    async with db_pool.acquire() as conn:
        # Get tasks due in the next 24 hours
        rows = await conn.fetch("""
            SELECT id, assigned_to, title, due_date
            FROM tasks
            WHERE status != 'completed'
            AND due_date BETWEEN date('now') AND date('now', '+1 day')
        """)

        for row in rows:
            await send_task_notification(row['id'], row['assigned_to'], "due_soon")

async def check_and_create_client_tasks():
    # Try to obtain the pool via the helper which prefers app.state but
    # falls back to the module-level reference. If the pool isn't yet
    # initialized because the app is still starting, wait briefly to
    # reduce races during startup.
    pool = get_db_pool()
    if not pool:
        pool = await get_db_pool_async(timeout_seconds=3.0)

    if not pool:
        logger.error("[CLIENT TASKS] Database pool not available")
        return

    async with pool.acquire() as conn:
        # Diagnostic counts to understand why no candidates are found
        try:
            total_contacts = await conn.fetchval("SELECT COUNT(*) FROM campaign_contacts")
            contacts_with_date = await conn.fetchval("SELECT COUNT(*) FROM campaign_contacts WHERE date IS NOT NULL AND date <> ''")
            contacts_with_assigned = await conn.fetchval("SELECT COUNT(*) FROM campaign_contacts WHERE assigned_to IS NOT NULL")
            contacts_with_date_and_assigned = await conn.fetchval("SELECT COUNT(*) FROM campaign_contacts WHERE date IS NOT NULL AND date <> '' AND assigned_to IS NOT NULL")
            logger.info(f"[CLIENT TASKS] campaign_contacts counts total={total_contacts} with_date={contacts_with_date} with_assigned={contacts_with_assigned} with_date_and_assigned={contacts_with_date_and_assigned}")
        except Exception as e:
            logger.error(f"[CLIENT TASKS] Failed to fetch diagnostic counts: {e}")

        # Optional targeted debug for a contact you mentioned (1166)
        try:
            debug_contact = await conn.fetchrow("SELECT id, assigned_to, date, status FROM campaign_contacts WHERE id = $1", 1166)
            logger.info(f"[CLIENT TASKS] debug contact 1166: {dict(debug_contact) if debug_contact else 'NOT FOUND'}")
        except Exception as e:
            logger.error(f"[CLIENT TASKS] debug fetch error for id=1166: {e}")

        # Fetch candidate contacts (we'll parse dates in Python to support multiple formats)
        rows = await conn.fetch("""
            SELECT c.id, c.name, c.email, c.organizer, c.assigned_to, c.date, c.status
            FROM campaign_contacts c
            WHERE c.status != 'completed' AND c.assigned_to IS NOT NULL
        """)

        total_candidates = len(rows)
        logger.info(f"[CLIENT TASKS] check_and_create_client_tasks: fetched {total_candidates} candidate contacts")

        today_dt = datetime.now(UTC).date()
        date_formats = [
            "%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y", "%m-%d-%Y", "%Y/%m/%d"
        ]

        for row in rows:
            customer_id = row['id']
            name = row.get('name')
            email = row.get('email')
            organizer = row.get('organizer')
            assigned_to = row.get('assigned_to')
            customer_date = row.get('date')
            status = row.get('status')

            logger.debug(f"[CLIENT TASKS] Candidate contact id={customer_id} assigned_to={assigned_to} raw_date={customer_date} status={status}")

            # Trim whitespace from stored date strings (handles trailing tabs/newlines)
            if isinstance(customer_date, str):
                customer_date = customer_date.strip()

            if not customer_date:
                logger.debug(f"[CLIENT TASKS] Skipping id={customer_id} because date is empty")
                continue

            parsed_date = None
            # Try multiple formats
            for fmt in date_formats:
                try:
                    parsed_date = datetime.strptime(customer_date, fmt).date()
                    logger.debug(f"[CLIENT TASKS] Parsed date for id={customer_id} using fmt={fmt}: {parsed_date}")
                    break
                except Exception:
                    continue

            # Try ISO fallback
            if not parsed_date:
                try:
                    parsed_date = datetime.fromisoformat(customer_date).date()
                    logger.debug(f"[CLIENT TASKS] Parsed ISO date for id={customer_id}: {parsed_date}")
                except Exception:
                    parsed_date = None

            if not parsed_date:
                logger.info(f"[CLIENT TASKS] Could not parse date for contact id={customer_id} raw_date={customer_date}")
                continue

            if parsed_date != today_dt:
                logger.debug(f"[CLIENT TASKS] Contact id={customer_id} parsed_date={parsed_date} does not match today={today_dt}")
                continue

            # Check if task already exists for today (Postgres CURRENT_DATE)
            existing = await conn.fetchrow("""
                SELECT id FROM client_tasks
                WHERE customer_id = $1 AND DATE(created_at) = CURRENT_DATE
            """, customer_id)

            if existing:
                logger.debug(f"[CLIENT TASKS] Task already exists for contact id={customer_id}, task_id={existing['id']}")
                continue

            # If contact has no assigned user, fall back to the first admin user so the task gets assigned
            if not assigned_to:
                try:
                    fallback_user = await conn.fetchrow("SELECT id, username FROM users WHERE is_admin = TRUE LIMIT 1")
                    if fallback_user:
                        fallback_id = fallback_user['id']
                        logger.info(f"[CLIENT TASKS] contact id={customer_id} has no assigned_to, falling back to admin user {fallback_user.get('username')} ({fallback_id})")
                        assigned_to = fallback_id
                    else:
                        logger.warning(f"[CLIENT TASKS] No admin user found to assign task for contact id={customer_id}; skipping")
                        continue
                except Exception as e:
                    logger.error(f"[CLIENT TASKS] Error fetching fallback admin user: {e}")
                    continue

            # Create new client task
            task_id = str(uuid4())
            await conn.execute("""
                INSERT INTO client_tasks (id, customer_id, assigned_to, status)
                VALUES ($1, $2, $3, 'pending')
            """, task_id, customer_id, assigned_to)
            logger.info(f"[CLIENT TASKS] Created client task {task_id} for contact id={customer_id} assigned_to={assigned_to}")

@app.get("/notifications")
async def get_notifications(
    current_user: dict = Depends(get_current_user),
    unread_only: bool = False
):
    async with db_pool.acquire() as conn:
        query = """
            SELECT n.*,
                   t.title as task_title,
                   c.name as customer_name
            FROM notifications n
            LEFT JOIN tasks t ON n.task_id = t.id
            LEFT JOIN campaign_contacts c ON n.customer_id = c.id
            WHERE n.user_id = $1
        """
        params = [current_user["user_id"]]

        if unread_only:
            query += " AND n.is_read = FALSE"

        query += " ORDER BY n.created_at DESC"

        rows = await conn.fetch(query, *params)
        notifications = []
        for row in rows:
            notifications.append({
                "id": row['id'],
                "user_id": row['user_id'],
                "task_id": row['task_id'],
                "customer_id": row['customer_id'],
                "type": row['type'],
                "message": row['message'],
                "is_read": bool(row['is_read']),
                "created_at": row['created_at'],
                "task_title": row['task_title'],
                "customer_name": row['customer_name']
            })

        return {"notifications": notifications}

@app.put("/notifications/{notification_id}/read")
async def mark_notification_read(
    notification_id: str,
    current_user: dict = Depends(get_current_user)
):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE notifications
            SET is_read = TRUE
            WHERE id = $1 AND user_id = $2
        """, notification_id, current_user["user_id"])
        return {"message": "Notification marked as read"}

@app.post("/notifications")
async def send_notification(
    req: NotificationRequest,
    current_user: dict = Depends(get_current_user)
):
    if current_user.get("username") != "admin":
        raise HTTPException(status_code=403, detail="Only the user named 'admin' can send notifications.")

    async with db_pool.acquire() as conn:
        if req.user_id == 'all':
            rows = await conn.fetch("SELECT id FROM users")
            user_ids = [row['id'] for row in rows]
        else:
            user_ids = [req.user_id]

        for user_id in user_ids:
            await create_notification(
                user_id=user_id,
                notification_type=req.type,
                message=req.message
            )
        return {"status": "Notifications sent", "user_ids": user_ids}

@app.post("/notifications/{notification_id}/reply")
async def reply_to_notification(
    notification_id: str = Path(...),
    req: NotificationReplyRequest = None,
    current_user: dict = Depends(get_current_user)
):
    async with db_pool.acquire() as conn:
        # Check if notification exists and belongs to user
        notification = await conn.fetchrow("""
            SELECT * FROM notifications WHERE id = $1 AND user_id = $2
        """, notification_id, current_user["user_id"])

        if not notification:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Notification not found"
            )

        reply_id = str(uuid4())
        await conn.execute("""
            INSERT INTO notification_replies (id, notification_id, user_id, reply_text)
            VALUES ($1, $2, $3, $4)
        """, reply_id, notification_id, current_user["user_id"], req.reply_text)

        return {"message": "Reply added successfully"}

@app.get("/notifications/replies")
async def get_all_notification_replies(current_user: dict = Depends(get_current_user)):
    if not current_user["is_admin"]:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can view all replies"
        )
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT nr.*, n.message as notification_message, u.username
            FROM notification_replies nr
            JOIN notifications n ON nr.notification_id = n.id
            JOIN users u ON nr.user_id = u.id
            ORDER BY nr.created_at DESC
        """)

        replies = []
        for row in rows:
            replies.append({
                "id": row['id'],
                "notification_id": row['notification_id'],
                "user_id": row['user_id'],
                "notification_message": row['notification_message'],
                "username": row['username'],
                "reply_text": row['reply_text'],
                "created_at": row['created_at']
            })
        return {"replies": replies}

# --- Refactored Email Sending Worker ---
def to_plain_text(html_or_text: str) -> str:
    """
    Convert HTML (or plain text) into normalized plain-text suitable for sending.
    - Converts <br> and </p> to newlines
    - Removes script/style blocks and other tags
    - Unescapes HTML entities
    - Collapses multiple blank lines into a single blank line (i.e. max one empty line)
    - Trims trailing/leading whitespace on lines and normalizes internal spaces
    - Returns text using CRLF (\r\n) line endings which mail clients expect
    """
    try:
        if not html_or_text:
            return ''
        s = str(html_or_text)
        import re as _re
        # Remove script and style contents
        s = _re.sub(r'(?is)<(script|style).*?>.*?</\1>', '', s)
        # Replace <br> and variants with newlines
        s = _re.sub(r'(?i)<br\s*/?>', '\n', s)
        # Replace closing paragraph tags with double newline
        s = _re.sub(r'(?i)</p\s*>', '\n\n', s)
        # Remove all remaining tags
        s = _re.sub(r'<[^>]+>', '', s)
        # Unescape HTML entities
        s = html.unescape(s)
        # Normalize line endings to LF
        s = s.replace('\r\n', '\n').replace('\r', '\n')
        # Collapse multiple blank lines to one
        s = _re.sub(r'\n\s*\n+', '\n\n', s)
        # Trim spaces on each line and collapse multiple spaces
        lines = [ _re.sub(r' +', ' ', ln.strip()) for ln in s.split('\n') ]
        s = '\n'.join(lines).strip()
        # Convert to CRLF for email bodies
        s = s.replace('\n', '\r\n')
        return s
    except Exception as e:
        logger.error(f"to_plain_text failed: {e}")
        try:
            return str(html_or_text)
        except Exception:
            return ''

# --- Refactored Email Sending Worker ---
async def prepare_email_body(conn, contact_id: int, new_body: str) -> str:
    """Prepare email body with conversation history."""
    contact = await conn.fetchrow("""
        SELECT c.*, 
               cr.body as last_reply_body,
               cr.received_at as last_reply_at,
               c.last_sent_body,
               c.last_sent_at
        FROM campaign_contacts c
        LEFT JOIN LATERAL (
            SELECT body, received_at
            FROM campaign_contact_replies
            WHERE contact_id = c.id
            ORDER BY received_at DESC
            LIMIT 1
        ) cr ON true
        WHERE c.id = $1
    """, contact_id)
    
    if not contact:
        return new_body
        
    # Format as dict for build_outgoing_body
    contact_dict = {
        'id': contact['id'],
        'name': contact['name'],
        'email': contact['email'],
        'last_reply_body': contact['last_reply_body'],
        'last_reply_at': contact['last_reply_at'],
        'last_sent_body': contact['last_sent_body'],
        'last_sent_at': contact['last_sent_at']
    }
    
    return await build_outgoing_body(contact_dict, new_body)

async def send_email_worker():
    """Background worker that processes the email queue and sends emails."""
    SAFE_TIMEOUT = 300  # 5 minutes max for pending messages
    
    async def prepare_message_for_sending(conn, queue_item):
        """Prepare email message with conversation history"""
        logger.debug(f"[HISTORY] Preparing message for contact_id={queue_item['contact_id']}")
        
        # Get the contact's details and conversation history
        # Fetch latest reply body: prefer campaign_contact_replies, fallback to messages.received entries
        contact = await conn.fetchrow("""
            SELECT c.*, 
                   COALESCE(cr.body, m.body, c.last_reply_body) as reply_body,
                   COALESCE(cr.received_at, m.received_at, c.last_reply_at) as reply_at,
                   COALESCE(cr.message_id, m.message_id) as reply_message_id
            FROM campaign_contacts c
            LEFT JOIN LATERAL (
                SELECT body, received_at, message_id
                FROM campaign_contact_replies 
                WHERE contact_id = c.id 
                ORDER BY received_at DESC 
                LIMIT 1
            ) cr ON true
            LEFT JOIN LATERAL (
                SELECT body, received_at, message_id
                FROM messages
                WHERE contact_id = c.id AND direction = 'received'
                ORDER BY received_at DESC
                LIMIT 1
            ) m ON true
            WHERE c.id = $1
        """, queue_item['contact_id'])
        
        # Compute ages safely: handle naive and aware datetimes
        def safe_age(ts):
            try:
                if not ts:
                    return 'N/A'
                now = datetime.now(timezone.utc)
                if getattr(ts, 'tzinfo', None) is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                return now - ts
            except Exception:
                return 'N/A'

        logger.debug(f"[HISTORY] Found contact: name={contact['name'] if contact else 'None'}, " +
                    f"has_reply={'Yes' if contact and contact['reply_body'] else 'No'}, " +
                    f"reply_age={safe_age(contact['reply_at']) if contact else 'N/A'}, " +
                    f"last_sent_age={safe_age(contact['last_sent_at']) if contact else 'N/A'}")
        
        if not contact:
            logger.debug("[HISTORY] No contact found, returning original message")
            return queue_item['message']
            
        message_body = queue_item['message']
        logger.debug(f"[HISTORY] Original message length: {len(message_body)} chars")

        # By default we will include the last sent quote unless a reply exists.
        skip_sent_quote = False

        # Add quoted reply if available
        if contact['reply_body'] and contact['reply_at']:
            logger.debug(f"[HISTORY] Adding reply quote from {contact['email']}, " +
                         f"reply length: {len(contact['reply_body'])} chars, " +
                         f"reply message_id: {contact.get('reply_message_id', 'N/A')}")

            reply_time = contact['reply_at'].strftime("%a, %b %-d, %Y at %-I:%M %p")
            quote_header = f"\n\nOn {reply_time} {contact['name']} <{contact['email']}> wrote:\n"
            quote_text = "\n".join(contact['reply_body'].splitlines())
            message_body = f"{message_body}\n{quote_header}{quote_text}"

            # When a contact has replied, do not append the previous 'last sent' quote.
            skip_sent_quote = True
            logger.debug(f"[HISTORY] Added reply quote, new message length: {len(message_body)} chars; will skip last_sent quote")

        if not skip_sent_quote and contact['last_sent_body'] and contact['last_sent_at']:
            logger.debug(f"[HISTORY] No reply found, adding last sent message quote, " +
                         f"sent message length: {len(contact['last_sent_body'])} chars, " +
                         f"sent at: {contact['last_sent_at']}")

            sent_time = contact['last_sent_at'].strftime("%a, %b %-d, %Y at %-I:%M %p")
            quote_header = f"\n\nOn {sent_time} {queue_item['sender_email']} wrote:\n"
            quote_text = "\n".join(contact['last_sent_body'].splitlines())
            message_body = f"{message_body}\n{quote_header}{quote_text}"

            logger.debug(f"[HISTORY] Added sent message quote, new message length: {len(message_body)} chars")

        logger.debug(f"[HISTORY] Final message summary for contact {contact['id']}: " +
                     f"total_length={len(message_body)}, " +
                     f"has_reply_quote={'Yes' if contact['reply_body'] else 'No'}, " +
                     f"has_sent_quote={'Yes' if (contact['last_sent_body'] and not skip_sent_quote) else 'No'}, " +
            f"stage={queue_item.get('campaign_stage', 'N/A')}, " +
            f"message_type={queue_item.get('type', 'N/A')}")
            
        return message_body
    
    async def mark_stuck_messages():
        """Mark long-pending messages as failed"""
        async with asyncpg.create_pool(POSTGRES_DSN) as pool:
            async with pool.acquire() as conn:
                # Find messages stuck in pending state
                stuck_messages = await conn.fetch("""
                    UPDATE email_queue
                    SET status = 'failed',
                        error_message = 'Message stuck in pending state too long'
                    WHERE status = 'pending'
                    AND created_at < NOW() - interval '5 minutes'
                    RETURNING id
                """)
                for msg in stuck_messages:
                    logger.warning(f"Marked stuck pending message {msg['id']} as failed after {SAFE_TIMEOUT} seconds (safe limit {SAFE_TIMEOUT}s)")

    # Worker main loop (keep inside send_email_worker)
    global db_pool
    worker_pool = db_pool
    ADVISORY_LOCK_KEY = 90002  # Unique key for send_email_worker

    while True:
        lock_acquired = False
        lock_conn = None
        try:
            # Acquire advisory lock to prevent multiple instances
            try:
                lock_conn = await worker_pool.acquire()
                lock_acquired = await lock_conn.fetchval(f"SELECT pg_try_advisory_lock({ADVISORY_LOCK_KEY})")
                
                if not lock_acquired:
                    logger.debug("[SEND EMAIL WORKER] Skipped due to active lock (another instance running)")
                    await asyncio.sleep(5)
                    continue
                    
                logger.debug(f"[SEND EMAIL WORKER] Acquired advisory lock {ADVISORY_LOCK_KEY}")
                
            except Exception as e:
                logger.error(f"[SEND EMAIL WORKER] Error acquiring lock: {e}")
                await asyncio.sleep(5)
                continue

            try:
                await update_worker_heartbeat('send_email_worker', 'running')
                # Fetch pending rows using a short-lived connection so we don't hold
                # a connection for the entire processing loop. Each email will be
                # processed within its own acquired connection and transaction below.
                async with worker_pool.acquire() as fetch_conn:
                    # Order queued items by business priority rather than pure FIFO.
                    # Priority groups (lower number = higher priority):
                    #  1 = payments (initial + all payment reminders)
                    #  2 = forms (initial + all form reminders)
                    #  3 = first message and its reminders (campaign_main, reminder1, reminder2)
                    #  4 = everything else
                    rows = await fetch_conn.fetch("""
                        SELECT * FROM email_queue
                        WHERE status = 'pending'
                        AND (scheduled_at IS NULL OR scheduled_at <= NOW())
                        ORDER BY
                            -- Priority Tier 1: Initial messages (forms_initial, payments_initial, etc.)
                            CASE
                                WHEN last_message_type IN (
                                    'forms_initial', 'forms_main', 
                                    'payments_initial', 'payment_main', 
                                    'sepa_initial', 'rh_initial'
                                ) THEN 0
                                -- Priority Tier 2: Forms reminders (after forms_initial sent)
                                WHEN last_message_type LIKE 'forms_reminder%' THEN 1
                                -- Priority Tier 3: Payments reminders (after payments_initial sent)
                                WHEN last_message_type LIKE 'payments_reminder%' THEN 2
                                -- Priority Tier 4: SEPA reminders
                                WHEN last_message_type LIKE 'sepa_reminder%' THEN 3
                                -- Priority Tier 5: RH reminders
                                WHEN last_message_type LIKE 'rh_reminder%' THEN 4
                                -- Priority Tier 6: Campaign/reminder messages (legacy)
                                WHEN last_message_type IN ('campaign_main', 'reminder1', 'reminder2') THEN 5
                                -- Priority Tier 7: Everything else
                                ELSE 6

                            END ASC,
                            -- Within each priority tier: oldest first (FIFO)
                            created_at ASC
                        LIMIT 1000
                    """)
        

                now = datetime.now(UTC).replace(tzinfo=None)  # Convert to timezone-naive
                for email_data in rows:
                    now = datetime.now(UTC).replace(tzinfo=None)  # Update current time for each iteration
                    # Convert asyncpg.Record to a mutable dict to allow safe assignment
                    email_data = dict(email_data)
                    queue_id = email_data['id']
                    contact_id = email_data['contact_id']
                    message_type = email_data['last_message_type']
                    
                    # Start a transaction for each email to prevent race conditions.
                    # Acquire a fresh connection from the pool here (the earlier
                    # fetch_conn was released after fetching rows). Using a per-item
                    # connection ensures the connection is valid for the duration
                    # of the transaction and avoids "connection has been released"
                    # errors.
                    async with worker_pool.acquire() as conn:
                        async with conn.transaction():
                            # Lock the row and check for duplicates by message_type AND recipient_email
                            email_data = await conn.fetchrow("""
                            WITH locked AS (
                                SELECT eq.* FROM email_queue eq
                                WHERE id = $1 AND status = 'pending' AND (scheduled_at IS NULL OR scheduled_at <= NOW())
                                FOR UPDATE SKIP LOCKED
                                LIMIT 1
                            )
                            SELECT l.* FROM locked l
                            WHERE NOT EXISTS (
                                SELECT 1 FROM email_queue eq2
                                WHERE eq2.last_message_type = l.last_message_type
                                AND eq2.recipient_email = l.recipient_email
                                AND eq2.status IN ('sent', 'pending')
                                AND eq2.id != l.id  -- Exclude current message
                                AND eq2.created_at > NOW() - INTERVAL '1 hour'  -- Consider duplicates within last hour
                            )
                        """, queue_id)

                        # fetchrow returns an asyncpg.Record (immutable). Convert to dict
                        # so we can mutate fields (attachment normalization / propagation).
                        if email_data:
                            try:
                                email_data = dict(email_data)
                            except Exception:
                                # Fallback: leave as-is if conversion fails (rare)
                                pass

                        # If this queued row has a due_at timestamp, respect it and skip until due
                        try:
                            due_at = email_data.get('due_at') if email_data else None
                            if due_at:
                                # Diagnostic: get DB NOW() to compare clocks
                                try:
                                    db_now = await conn.fetchval('SELECT NOW()')
                                except Exception:
                                    db_now = None
                                logger.debug(f"[DUE_AT-DBG] queue_id={queue_id} python_now={now!r} db_now={db_now!r} due_at_raw={due_at!r} due_at_type={type(due_at)!r}")
                                # Normalize due_at to naive for comparison if tz-aware
                                if getattr(due_at, 'tzinfo', None) is not None:
                                    due_at = due_at.replace(tzinfo=None)
                                # Prefer DB clock for the decision to avoid host/DB clock skew
                                try:
                                    if db_now is not None:
                                        if getattr(db_now, 'tzinfo', None) is not None:
                                            db_now = db_now.replace(tzinfo=None)
                                        if db_now < due_at:
                                            logger.debug(f"[DUE_AT] Skipping queue_id={queue_id} not due until {due_at} (db_now={db_now})")
                                            continue
                                    else:
                                        # Fallback: if we couldn't get db_now, use python now
                                        if now < due_at:
                                            logger.debug(f"[DUE_AT] Skipping queue_id={queue_id} not due until {due_at}")
                                            continue
                                except Exception:
                                    # If comparison fails, don't skip silently; proceed
                                    logger.debug(f"[DUE_AT-DBG] Comparison error for queue_id={queue_id}", exc_info=True)
                        except Exception:
                            # If anything goes wrong with due_at handling, proceed to other checks
                            logger.debug(f"[DUE_AT-DBG] Error while evaluating due_at for queue_id={queue_id}", exc_info=True)
                            pass

                        if not email_data:
                            # Mark as skipped if duplicate found or not in pending state
                            await conn.execute(
                                "UPDATE email_queue SET status = 'skipped', "
                                "error_message = 'Duplicate message within 1 hour' "
                                "WHERE id = $1 AND status = 'pending'",
                                queue_id
                            )
                            continue

                        sender = email_data['sender_email']
                        # Sanitize recipient email: remove any accidental whitespace/newlines
                        recipient_raw = email_data['recipient_email'] or ''
                        recipient = recipient_raw.strip()
                        if not recipient:
                            logger.error(f"[SEND EMAIL] Empty recipient for queue_id={queue_id}, marking as failed")
                            await conn.execute(
                                "UPDATE email_queue SET status = 'failed', error_message = 'Empty recipient email' WHERE id = $1",
                                queue_id
                            )
                            continue
                        # Support both legacy column names: some code inserts into
                        # `message` while other parts (campaign bulk) insert into
                        # `body`. Prefer `message` if present, then fall back to
                        # `body`. Default to empty string to avoid None issues.
                        message = email_data.get('message') or email_data.get('body') or ''

                        # Normalize attachment: ensure bytes for Graph API. Handle memoryview -> bytes,
                        # and base64/data-URL encoded strings.
                        try:
                            att = email_data.get('attachment')
                            # memoryview can come back from asyncpg for BYTEA; convert to bytes
                            if att is not None and hasattr(att, 'tobytes') and not isinstance(att, (bytes, bytearray)):
                                try:
                                    att_bytes = bytes(att)
                                    att = att_bytes
                                    email_data['attachment'] = att_bytes
                                except Exception:
                                    # leave as-is if conversion fails
                                    pass

                            if att and isinstance(att, str):
                                # Handle data URL like: data:application/pdf;base64,....
                                b64 = att
                                if b64.startswith('data:') and ',' in b64:
                                    b64 = b64.split(',', 1)[1]
                                import base64
                                try:
                                    decoded = base64.b64decode(b64)
                                    # Replace in-memory so subsequent code uses bytes
                                    email_data['attachment'] = decoded
                                    # Persist decoded bytes back to the queue row to avoid re-decoding
                                    try:
                                        await conn.execute('UPDATE email_queue SET attachment = $1 WHERE id = $2', decoded, queue_id)
                                        logger.debug(f"[ATTACHMENT NORMALIZE] Decoded and persisted base64 attachment for queue_id={queue_id} (len={len(decoded)})")
                                    except Exception as e:
                                        logger.debug(f"[ATTACHMENT NORMALIZE] Could not persist decoded attachment for queue_id={queue_id}: {e}")
                                except Exception as e:
                                    logger.debug(f"[ATTACHMENT NORMALIZE] Failed to base64-decode attachment for queue_id={queue_id}: {e}")
                        except Exception as e:
                            logger.error(f"[ATTACHMENT NORMALIZE] Unexpected error while normalizing attachment for queue_id={queue_id}: {e}")

                        # ALWAYS ensure non-empty subject
                        subject = email_data['subject']
                        if not subject or not subject.strip():
                            # Only fallback if truly empty
                            event = await conn.fetchrow(
                                'SELECT event_name FROM event WHERE id = $1',
                                email_data.get('event_id')
                            )
                            event_name = event['event_name'] if event else 'your reservation'
                            subject = f"Follow-up regarding {event_name}"
                            logger.warning(f"[SEND EMAIL] Empty subject for email {queue_id}, using fallback: {subject}")

                        # Final validation - subject must NEVER be empty
                        if not subject or not subject.strip():
                            subject = "Follow-up regarding your reservation"
                            logger.error(f"[SEND EMAIL] Subject still empty after fallback, using default: {subject}")

                        # Clean up subject by collapsing whitespace
                        subject = ' '.join(str(subject).split())

                        # --- Get contact with campaign info (avoid FOR UPDATE with LEFT JOIN) ---
                        contact = await conn.fetchrow(
                            """
                            SELECT cc.campaign_paused, cc.stage, cc.status, cc.event_id
                            FROM campaign_contacts cc
                            WHERE cc.id = $1
                            FOR UPDATE
                            """,
                            contact_id
                        )

                        # Get event name separately if needed
                        event_name = None
                        if contact and contact['event_id']:
                            event_row = await conn.fetchrow(
                                'SELECT event_name FROM event WHERE id = $1',
                                contact['event_id']
                            )
                            event_name = event_row['event_name'] if event_row else None

                        # If we still don't have a valid subject, use a fallback
                        if not subject or not subject.strip():
                            fallback_event_name = event_name or 'your reservation'
                            subject = f"Follow-up regarding {fallback_event_name}"
                            logger.warning(f"[SEND EMAIL] Fallback subject for email {queue_id}: {subject}")
                        if not contact or contact['campaign_paused']:
                            logger.debug(f"[SKIP] Contact {contact_id} is paused or missing.")
                            continue

                        if contact['stage'] in ('completed', 'cancelled'):
                            logger.debug(f"[SKIP] Contact {contact_id} is in terminal stage '{contact['stage']}'.")
                            continue

                        if contact['status'] in ('Replied', 'completed', 'cancelled'):
                            logger.debug(f"[SKIP] Contact {contact_id} has terminal status '{contact['status']}'.")
                            continue

                        # --- DUE-TIME VERIFICATION ---
                        # Ensure queued reminders are only sent when the configured
                        # delay since the last sent message has actually elapsed.
                        try:
                            # Determine reference timestamp similar to campaign logic
                            ref_time = None
                            used_reference = None
                            sent_row = await conn.fetchrow("""
                                SELECT sent_at FROM email_queue
                                WHERE contact_id = $1 AND status = 'sent' AND sent_at IS NOT NULL
                                ORDER BY sent_at DESC LIMIT 1
                            """, contact_id)
                            if sent_row and sent_row.get('sent_at'):
                                ref_time = sent_row['sent_at']
                                used_reference = 'email_queue.sent_at'
                            else:
                                msg_row = await conn.fetchrow("""
                                    SELECT sent_at FROM messages
                                    WHERE contact_id = $1 AND direction = 'sent' AND sent_at IS NOT NULL
                                    ORDER BY sent_at DESC LIMIT 1
                                """, contact_id)
                                if msg_row and msg_row.get('sent_at'):
                                    ref_time = msg_row['sent_at']
                                    used_reference = 'messages.sent_at'
                                else:
                                    # Fallback to campaign_contacts.last_triggered_at (may be None)
                                    try:
                                        lr = await conn.fetchrow('SELECT last_triggered_at FROM campaign_contacts WHERE id = $1', contact_id)
                                        if lr and lr.get('last_triggered_at'):
                                            ref_time = lr['last_triggered_at']
                                            used_reference = 'campaign_contacts.last_triggered_at'
                                    except Exception:
                                        ref_time = None

                            # Compute days since last send
                            time_since_last = None
                            if ref_time:
                                if getattr(ref_time, 'tzinfo', None) is not None:
                                    ref_time = ref_time.replace(tzinfo=None)
                                delta = (now - ref_time)
                                time_since_last = delta.total_seconds() / 86400.0

                            # Mapping of minimum days required since last send for each reminder type
                            min_days_map = {
                                'reminder1': 3,
                                'reminder2': 4,
                                'forms_initial': 0,
                                'forms_reminder1': 2,
                                'forms_reminder2': 2,
                                'forms_reminder3': 3,
                                'payments_initial': 0,
                                'payments_reminder1': 2,
                                'payments_reminder2': 2,
                                'payments_reminder3': 3,
                                'payments_reminder4': 7,
                                'payments_reminder5': 7,
                                'payments_reminder6': 7,
                            }

                            # Only enforce for known reminder types; allow others through
                            required_days = min_days_map.get(message_type)
                            if required_days is not None:
                                # If we have no ref_time and required_days>0, it's not due yet
                                if time_since_last is None and required_days > 0:
                                    logger.debug(f"[DUE CHECK] Skipping send queue_id={queue_id} ({message_type}) for contact {contact_id}: no prior sent timestamp (need {required_days}d)")
                                    continue
                                if time_since_last is not None and time_since_last < required_days:
                                    logger.debug(f"[DUE CHECK] Skipping send queue_id={queue_id} ({message_type}) for contact {contact_id}: only {time_since_last:.2f}d since last send (need {required_days}d) based_on={used_reference}")
                                    continue
                        except Exception as e:
                            # On any error, do not block the send; log and proceed
                            logger.debug(f"[DUE CHECK] Error verifying due time for queue_id={queue_id}: {e}")

                        # --- Enhanced duplicate prevention: check for same message_type in sent OR pending status ---
                        duplicate = await conn.fetchval("""
                            SELECT 1 FROM email_queue
                            WHERE contact_id = $1
                            AND last_message_type = $2
                            AND status = 'pending'
                            AND id != $3  -- Exclude current message
                            LIMIT 1
                        """, contact_id, message_type, queue_id)

                        logger.debug(f"[DUPLICATE DEBUG] queue_id={queue_id} message_type={message_type} duplicate={bool(duplicate)}")

                        if duplicate:
                            logger.info(f"[DUPLICATE] Marking duplicate {message_type} as skipped for contact {contact_id}")
                            await conn.execute(
                                "UPDATE email_queue SET status = 'skipped', error_message = 'Duplicate message type already sent or pending' WHERE id = $1",
                                queue_id
                            )
                            continue

                        # --- EARLY BUSINESS HOURS CHECK ---
                        # Check BEFORE stuck-pending logic so rescheduled emails skip stuck check
                        # This prevents emails waiting for business hours from being marked as failed
                        business_hours_ok = False
                        try:
                            business_hours_ok = is_business_hours(now)
                            logger.debug(f"[BUSINESS HOURS CHECK] queue_id={queue_id} now={now} business_hours_ok={business_hours_ok}")
                            
                            if not business_hours_ok:
                                # Outside business hours - reschedule and skip this email
                                next_allowed = next_allowed_uk_business_time(now)
                                await conn.execute(
                                    "UPDATE email_queue SET scheduled_at = $1 WHERE id = $2",
                                    next_allowed, queue_id
                                )
                                logger.warning(f"[BUSINESS HOURS ENFORCEMENT] queue_id={queue_id} attempted send at {now} (outside business hours). Rescheduled to {next_allowed}")
                                continue
                        except Exception as e:
                            logger.error(f"[BUSINESS HOURS ENFORCEMENT] ERROR checking business hours for queue_id={queue_id}: {e}", exc_info=True)
                            # On error, DO NOT send - skip and let next cycle retry
                            continue

                        # Domain-aware cooldown & stuck-pending logic
                        # Prefer domain-level stats (key: domain:example.com) then per-email
                        domain = None
                        if sender and '@' in sender:
                            domain = sender.split('@', 1)[1].lower()
                        domain_key = f"domain:{domain}" if domain else None

                        # Fetch domain and email stats using short-lived connections so we
                        # do not rely on the possibly-released `conn` from earlier.
                        domain_stats = None
                        if domain_key:
                            async with worker_pool.acquire() as conn2:
                                domain_stats = await conn2.fetchrow('SELECT last_sent, cooldown FROM sender_stats WHERE sender_email = $1', domain_key)

                        email_stats = None
                        async with worker_pool.acquire() as conn2:
                            email_stats = await conn2.fetchrow('SELECT last_sent, cooldown FROM sender_stats WHERE sender_email = $1', sender)

                        stats = domain_stats or email_stats
                        cooldown_seconds = stats['cooldown'] if stats and stats.get('cooldown') else int(os.getenv('DOMAIN_COOLDOWN_SECONDS', '90'))
                        last_sent = stats['last_sent'] if stats else None

                        # --- Stuck-pending safeguard (improved) ---
                        # Check if CURRENT email has a future scheduled_at (due to business hours, cooldown, etc.)
                        # If yes, skip stuck-pending check entirely - email is intentionally waiting
                        current_email_scheduled = None
                        async with worker_pool.acquire() as conn2:
                            current_email_scheduled = await conn2.fetchval(
                                "SELECT scheduled_at FROM email_queue WHERE id = $1",
                                queue_id
                            )
                        
                        if current_email_scheduled:
                            if getattr(current_email_scheduled, 'tzinfo', None) is not None:
                                current_email_scheduled = current_email_scheduled.replace(tzinfo=None)
                            
                            if current_email_scheduled > now:
                                logger.debug(f"[SKIP_PENDING] queue_id={queue_id} scheduled for future ({current_email_scheduled} > {now}). Skipping stuck-pending check.")
                                continue

                        stuck_pending = None
                        async with worker_pool.acquire() as conn2:
                            stuck_pending = await conn2.fetchrow("""
                                SELECT id, created_at, scheduled_at FROM email_queue
                                WHERE contact_id = $1 AND last_message_type = $2 AND status = 'pending'
                                ORDER BY created_at ASC LIMIT 1
                            """, contact_id, message_type)

                        if stuck_pending:
                                pending_age = (now - stuck_pending['created_at']).total_seconds()
                                max_safe_age = 300

                                # Check if email is scheduled for the future (e.g., due to business hours)
                                # If scheduled_at > now, the email is waiting for a future time window (not stuck)
                                scheduled_at = stuck_pending.get('scheduled_at')
                                if scheduled_at:
                                    if getattr(scheduled_at, 'tzinfo', None) is not None:
                                        scheduled_at = scheduled_at.replace(tzinfo=None)
                                    
                                    if scheduled_at > now:
                                        logger.debug(f"[SKIP_PENDING] Email {stuck_pending['id']} scheduled for future (scheduled_at={scheduled_at} > now={now}). Not marking as stuck.")
                                        continue

                                # If sender/domain is still cooling down, do NOT mark as failed ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ just wait
                                if last_sent:
                                    if getattr(last_sent, 'tzinfo', None) is not None:
                                        last_sent = last_sent.replace(tzinfo=None)
                                    elapsed_since_last = (now - last_sent).total_seconds()
                                    if elapsed_since_last < cooldown_seconds:
                                        logger.debug(f"[SKIP_PENDING] Sender {sender} cooling down ({elapsed_since_last:.0f}s < {cooldown_seconds}s). Not considering stuck yet for contact {contact_id}")
                                        # If this is a custom flow step, allow bypassing the cooling down behavior
                                        if isinstance(message_type, str) and message_type.startswith('custom-step-'):
                                            logger.debug(f"[COOLDOWN BYPASS] Allowing custom-step to proceed despite cooldown for queue_id={queue_id}, sender={sender}")
                                        else:
                                            continue

                                # Sender/domain ready ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Â ÃƒÂ¢Ã¢â€šÂ¬Ã¢â€žÂ¢ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã¢â‚¬Â¦Ãƒâ€šÃ‚Â¡ÃƒÆ’Ã†â€™ÃƒÂ¢Ã¢â€šÂ¬Ã…Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ÃƒÆ’Ã†â€™Ãƒâ€ Ã¢â‚¬â„¢ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã†â€™Ãƒâ€šÃ‚Â¢ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬Ãƒâ€¦Ã‚Â¡ÃƒÆ’Ã¢â‚¬Å¡Ãƒâ€šÃ‚Â¬ now decide if message really stuck
                                if pending_age > max_safe_age:
                                    async with worker_pool.acquire() as conn2:
                                        await conn2.execute('''
                                            UPDATE email_queue
                                            SET status = 'failed', error_message = 'Message was stuck in pending state'
                                            WHERE id = $1
                                        ''', stuck_pending['id'])
                                    logger.warning(f"Marked stuck pending message {stuck_pending['id']} as failed after {pending_age:.0f} seconds (safe limit {max_safe_age}s)")
                                else:
                                    logger.debug(f"[SKIP] Message type {message_type} is still pending for contact {contact_id} (age: {pending_age:.0f}s, safe <= {max_safe_age}s)")
                                    continue

                        # Final cooldown enforcement - ALL messages respect cooldown
                        # Cooldown applies to all message types equally; priority is handled via ORDER BY
                        async with worker_pool.acquire() as conn2:
                            cooldown_ok = await check_sender_cooldown(conn2, sender, now)
                        logger.debug(f"[COOLDOWN CHECK] queue_id={queue_id} sender={sender} message_type={message_type} cooldown_ok={cooldown_ok}")

                        if not cooldown_ok:
                            # --- COOLDOWN RESCHEDULE LOGIC ---
                            # If cooldown would delay sending, check if it pushes past 21:00 UK time
                            # If so, reschedule to next business window
                            try:
                                async with worker_pool.acquire() as conn2:
                                    sender_stats = await conn2.fetchrow(
                                        'SELECT last_sent, cooldown FROM sender_stats WHERE sender_email = $1 OR sender_email = $2',
                                        sender, f"domain:{sender.split('@')[1].lower() if '@' in sender else ''}"
                                    )
                                
                                if sender_stats and sender_stats.get('last_sent'):
                                    last_sent = sender_stats['last_sent']
                                    if getattr(last_sent, 'tzinfo', None) is not None:
                                        last_sent = last_sent.replace(tzinfo=None)
                                    
                                    default_cooldown = int(os.getenv('DOMAIN_COOLDOWN_SECONDS', '90'))
                                    cooldown_seconds = sender_stats['cooldown'] if sender_stats.get('cooldown') else default_cooldown
                                    cooldown_seconds = max(30, min(300, int(cooldown_seconds)))
                                    
                                    # Calculate when cooldown expires
                                    cooldown_expires = last_sent + timedelta(seconds=cooldown_seconds)
                                    
                                    # Check if this time is after 21:00 UK time
                                    try:
                                        if not is_business_hours(cooldown_expires):
                                            # Cooldown expires outside business hours
                                            # Recalculate scheduled_at to next business window
                                            new_scheduled_at = next_allowed_uk_business_time(cooldown_expires)
                                            await conn.execute(
                                                "UPDATE email_queue SET scheduled_at = $1 WHERE id = $2",
                                                new_scheduled_at, queue_id
                                            )
                                            logger.info(f"[COOLDOWN RESCHEDULE] queue_id={queue_id} sender={sender}: cooldown expires at {cooldown_expires} (outside business hours). Rescheduled to {new_scheduled_at}")
                                    except Exception as e:
                                        logger.debug(f"[COOLDOWN RESCHEDULE] Error checking business hours for reschedule: {e}")
                            except Exception as e:
                                logger.debug(f"[COOLDOWN RESCHEDULE] Error in cooldown reschedule logic: {e}")
                            
                            logger.debug(f"[COOLDOWN] Skipping message {message_type} for sender {sender} due to cooldown (priority order preserved)")
                            # Keep the queued item as 'pending' - worker will retry it later
                            # Next cycle will fetch in priority order again
                            continue

                        # --- BOUNCE CHECK ---
                        # Parse the email column to get the main recipient for bounce checking
                        parsed_emails = process_emails(recipient, validate=True)
                        main_email_for_bounce_check = parsed_emails[0] if parsed_emails else recipient

                        # Check if main recipient email has bounced (use fresh conn)
                        bounced_check = None
                        async with worker_pool.acquire() as conn2:
                            bounced_check = await conn2.fetchrow('SELECT bounce_type FROM bounced_emails WHERE LOWER(email) = LOWER($1)', main_email_for_bounce_check)
                            logger.debug(f"[BOUNCE CHECK] queue_id={queue_id} email={main_email_for_bounce_check} bounced={bool(bounced_check)}")
                            if bounced_check:
                                logger.warning(f"[BOUNCE] Skipping email to {main_email_for_bounce_check} - previously bounced ({bounced_check['bounce_type']})")
                                # Mark email as failed due to bounce
                                await conn2.execute('''
                                    UPDATE email_queue
                                    SET status = 'failed', error_message = 'Email previously bounced'
                                    WHERE id = $1
                                ''', queue_id)
                                continue

                        # --- SEND EMAIL ---
                            # The connection `conn` is already acquired from line 6327 and remains
                            # valid through all the checks and send operations. Do NOT acquire a new one.
                            try:
                                logger.info(f"[SEND] Sending to {main_email_for_bounce_check} from {sender} (subject: {subject[:50]}...)")

                                # --- INDIVIDUAL EMAIL LOGIC (NO THREADING) ---
                                # Each email is sent independently with its own subject
                                logger.debug(f"[INDIVIDUAL] Sending {message_type} as individual email to {main_email_for_bounce_check}")

                                # --- SUBJECT HANDLING ---
                                # Use the subject from the template as-is (already cleaned above)
                                # Do NOT override with original subject - each stage has its own subject

                                logger.info(f"[EMAIL] Preparing to send {message_type} to {main_email_for_bounce_check} with subject: {subject[:50]}...")

                                # NO QUOTED BLOCK - Each email sends individually with its own subject
                                # Skip all conversation history and quoted block generation
                                logger.debug(f"[INDIVIDUAL] Sending individual email with no conversation history")

                                # Format message with proper line breaks - INDIVIDUAL EMAIL (NO QUOTES)
                                # Convert HTML or text into normalized plain-text suitable for email
                                message_body = to_plain_text(message)
                                # Force plain text content type
                                content_type = "Text"

                                # Send the email WITHOUT threading - individual email with unique subject
                                # IMPORTANT: Do NOT use the `cc_store` column for sending. cc_store is persistent
                                # storage only. When composing recipients, derive them from the contact's
                                # `email` field (legacy comma-separated behavior) or from the queued
                                # recipient value. This ensures cc_store is never used in campaign sends.
                                # Fetch contact including any stored attachment metadata so we can
                                # fallback to a contact-level attachment if the queued row lacks one.
                                contact_row = await conn.fetchrow(
                                    'SELECT email, event_id, attachment, attachment_filename, attachment_mimetype FROM campaign_contacts WHERE id = $1',
                                    contact_id
                                )
                                contact_email_field = contact_row['email'] if contact_row and contact_row.get('email') else recipient

                                # If the queued email has no attachment but the contact has one,
                                # attach it now to avoid missing files due to race conditions
                                try:
                                    if not email_data.get('attachment') and contact_row and contact_row.get('attachment'):
                                        email_data['attachment'] = contact_row.get('attachment')
                                        email_data['attachment_filename'] = contact_row.get('attachment_filename')
                                        email_data['attachment_mimetype'] = contact_row.get('attachment_mimetype')
                                        # Persist the attachment into the queued row so subsequent retries/workers see it
                                        try:
                                            await conn.execute(
                                                'UPDATE email_queue SET attachment = $1, attachment_filename = $2, attachment_mimetype = $3 WHERE id = $4',
                                                email_data['attachment'], email_data.get('attachment_filename'), email_data.get('attachment_mimetype'), queue_id
                                            )
                                            logger.debug(f"[ATTACHMENT FALLBACK] Propagated contact attachment to queue_id={queue_id} from contact_id={contact_id}")
                                        except Exception as e:
                                            logger.debug(f"[ATTACHMENT FALLBACK] Failed to persist propagated attachment for queue_id={queue_id}: {e}")
                                except Exception as e:
                                    logger.error(f"[ATTACHMENT FALLBACK] Error while applying contact-level attachment for queue_id={queue_id}: {e}")

                                # Prefer cc_recipients stored on the queued row (this may be populated from cc_store at queue time)
                                cc_raw = email_data.get('cc_recipients') if email_data and email_data.get('cc_recipients') else None
                                cc_emails = None
                                if cc_raw:
                                    # cc_recipients stored as semicolon-separated string - normalize to list
                                    cc_emails = [e.strip() for e in re.split(r'[;,\s]+', cc_raw) if e.strip()]

                                # Fallback: legacy behavior - parse additional addresses embedded in the contact email field
                                if not cc_emails:
                                    contact_emails = process_emails(contact_email_field or recipient, validate=True)
                                    if contact_emails:
                                        main_recipient = contact_emails[0]
                                        cc_emails = contact_emails[1:] if len(contact_emails) > 1 else None
                                    else:
                                        main_recipient = recipient
                                        cc_emails = None
                                else:
                                    # If cc_recipients was present, ensure main_recipient comes from the queued recipient (parsed)
                                    parsed_main = process_emails(contact_email_field or recipient, validate=True)
                                    main_recipient = parsed_main[0] if parsed_main else recipient

                                # Debug: log resolved recipient and CCs to help diagnose missing CC issues
                                logger.debug(f"[SEND DEBUG] queue_id={queue_id}, sender={sender}, recipient_raw={recipient_raw}, contact_email_field={contact_email_field}, main_recipient={main_recipient}, cc_emails={cc_emails}")

                                # Send using the resolved main_recipient and cc_emails (from queue or legacy parsing)
                                # Prepare message with history
                                queue_item = {
                                    'contact_id': contact_id,
                                    'sender_email': sender,
                                    'message': message_body,
                                    'campaign_stage': message_type,
                                    'type': message_type
                                }
                                prepared_message = await prepare_message_for_sending(conn, queue_item)
                                # Use the prepared message body for sending
                                message_body = prepared_message

                                # Send email
                                # Debug: log attachment presence before sending
                                try:
                                    att = email_data.get('attachment')
                                    att_name = email_data.get('attachment_filename')
                                    att_type = email_data.get('attachment_mimetype')
                                    # Normalize common container types to raw bytes
                                    if att is not None and not isinstance(att, (bytes, bytearray)):
                                        try:
                                            # memoryview or other buffer-like
                                            att = bytes(att)
                                            email_data['attachment'] = att
                                        except Exception:
                                            # leave as-is if conversion fails
                                            pass

                                    if att is None:
                                        logger.debug(f"[ATTACHMENT DEBUG] No attachment for queue_id={queue_id}")
                                    else:
                                        try:
                                            att_len = len(att)
                                        except Exception:
                                            att_len = 'unknown'
                                        logger.debug(f"[ATTACHMENT DEBUG] queue_id={queue_id} has attachment name={att_name} type={att_type} length={att_len} ({type(att)})")
                                except Exception as e:
                                    logger.error(f"[ATTACHMENT DEBUG] Error inspecting attachment for queue_id={queue_id}: {e}")

                                result = graph_email.send_graph_email(
                                    sender,
                                    main_recipient,
                                    subject,
                                    message_body,
                                    test_mode=False,
                                    in_reply_to=None,
                                    conversation_id=None,
                                    references=None,
                                    content_type=content_type,
                                    cc_emails=cc_emails,
                                    attachment_bytes=att,
                                    attachment_filename=att_name,
                                    attachment_mimetype=att_type
                                )

                                logger.debug(f"[GRAPH RESULT] queue_id={queue_id} send result: {result}")

                                # Check if email send failed and capture error
                                if result.get('status') == 'failed':
                                    error_msg = result.get('error_message', 'Unknown error')
                                    http_code = result.get('code', 0)
                                    full_error = f"[{http_code}] {error_msg}"
                                    logger.error(f"[SEND FAILED] queue_id={queue_id} contact_id={contact_id} to {main_recipient}: {full_error}")

                                    # Store error in campaign_contacts so it shows in the UI
                                    try:
                                        async with worker_pool.acquire() as conn2:
                                            await conn2.execute('''
                                                UPDATE campaign_contacts
                                                SET email_error = $1,
                                                    last_error_at = $2
                                                WHERE id = $3
                                            ''', full_error, now, contact_id)
                                        logger.debug(f"[ERROR STORED] Saved email_error for contact_id={contact_id}: {full_error}")
                                    except Exception as e:
                                        logger.error(f"[ERROR STORE FAILED] Could not store error for contact_id={contact_id}: {e}")

                                    # Mark queue as failed
                                    try:
                                        async with worker_pool.acquire() as conn2:
                                            await conn2.execute('''
                                                UPDATE email_queue
                                                SET status = 'failed', error_message = $1
                                                WHERE id = $2
                                            ''', full_error, queue_id)
                                    except Exception as e:
                                        logger.error(f"[ERROR] Failed to mark queue {queue_id} as failed: {e}")

                                    continue

                                # Get the message ID and conversation ID from the result
                                result_message_id = result.get('message_id')
                                result_conversation_id = result.get('conversation_id')

                                # Log the send result (no threading)
                                if result_message_id or result_conversation_id:
                                    logger.info(f"[INDIVIDUAL] Email sent to {main_recipient}: "
                                              f"message_id={result_message_id}, conversation_id={result_conversation_id}")
                                else:
                                    logger.debug(f"[INDIVIDUAL] Email sent to {main_recipient} (no threading info returned)")

                                # Immediately update queue status to 'sent' and record sent_at so
                                # downstream failures (storing messages/mappings) don't remove the
                                # record that the message actually left our system.
                                try:
                                    async with worker_pool.acquire() as conn2:
                                        await conn2.execute('''
                                            UPDATE email_queue
                                            SET status = 'sent', sent_at = $1, conversation_id = $2, message_id = $3
                                            WHERE id = $4
                                        ''', now, result_conversation_id, result_message_id, queue_id)
                                except Exception as e:
                                    logger.error(f"[CRITICAL] Failed to mark queue_id={queue_id} as sent: {e}")

                                # Store sent message in messages table for reply detection tracking
                                # Extract main message content without history block
                                main_content = message_body.strip()

                                # Store the message with threading info for reply detection (even though email was sent individually)

                                # Store message record; do NOT populate cc_recipients from cc_store.
                                try:
                                    async with worker_pool.acquire() as conn2:
                                        await conn2.execute('''
                                            INSERT INTO messages (contact_id, direction, sender_email, recipient_email, cc_recipients, subject, body, sent_at, stage, message_type, message_id, conversation_id)
                                            VALUES ($1, 'sent', $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                                        ''', contact_id, sender, main_recipient, ';'.join(cc_emails) if cc_emails else None, subject, main_content, now, contact['stage'] if contact else None, message_type, result_message_id, result_conversation_id)
                                except Exception as e:
                                    logger.error(f"[ERROR] Failed to insert message record for queue_id={queue_id}: {e}")

                                # Ensure campaign_contacts keeps last_sent_body/last_sent_at for quoting
                                try:
                                    async with worker_pool.acquire() as conn2:
                                        await conn2.execute('''
                                            UPDATE campaign_contacts
                                            SET last_sent_body = $1,
                                                last_sent_at = $2
                                            WHERE id = $3
                                        ''', main_content, now, contact_id)
                                    logger.debug(f"[HISTORY] Updated campaign_contacts.last_sent_body for contact {contact_id} (len={len(main_content)})")
                                except Exception as e:
                                    logger.error(f"[HISTORY] Failed to update campaign_contacts.last_sent_body for contact {contact_id}: {e}")

                                # Map message_id to contact(s) so replies addressed to CC recipients can be detected deterministically
                                try:
                                    if result_message_id:
                                        # Normalize message id (strip angle brackets and whitespace) to ensure consistent lookups
                                        try:
                                            mid_norm = (result_message_id or '').strip(' <>')
                                        except Exception:
                                            mid_norm = result_message_id
                                        # Insert mapping for main recipient contact_id
                                        try:
                                            async with worker_pool.acquire() as conn2:
                                                await conn2.execute('''
                                                    INSERT INTO message_contact_map (message_id, contact_id)
                                                    VALUES ($1, $2)
                                                    ON CONFLICT DO NOTHING
                                                ''', mid_norm, contact_id)
                                        except Exception as e:
                                            logger.error(f"[MAPPING] Failed to insert message_contact_map for message {result_message_id}: {e}")

                                        # NOTE: We intentionally DO NOT create message_contact_map entries
                                        # for CC recipients derived from `cc_store`. cc_store is storage-only
                                        # and must not affect campaign sends or reply mapping.
                                except Exception as e:
                                    logger.error(f"[MAPPING] Failed to process message mapping for message {result_message_id}: {e}")

                                # FALLBACK THREADING: Only attempt for follow-up messages, not the initial campaign_main
                                # The initial message establishes the thread, it doesn't reply to one
                                if message_type != 'campaign_main' and (not result_conversation_id or not result_message_id):
                                    try:
                                        logger.info(f"[FALLBACK] Attempting to fetch threading info from Sent Items for follow-up {message_type} to {recipient}")
                                        threading_info = graph_email.fetch_sent_message_ids(
                                            sender,
                                            subject,
                                            recipient
                                        )
                                        if threading_info:
                                            result_message_id = threading_info.get('message_id') or result_message_id
                                            result_conversation_id = threading_info.get('conversation_id') or result_conversation_id
                                            logger.info(f"[FALLBACK] Successfully retrieved threading info: "
                                                      f"message_id={result_message_id}, conversation_id={result_conversation_id}")
                                        else:
                                            logger.warning(f"[FALLBACK] Could not retrieve threading info from Sent Items for {recipient}")
                                    except Exception as e:
                                        logger.error(f"[FALLBACK] Error fetching threading info from Sent Items: {e}")
                                elif message_type == 'campaign_main':
                                    logger.info(f"[THREADING] Initial message sent - no fallback needed for {message_type}")

                                # --- Update queue status ---
                                try:
                                    async with worker_pool.acquire() as conn2:
                                        await conn2.execute('''
                                            UPDATE email_queue
                                            SET status = 'sent', sent_at = $1, conversation_id = $2, message_id = $3
                                            WHERE id = $4
                                        ''', now, result_conversation_id, result_message_id, queue_id)
                                except Exception as e:
                                    logger.error(f"[ERROR] Failed to update queue status for queue_id={queue_id}: {e}")



                                # --- Update sender cooldown (domain + email) ---
                                # Randomize domain cooldown between 60 and 180 seconds on each successful send
                                default_domain_cd = int(os.getenv('DOMAIN_COOLDOWN_SECONDS', '90'))
                                if sender and '@' in sender:
                                    domain = sender.split('@', 1)[1].lower()
                                    domain_key = f"domain:{domain}"
                                    domain_cd = random.randint(60, 180)
                                    # Upsert domain-level row and set randomized cooldown
                                    try:
                                        async with worker_pool.acquire() as conn2:
                                            await conn2.execute('''
                                                INSERT INTO sender_stats (sender_email, last_sent, cooldown)
                                                VALUES ($1, $2, $3)
                                                ON CONFLICT (sender_email) DO UPDATE SET
                                                    last_sent = EXCLUDED.last_sent,
                                                    cooldown = EXCLUDED.cooldown
                                            ''', domain_key, now, domain_cd)
                                    except Exception as e:
                                        logger.error(f"[COOLDOWN] Failed to upsert domain sender_stats for {domain_key}: {e}")

                                # Also upsert per-email last_sent so per-address checks remain possible
                                try:
                                    async with worker_pool.acquire() as conn2:
                                        await conn2.execute('''
                                            INSERT INTO sender_stats (sender_email, last_sent, cooldown)
                                            VALUES ($1, $2, $3)
                                            ON CONFLICT (sender_email) DO UPDATE SET
                                                last_sent = EXCLUDED.last_sent
                                        ''', sender, now, default_domain_cd)
                                except Exception as e:
                                    logger.error(f"[COOLDOWN] Failed to upsert sender_stats for {sender}: {e}")

                                # --- Update contact trigger and status only AFTER successful send ---
                                detailed_trigger = f"{now.strftime('%Y-%m-%d %H:%M:%S')} - EMAIL SENT: {message_type} to {recipient}"

                                # Clear any previous email errors since send succeeded
                                try:
                                    await conn.execute('''
                                        UPDATE campaign_contacts
                                        SET email_error = NULL,
                                            last_error_at = NULL
                                        WHERE id = $1
                                    ''', contact_id)
                                except Exception as e:
                                    logger.debug(f"[DEBUG] Could not clear email_error for contact_id={contact_id}: {e}")

                                # Map last_message_type to the corresponding contact status
                                status_map = {
                                    'campaign_main': 'first_message_sent',
                                    'reminder1': 'first_reminder',
                                    'reminder2': 'second_reminder',
                                    'forms_initial': 'forms_initial_sent',
                                    'forms_reminder1': 'forms_reminder1_sent',
                                    'forms_reminder2': 'forms_reminder2_sent',
                                    'forms_reminder3': 'forms_reminder3_sent',
                                    'payments_initial': 'payments_initial_sent',
                                    'payments_reminder1': 'payments_reminder1_sent',
                                    'payments_reminder2': 'payments_reminder2_sent',
                                    'payments_reminder3': 'payments_reminder3_sent',
                                    'payments_reminder4': 'payments_reminder4_sent',
                                    'payments_reminder5': 'payments_reminder5_sent',
                                    'payments_reminder6': 'payments_reminder6_sent'
                                }
                                new_status = status_map.get(message_type, message_type)

                                # If this is a custom flow step, set stage to 'custom' and a status indicating step number
                                if isinstance(message_type, str) and message_type.startswith('custom-step-'):
                                    try:
                                        step_num = int(message_type.split('custom-step-')[-1])
                                        await conn.execute('''
                                            UPDATE campaign_contacts
                                            SET last_triggered_at = $1,
                                                trigger = COALESCE(trigger || E'\n', '') || $2,
                                                status = $3,
                                                stage = 'custom',
                                                last_message_type = $4
                                            WHERE id = $5
                                        ''', now, detailed_trigger, f'step-{step_num}_sent', message_type, contact_id)
                                    except Exception:
                                        await conn.execute('''
                                            UPDATE campaign_contacts
                                            SET last_triggered_at = $1,
                                                trigger = COALESCE(trigger || E'\n', '') || $2,
                                                status = $3,
                                                last_message_type = $4
                                            WHERE id = $5
                                        ''', now, detailed_trigger, new_status, message_type, contact_id)
                                else:
                                    await conn.execute('''
                                        UPDATE campaign_contacts
                                        SET last_triggered_at = $1,
                                            trigger = COALESCE(trigger || E'\n', '') || $2,
                                            status = $3,
                                            last_message_type = $4
                                        WHERE id = $5
                                    ''', now, detailed_trigger, new_status, message_type, contact_id)

                                logger.info(f"[SUCCESS] Email sent to {main_recipient} from {sender}" + (f" with CC: {', '.join(cc_emails)}" if cc_emails else ""))
                                # Note: cooldown already updated for domain and sender above
                                logger.debug(f"[COOLDOWN] Updated domain and sender last_sent for {sender} at {now}")
                            except Exception as e:
                                # main_recipient may not have been assigned if the error
                                # happened before recipient parsing; use queued recipient
                                # as a safe fallback to avoid another UnboundLocalError.
                                safe_recipient = locals().get('main_recipient', recipient)
                                logger.error(f"[ERROR] Failed to send email to {safe_recipient}: {e}")
                                # Update queue to failed
                                try:
                                    await conn.execute('''
                                        UPDATE email_queue
                                        SET status = 'failed', error_message = $1
                                        WHERE id = $2
                                    ''', str(e), queue_id)
                                except Exception as db_e:
                                    logger.error(f"[ERROR] Also failed to mark queue {queue_id} as failed: {db_e}")
                                # Notify monitoring service about this failed send
                                try:
                                    from monitoring import log_worker_error
                                    details = {
                                        'recipient': safe_recipient,
                                        'sender': sender,
                                        'subject': (subject if 'subject' in locals() else None),
                                        'queue_id': queue_id
                                    }
                                    await log_worker_error('send_email_worker', 'send_failure', str(e), json.dumps(details))
                                except Exception:
                                    pass

            except Exception as inner_e:
                logger.error(f"[SEND EMAIL WORKER] Inner exception: {inner_e}")
                try:
                    from monitoring import log_worker_error
                    await log_worker_error('send_email_worker', 'worker_exception', str(inner_e), None)
                except Exception:
                    pass
                    
            finally:
                # Always release the advisory lock
                if lock_acquired and lock_conn:
                    try:
                        await lock_conn.execute(f"SELECT pg_advisory_unlock({ADVISORY_LOCK_KEY})")
                        logger.debug(f"[SEND EMAIL WORKER] Released advisory lock {ADVISORY_LOCK_KEY}")
                    except Exception as lock_e:
                        logger.error(f"[SEND EMAIL WORKER] Error releasing lock: {lock_e}")
                    finally:
                        await worker_pool.release(lock_conn)

        except Exception as outer_e:
            logger.error(f"[WORKER ERROR] {outer_e}")
            try:
                from monitoring import log_worker_error
                await log_worker_error('send_email_worker', 'worker_exception', str(outer_e), None)
            except Exception:
                pass

        await asyncio.sleep(10)

# --- Campaign Worker: Handles campaign and reminder flows ---
async def campaign_worker():
    """Background worker that processes campaigns and sends reminders with proper timing"""
    ADVISORY_LOCK_KEY = 90003  # Unique key for campaign_worker
    
    while True:
        lock_acquired = False
        lock_conn = None
        try:
            # Acquire advisory lock to prevent multiple instances
            try:
                lock_conn = await db_pool.acquire()
                lock_acquired = await lock_conn.fetchval(f"SELECT pg_try_advisory_lock({ADVISORY_LOCK_KEY})")
                
                if not lock_acquired:
                    logger.debug("[CAMPAIGN WORKER] Skipped due to active lock (another instance running)")
                    await asyncio.sleep(5)
                    continue
                    
                logger.debug(f"[CAMPAIGN WORKER] Acquired advisory lock {ADVISORY_LOCK_KEY}")
                
            except Exception as e:
                logger.error(f"[CAMPAIGN WORKER] Error acquiring lock: {e}")
                await asyncio.sleep(5)
                continue

            try:
                await update_worker_heartbeat('campaign_worker', 'running')
                async with db_pool.acquire() as conn:
                    now = datetime.now(UTC).replace(tzinfo=None)
                    logger.info(f"[CAMPAIGN WORKER] Starting campaign processing at {now}")

                    # Get all contacts that need processing
                    contacts = await conn.fetch("""
                        SELECT
                            cc.id, cc.email, cc.name, cc.stage, cc.status, cc.campaign_paused,
                            cc.last_triggered_at, cc.last_message_type, cc.event_id,
                            e.sender_email, e.org_name, e.city, e.month, e.venue, e.date2,
                            cc.forms_link, cc.payment_link, cc.trigger
                        FROM campaign_contacts cc
                        JOIN event e ON cc.event_id = e.id
                        WHERE cc.campaign_paused = false
                        AND cc.status NOT IN ('completed', 'cancelled', 'Replied')
                        AND cc.stage NOT IN ('completed', 'cancelled')
                        ORDER BY cc.last_triggered_at ASC NULLS FIRST
                    """)

                    logger.info(f"[CAMPAIGN WORKER] Found {len(contacts)} contacts to process")

                    for contact in contacts:
                        try:
                            await process_contact_campaign(conn, contact, now)
                        except Exception as e:
                            logger.error(f"[CAMPAIGN WORKER] Error processing contact {contact['id']}: {e}")
                            continue

            except Exception as e:
                logger.error(f"[CAMPAIGN WORKER] Worker error: {e}")
                await update_worker_heartbeat('campaign_worker', 'error', str(e))
                
            finally:
                # Always release the advisory lock
                if lock_acquired and lock_conn:
                    try:
                        await lock_conn.execute(f"SELECT pg_advisory_unlock({ADVISORY_LOCK_KEY})")
                        logger.debug(f"[CAMPAIGN WORKER] Released advisory lock {ADVISORY_LOCK_KEY}")
                    except Exception as e:
                        logger.error(f"[CAMPAIGN WORKER] Error releasing lock: {e}")
                    finally:
                        await db_pool.release(lock_conn)
                        
        except Exception as e:
            logger.error(f"[CAMPAIGN WORKER] Unexpected error: {e}")

        # Wait 60 seconds before next cycle
        await asyncio.sleep(60)

async def process_contact_campaign(conn, contact, now):
    """Process a single contact for campaign actions"""
    # Defensive access to record fields (asyncpg.Record or dict)
    contact_id = contact.get('id') if hasattr(contact, 'get') else (contact['id'] if 'id' in contact else None)
    sender_email = contact.get('sender_email') if hasattr(contact, 'get') else (contact['sender_email'] if 'sender_email' in contact else None)
    stage = (contact.get('stage') or '').lower() if hasattr(contact, 'get') else (contact['stage'] or '').lower() if 'stage' in contact else ''
    status = contact.get('status') if hasattr(contact, 'get') else (contact['status'] if 'status' in contact else None)
    status = status or 'pending'
    last_triggered = contact.get('last_triggered_at') if hasattr(contact, 'get') else (contact['last_triggered_at'] if 'last_triggered_at' in contact else None)
    last_message_type = contact.get('last_message_type') if hasattr(contact, 'get') else (contact['last_message_type'] if 'last_message_type' in contact else None)

    # Try fallback to default sender email if missing
    if not sender_email or '@' not in str(sender_email):
        event_id = contact.get('event_id') if hasattr(contact, 'get') else (contact['event_id'] if 'event_id' in contact else None)
        if event_id:    
            try:
                event_sender = await conn.fetchval("SELECT sender_email FROM event WHERE id = $1", event_id)
                event_sender = event_sender.strip() if event_sender and isinstance(event_sender, str) else None
                if event_sender and '@' in event_sender:
                    sender_email = event_sender
                    logger.debug(f"[CAMPAIGN] Using event sender_email for contact {contact_id}: {sender_email}")
            except Exception as e:
                logger.debug(f"[CAMPAIGN] Error fetching event sender_email for contact {contact_id}: {e}")

        if not sender_email or '@' not in str(sender_email):
            if DEFAULT_SENDER_EMAIL:
                sender_email = DEFAULT_SENDER_EMAIL
                loger.warning(f"[CAMPAIGN] Using DEFAULT_SENDER_EMAIL fallback for contact {contact_id}")
            logger.debug(f"[CAMPAIGN] Using DEFAULT_SENDER_EMAIL fallback for contact {contact_id}")
        else:
            logger.error(f"[CAMPAIGN] No valid sender_email for contact {contact_id}")  
            return

    # Acquire a transaction-scoped advisory lock per contact to prevent multiple workers
    # from deciding and queueing the same contact concurrently. We use
    # pg_try_advisory_xact_lock which is released at the end of the transaction.
    # If we can't get the lock, another worker is processing this contact; skip.
    try:
        async with conn.transaction():
            got_lock = await conn.fetchval("SELECT pg_try_advisory_xact_lock($1)", contact_id)
            if not got_lock:
                logger.debug(f"[CAMPAIGN] Contact {contact_id} is being processed by another worker; skipping")
                return

            # Re-fetch the contact row under FOR UPDATE to get the most up-to-date
            # values and to serialize status/last_triggered updates against other workers.
            locked_contact = await conn.fetchrow("SELECT * FROM campaign_contacts WHERE id = $1 FOR UPDATE", contact_id)
            if locked_contact:
                # Normalize stage to a canonical short name so downstream
                # decision logic behaves consistently when frontend stores
                # multi-word stage strings like 'rh bt payment'. We prefer
                # the canonical tokens: 'rh', 'payments', 'sepa', 'forms'.
                raw_stage = (locked_contact.get('stage') or '').lower()
                if raw_stage:
                    if 'rh' in raw_stage:
                        stage = 'rh'
                    elif re.search(r"\b(payment|payments)\b", raw_stage):
                        stage = 'payments'
                    elif 'sepa' in raw_stage:
                        stage = 'sepa'
                    elif 'forms' in raw_stage:
                        stage = 'forms'
                    else:
                        stage = raw_stage
                else:
                    stage = raw_stage
                status = locked_contact.get('status') or 'pending'
                last_triggered = locked_contact.get('last_triggered_at')
                last_message_type = locked_contact.get('last_message_type')
                sender_email = locked_contact.get('sender_email') or sender_email

            # Recompute time_since_last using the authoritative sent timestamps
            time_since_last = None
            used_reference = None
            ref_time = None
            try:
                # 1) Prefer sent emails from the current stage (avoid old-stage rows)
                if stage:
                    sent_row = await conn.fetchrow("""
                        SELECT sent_at FROM email_queue
                        WHERE contact_id = $1 AND status = 'sent' AND sent_at IS NOT NULL
                          AND last_message_type LIKE $2
                        ORDER BY sent_at DESC LIMIT 1
                    """, contact_id, f"{stage}%")
                else:
                    sent_row = await conn.fetchrow("""
                        SELECT sent_at FROM email_queue
                        WHERE contact_id = $1 AND status = 'sent' AND sent_at IS NOT NULL
                        ORDER BY sent_at DESC LIMIT 1
                    """, contact_id)

                if sent_row and sent_row.get('sent_at'):
                    ref_time = sent_row['sent_at']
                    used_reference = 'email_queue.sent_at (current stage)'
                else:
                    # 2) Use last_triggered_at (recent resume / manual action) before falling back
                    if last_triggered:
                        ref_time = last_triggered
                        used_reference = 'campaign_contacts.last_triggered_at'
                    else:
                        # 3) Last resort: use messages table (historical sent messages)
                        msg_row = await conn.fetchrow("""
                            SELECT sent_at FROM messages
                            WHERE contact_id = $1 AND direction = 'sent' AND sent_at IS NOT NULL
                            ORDER BY sent_at DESC LIMIT 1
                        """, contact_id)
                        if msg_row and msg_row.get('sent_at'):
                            ref_time = msg_row['sent_at']
                            used_reference = 'messages.sent_at'
            except Exception as e:
                logger.debug(f"[CAMPAIGN] Error fetching last sent timestamp for contact {contact_id}: {e}")
                ref_time = None

            time_since_last_seconds = None
            if ref_time:
                if getattr(ref_time, 'tzinfo', None) is not None:
                    ref_time = ref_time.replace(tzinfo=None)
                delta = (now - ref_time)
                time_since_last_seconds = delta.total_seconds()
                time_since_last = time_since_last_seconds / 86400  # days
            elif last_triggered:
                if getattr(last_triggered, 'tzinfo', None) is not None:
                    last_triggered = last_triggered.replace(tzinfo=None)
                delta = (now - last_triggered)
                time_since_last_seconds = delta.total_seconds()
                time_since_last = time_since_last_seconds / 86400  # days
                used_reference = 'campaign_contacts.last_triggered_at'
                # Log fallback usage for monitoring so we can track missing sent_at coverage
                try:
                    await conn.execute('''
                        INSERT INTO monitoring_logs (log_level, message, component, timestamp, metadata)
                        VALUES ($1, $2, $3, $4, $5)
                    ''', 'warning', f'Fallback to last_triggered_at for contact {contact_id}', 'campaign_worker', now, json.dumps({'contact_id': contact_id, 'last_triggered_at': str(last_triggered)}))
                except Exception:
                    pass

            logger.debug(f"[CAMPAIGN] Processing contact {contact_id}: stage={stage}, status={status}, last_message={last_message_type}, days_since_last={time_since_last} (based_on={used_reference})")

            # --- Prevent auto-progression immediately after resume ---
            # If a contact was recently resumed (last_triggered_at is fresh but last_message_type is NULL),
            # skip processing to allow the single-contact processor to queue the initial message first
            try:
                if last_triggered and last_message_type is None:
                    if getattr(last_triggered, 'tzinfo', None) is not None:
                        last_triggered_check = last_triggered.replace(tzinfo=None)
                    else:
                        last_triggered_check = last_triggered
                    seconds_since_resume = (now - last_triggered_check).total_seconds()
                    if seconds_since_resume < 300:  # 5 minutes
                        logger.info(f"[CAMPAIGN] Contact {contact_id} was recently resumed ({seconds_since_resume:.1f}s ago), skipping to allow initial message processing")
                        return
            except Exception as e:
                logger.debug(f"[CAMPAIGN] Error checking recent resume status for contact {contact_id}: {e}")

            # --- Prevent double-queue: check for pending reminders for this stage while holding the lock ---
            # Use a safer dual-check: only skip if a pending reminder exists AND the main stage message was already sent
            pending_exists = False
            main_sent = False
            try:
                if stage:
                    pattern = f"{stage}_reminder%"
                    pending_exists = await conn.fetchval("""
                        SELECT 1 FROM email_queue
                        WHERE contact_id = $1 AND last_message_type LIKE $2 AND status = 'pending' LIMIT 1
                    """, contact_id, pattern)

                    # Ensure main stage message was already sent (e.g., payments_initial must be sent before reminders)
                    main_sent = await conn.fetchval("""
                        SELECT 1 FROM email_queue
                        WHERE contact_id = $1 AND last_message_type = $2 AND status = 'sent' LIMIT 1
                    """, contact_id, stage)
            except Exception:
                pending_exists = False
                main_sent = False

            # Only skip if both a pending reminder exists AND the main message was already sent
            if pending_exists and main_sent:
                logger.debug(f"[CAMPAIGN] Pending reminder already exists for contact {contact_id} (stage={stage}), skipping next action")
                return

            # Determine next action while holding the lock so only one worker can queue
            next_action = determine_next_action(stage, status, last_message_type, time_since_last, time_since_last_seconds)

            if not next_action:
                return

            action_type, template_type, template_stage, new_status, trigger_text = next_action

            # Check for duplicate messages (stronger consistency under lock)
            if await check_duplicate_message(conn, contact_id, action_type):
                logger.debug(f"[CAMPAIGN] Duplicate {action_type} already exists for contact {contact_id}")
                return

            # Now call send_campaign_message; it will insert queue row and update contact under the same transaction
            try:
                # Ensure the contact passed to send_campaign_message contains event-level
                # fields (like sender_email) which may be present on the original
                # `contact` fetched with a JOIN to `event`, but missing from the
                # `locked_contact` which was selected only from `campaign_contacts`.
                merged_contact = dict(contact)
                if locked_contact:
                    try:
                        merged_contact.update(dict(locked_contact))
                    except Exception:
                        # Fallback: copy individual keys if asyncpg.Record doesn't
                        # convert directly
                        for k in locked_contact.keys():
                            merged_contact[k] = locked_contact.get(k)

                sent_ok = await send_campaign_message(conn, merged_contact, action_type, template_type, template_stage, new_status, trigger_text, now)
                if sent_ok:
                    logger.info(f"[CAMPAIGN] Queued {action_type} for contact {contact_id}")
                else:
                    logger.info(f"[CAMPAIGN] Skipped sending {action_type} to contact {contact_id} (send_campaign_message returned False)")
            except Exception as e:
                logger.error(f"[CAMPAIGN] Failed to send {action_type} to contact {contact_id}: {e}")
            return
    except Exception as e:
        logger.error(f"[CAMPAIGN] Error processing contact {contact_id} with lock: {e}")
        return

    # --- Determine next action based on stage, status, and timing ---
    # If contact has a custom flow, defer to custom flow processing
    try:
        flow_type_val = await conn.fetchval('SELECT flow_type FROM campaign_contacts WHERE id = $1', contact_id)
    except Exception:
        flow_type_val = None

    if flow_type_val == 'custom':
        # Process custom flow steps
        try:
            # Fetch active flow
            flow_row = await conn.fetchrow('SELECT id FROM custom_flows WHERE contact_id = $1 AND active = TRUE', contact_id)
            if not flow_row:
                logger.debug(f"[CUSTOM FLOW] No active custom flow for contact {contact_id}")
                return

            flow_id = flow_row['id']
            # Find last completed step number for this contact (from campaign_contacts.status or messages)
            # We'll store current step in last_message_type as 'custom-step-{n}' when sent
            # Use the transaction-locked contact row's value for last_message_type
            # rather than the outer snapshot `contact` which may be stale.
            last_msg = last_message_type or ''
            m = None
            try:
                import re as _re
                mm = _re.search(r'step-(\d+)', last_msg or '')
                if mm:
                    m = int(mm.group(1))
            except Exception:
                m = None

            last_completed = m or 0

            # Next step to run is last_completed + 1
            next_step_order = last_completed + 1
            step_row = await conn.fetchrow('SELECT * FROM custom_flow_steps WHERE flow_id = $1 AND step_order = $2', flow_id, next_step_order)
            if not step_row:
                logger.info(f"[CUSTOM FLOW] No more steps for contact {contact_id} (flow {flow_id})")
                # Optionally mark flow completed
                await conn.execute("UPDATE campaign_contacts SET status = 'custom-complete' WHERE id = $1", contact_id)
                return

            # Determine when to queue this step: only email steps create email_queue entries
            # Skip non-email steps
            if (step_row.get('type') or '').lower() != 'email':
                logger.info(f"[CUSTOM FLOW] Skipping non-email step {next_step_order} for contact {contact_id}")
                # If non-email, update status but don't enqueue; advance will be handled by other logic
                await conn.execute("UPDATE campaign_contacts SET status = $1 WHERE id = $2", f'step-{next_step_order}', contact_id)
                return

            # Determine when to queue this step: use last sent timestamp + delay_days
            # Prefer the last sent_at from email_queue or messages
            ref_time = None
            sent_row = await conn.fetchrow("SELECT sent_at FROM email_queue WHERE contact_id = $1 AND status = 'sent' ORDER BY sent_at DESC LIMIT 1", contact_id)
            if sent_row and sent_row.get('sent_at'):
                ref_time = sent_row['sent_at']
            else:
                msg_row = await conn.fetchrow("SELECT sent_at FROM messages WHERE contact_id = $1 AND direction='sent' ORDER BY sent_at DESC LIMIT 1", contact_id)
                if msg_row and msg_row.get('sent_at'):
                    ref_time = msg_row['sent_at']

            # If no ref_time, use last_triggered_at or now
            if not ref_time:
                ref_time = contact.get('last_triggered_at') or now

            # Compute when step is due
            delay_days = step_row.get('delay_days') or 0
            # If this is the first step, enqueue immediately regardless of delay
            if next_step_order == 1:
                due_time = now
            else:
                due_time = ref_time + timedelta(days=delay_days)
            if getattr(due_time, 'tzinfo', None) is not None:
                due_time = due_time.replace(tzinfo=None)

            if now >= due_time:
                # Queue the custom step into email_queue (reuse similar insertion logic)
                # Prevent duplicates by checking existing pending/sent for this custom-step
                logger.debug(f"[CUSTOM FLOW] About to check existing queue entries for contact {contact_id}, step {next_step_order}")
                existing = await conn.fetchval("SELECT 1 FROM email_queue WHERE contact_id = $1 AND last_message_type = $2 AND status IN ('pending','sent') LIMIT 1", contact_id, f'custom-step-{next_step_order}')
                logger.debug(f"[CUSTOM FLOW] Existing check returned: {existing} for contact {contact_id}, step {next_step_order}")
                if existing:
                    logger.info(f"[CUSTOM FLOW] Step {next_step_order} already queued for contact {contact_id}")
                    return

                # Build email fields
                subject = step_row.get('subject') or f'Follow-up step {next_step_order}'
                body = step_row.get('body') or ''

                cc_recipients = None
                try:
                    if contact.get('cc_store'):
                        parts = [p.strip() for p in re.split(r'[;,\s]+', contact.get('cc_store') or '') if p.strip()]
                        cc_recipients = ';'.join(parts) if parts else None
                    else:
                        parsed = process_emails(contact.get('email') or '', validate=True)
                        if parsed and len(parsed) > 1:
                            cc_recipients = ';'.join([p for p in parsed[1:]])
                except Exception:
                    cc_recipients = None

                # Custom flow steps should not include contact-level attachments by default
                try:
                    # Calculate scheduled_at with UK business hours enforcement
                    try:
                        scheduled_at_val = next_allowed_uk_business_time(due_time) if due_time else next_allowed_uk_business_time(now)
                    except Exception as e:
                        logger.warning(f"[CUSTOM FLOW] Error calculating business hours for contact {contact_id}: {e}. Using due_time as fallback.")
                        scheduled_at_val = due_time if due_time else now

                    logger.debug(f"[CUSTOM FLOW] Inserting email_queue row for contact {contact_id}, step {next_step_order}, due {due_time}, scheduled_at {scheduled_at_val}")
                    await conn.execute('''
                        INSERT INTO email_queue (contact_id, event_id, sender_email, recipient_email, cc_recipients, subject, message, last_message_type, status, created_at, due_at, scheduled_at, type, attachment, attachment_filename, attachment_mimetype)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', $9, $10, $11, $12, $13, $14, $15)
                    ''', contact_id, contact.get('event_id'), contact.get('sender_email'), contact.get('email'), cc_recipients, subject, body, f'custom-step-{next_step_order}', now, due_time, scheduled_at_val, 'custom', None, None, None)

                    # Update campaign_contacts to reflect queued step: mark as in custom flow and pending
                    await conn.execute("UPDATE campaign_contacts SET last_triggered_at = $1, last_message_type = $2, status = $3, stage = 'custom' WHERE id = $4", now, f'custom-step-{next_step_order}', 'pending', contact_id)
                    logger.info(f"[CUSTOM FLOW] Queued custom step {next_step_order} for contact {contact_id}")
                except Exception as db_e:
                    logger.error(f"[CUSTOM FLOW] Failed to insert queued step for contact {contact_id}, step {next_step_order}: {db_e}", exc_info=True)
                    # Ensure we propagate or at least record failure state
                    try:
                        await conn.execute("UPDATE campaign_contacts SET trigger = COALESCE(trigger || E'\n', '') || $1, last_triggered_at = $2 WHERE id = $3", f'ERROR: failed to queue custom-step-{next_step_order}', now, contact_id)
                    except Exception:
                        logger.debug(f"[CUSTOM FLOW] Also failed to update contact error trigger for {contact_id}")
                    return
            else:
                logger.debug(f"[CUSTOM FLOW] Step {next_step_order} not due yet for contact {contact_id} (due {due_time})")

            return
        except Exception as e:
            logger.error(f"[CUSTOM FLOW] Error processing custom flow for contact {contact_id}: {e}")
            return

    # Prevent parallel workers from queuing the next reminder if a pending reminder for this stage already exists.
    # Use safer dual-check so reminders are only skipped if the main stage message was already sent.
    pending_exists = False
    main_sent = False
    try:
        if stage:
            pattern = f"{stage}_reminder%"
            pending_exists = await conn.fetchval("""
                SELECT 1 FROM email_queue
                WHERE contact_id = $1 AND last_message_type LIKE $2 AND status = 'pending' LIMIT 1
            """, contact_id, pattern)

            # Determine canonical initial/main tokens for the stage so we can
            # correctly detect whether the stage's main message has been sent.
            if stage == 'forms':
                main_tokens = ['forms_initial', 'forms_main']
            elif stage == 'payments':
                main_tokens = ['payments_initial', 'payment_main']
            elif stage == 'sepa':
                # SEPA stage uses its own initial token
                main_tokens = ['sepa_initial', 'payments_initial', 'payment_main']
            else:
                # Generic fallback: look for '<stage>_initial' as the main token
                main_tokens = [f"{stage}_initial"]

            # Use ANY(...) to match any of the candidate tokens in the DB row
            main_sent = await conn.fetchval("""
                SELECT 1 FROM email_queue
                WHERE contact_id = $1 AND last_message_type = ANY($2::text[]) AND status = 'sent' LIMIT 1
            """, contact_id, main_tokens)
    except Exception:
        pending_exists = False
        main_sent = False

    # Only skip if a pending reminder exists AND the main stage message was already sent
    if pending_exists and main_sent:
        logger.debug(f"[CAMPAIGN] Pending reminder already exists for contact {contact_id} (stage={stage}), skipping next action")
        return

    # Use the new verification function that ensures proper message sequencing
    next_action = await determine_next_action_with_verification(conn, contact_id, stage, status, last_message_type, time_since_last, time_since_last_seconds)

    if not next_action:
        return

    action_type, template_type, template_stage, new_status, trigger_text = next_action

    # Do NOT skip queueing here due to sender cooldown. The send worker enforces
    # per-sender/domain cooldowns when picking up rows from `email_queue`.
    # If the sender is currently cooling down, we keep the message in the
    # queue (status='pending') so it will be sent automatically once the
    # cooldown expires. This avoids missing reminders when multiple workers
    # race to decide next actions.
    if not await check_sender_cooldown(conn, sender_email, now):
        logger.debug(f"[CAMPAIGN] Sender {sender_email} currently in cooldown; will still queue contact {contact_id} and let send worker handle timing")

    # Check for duplicate messages
    if await check_duplicate_message(conn, contact_id, action_type):
        logger.debug(f"[CAMPAIGN] Duplicate {action_type} already exists for contact {contact_id}")
        return

    # Send the message
    try:
        sent_ok = await send_campaign_message(conn, contact, action_type, template_type, template_stage, new_status, trigger_text, now)
        if sent_ok:
            logger.info(f"[CAMPAIGN] Sent {action_type} to contact {contact_id}")
        else:
            logger.info(f"[CAMPAIGN] Skipped sending {action_type} to contact {contact_id} (send_campaign_message returned False)")
    except Exception as e:
        logger.error(f"[CAMPAIGN] Failed to send {action_type} to contact {contact_id}: {e}")


async def determine_next_action_with_verification(conn, contact_id, stage, status, last_message_type, time_since_last, time_since_last_seconds=None):
    """
    Enhanced version that verifies prior messages were actually SENT before allowing reminders.
    This prevents sending reminder1 before initial, or sending final before gentle, etc.
    """
    
    # CRITICAL: Before allowing any reminder, verify the prior stage message was SENT
    async def verify_prior_message_sent(expected_message_type):
        """Check if a message type was actually sent (not just pending/failed)"""
        result = await conn.fetchval("""
            SELECT 1 FROM email_queue
            WHERE contact_id = $1 
            AND last_message_type = $2 
            AND status = 'sent'
            LIMIT 1
        """, contact_id, expected_message_type)
        return bool(result)
    
    # ===== FORMS STAGE =====
    if stage == 'forms':
        # Check: Has forms_initial been SENT?
        if last_message_type != 'forms_initial':
            # No prior forms message, send initial
            logger.info(f"[VERIFY] Contact {contact_id}: No prior forms message, sending forms_initial")
            return ('forms_initial', 'forms', 'initial', 'forms_initial_sent', 'Sent initial forms message')
        
        # Has forms_initial been actually sent?
        forms_initial_sent = await verify_prior_message_sent('forms_initial')
        if not forms_initial_sent:
            logger.warning(f"[VERIFY] Contact {contact_id}: forms_initial queued but NOT yet sent, skipping reminder")
            return None  # Don't send reminder yet
        
        # forms_initial was sent, check for reminder progression
        if time_since_last and time_since_last >= 2:
            reminder1_sent = await verify_prior_message_sent('forms_reminder1')
            if not reminder1_sent:
                # Reminder1 not sent yet, send it
                logger.info(f"[VERIFY] Contact {contact_id}: Sending forms_reminder1")
                return ('forms_reminder1', 'forms', 'reminder1', 'forms_reminder1_sent', 'Sent forms reminder 1')
            
            if time_since_last >= 4:  # 2 more days
                reminder2_sent = await verify_prior_message_sent('forms_reminder2')
                if not reminder2_sent:
                    logger.info(f"[VERIFY] Contact {contact_id}: Sending forms_reminder2")
                    return ('forms_reminder2', 'forms', 'reminder2', 'forms_reminder2_sent', 'Sent forms reminder 2')
                
                if time_since_last >= 7:  # 3 more days
                    reminder3_sent = await verify_prior_message_sent('forms_reminder3')
                    if not reminder3_sent:
                        logger.info(f"[VERIFY] Contact {contact_id}: Sending forms_reminder3 (final)")
                        return ('forms_reminder3', 'forms', 'reminder3', 'forms_reminder3_sent', 'Sent final forms reminder')
    
    # ===== PAYMENTS STAGE (same pattern) =====
    if stage in ('payments', 'sepa', 'rh'):
        initial_type = f"{stage}_initial"
        
        # Has the initial payment message been SENT?
        if last_message_type != initial_type:
            logger.info(f"[VERIFY] Contact {contact_id}: No prior {stage} message, sending {initial_type}")
            return (initial_type, stage, 'initial', f'{initial_type}_sent', f'Sent {stage} initial message')
        
        initial_sent = await verify_prior_message_sent(initial_type)
        if not initial_sent:
            logger.warning(f"[VERIFY] Contact {contact_id}: {initial_type} queued but NOT yet sent, skipping reminders")
            return None
        
        # Initial was sent, check reminders in order
        reminder_sequence = [
            (f"{stage}_reminder1", 'reminder1', 2),
            (f"{stage}_reminder2", 'reminder2', 2),
            (f"{stage}_reminder3", 'reminder3', 3 if stage == 'payments' else 2),
            ('payments_reminder4', 'reminder4', 7),  # All stages use payments reminders 4-6
            ('payments_reminder5', 'reminder5', 7),
            ('payments_reminder6', 'reminder6', 7),
        ]
        
        for i, (reminder_type, template_stage, interval) in enumerate(reminder_sequence):
            # Has this reminder already been sent?
            reminder_sent = await verify_prior_message_sent(reminder_type)
            if reminder_sent:
                logger.debug(f"[VERIFY] Contact {contact_id}: {reminder_type} already sent, checking next")
                continue  # Skip to next
            
            # Get prior message type to check timing
            if i == 0:
                prior_type = initial_type
            else:
                prior_type = reminder_sequence[i-1][0]
            
            prior_sent = await verify_prior_message_sent(prior_type)
            
            # Only progress if prior was sent AND enough time has passed
            if prior_sent and time_since_last and time_since_last >= interval:
                logger.info(f"[VERIFY] Contact {contact_id}: Sending {reminder_type} (interval {interval} days met)")
                return (reminder_type, stage, template_stage, f'{reminder_type}_sent', f'Sent {stage} {template_stage}')
            else:
                logger.debug(f"[VERIFY] Contact {contact_id}: {reminder_type} not yet due (prior_sent={prior_sent}, time_since_last={time_since_last}, interval={interval})")
                break  # Don't skip ahead in the sequence
    
    # ===== INITIAL CAMPAIGN MESSAGE AND REMINDERS =====
    # Handle error state - retry the last failed message after 1 hour
    if last_message_type == 'error':
        if time_since_last and time_since_last >= (1/24):  # 1 hour cooldown for retries
            logger.info(f"[VERIFY] Contact {contact_id}: Retrying after error")
            # Get the last successful message type before the error
            return ('campaign_main', 'campaign', 'default', 'first_message_sent', 'Retrying initial campaign message')
        return None  # Still in cooldown period
    
    # Initial campaign message (no previous messages)
    if not last_message_type and status == 'pending':
        logger.info(f"[VERIFY] Contact {contact_id}: No messages sent yet, sending campaign_main")
        return ('campaign_main', 'campaign', 'default', 'first_message_sent', 'Sent initial campaign message')
    
    # First message reminders (3 days, then 4 days)
    if last_message_type == 'campaign_main':
        campaign_main_sent = await verify_prior_message_sent('campaign_main')
        if not campaign_main_sent:
            logger.warning(f"[VERIFY] Contact {contact_id}: campaign_main queued but NOT yet sent, skipping reminder")
            return None
        
        if time_since_last and time_since_last >= 3:
            reminder1_sent = await verify_prior_message_sent('reminder1')
            if not reminder1_sent:
                logger.info(f"[VERIFY] Contact {contact_id}: Sending reminder1 (3 days after initial)")
                return ('reminder1', 'reminder', 'reminder1', 'first_reminder', 'Sent first reminder (3 days after initial)')
    
    if last_message_type == 'reminder1':
        reminder1_sent = await verify_prior_message_sent('reminder1')
        if not reminder1_sent:
            logger.warning(f"[VERIFY] Contact {contact_id}: reminder1 queued but NOT yet sent, skipping reminder2")
            return None
        
        if time_since_last and time_since_last >= 4:  # 4 days after first reminder (7 days total)
            reminder2_sent = await verify_prior_message_sent('reminder2')
            if not reminder2_sent:
                logger.info(f"[VERIFY] Contact {contact_id}: Sending reminder2 (7 days after initial)")
                return ('reminder2', 'reminder', 'reminder2', 'second_reminder', 'Sent second reminder (7 days after initial)')
    
    return None


def determine_next_action(stage, status, last_message_type, time_since_last, time_since_last_seconds=None):
    """Determine what action to take based on current state and timing"""

    # Prevent quick re-evaluation from immediately advancing reminders due to race conditions.
    # If the last action was less than 60 seconds ago, skip deciding a next action now.
    try:
        if (time_since_last_seconds is not None and time_since_last_seconds < 60 
            and action_type == 'campaign_main'):
            return None
    except Exception:
        pass

    # If customer has replied, don't send any more automated messages
    if status == 'replied':
        return None

    # Normalize legacy or variant message/status names so timing logic is robust.
    # Historical data sometimes uses 'forms_main' instead of 'forms_initial',
    # or stores statuses without the '_sent' suffix. Map common variants to
    # the canonical names used below.
    message_type_aliases = {
        'forms_main': 'forms_initial',
        'forms_main_sent': 'forms_initial_sent',
    }

    status_aliases = {
        'forms_main': 'forms_initial_sent',
        'forms_main_sent': 'forms_initial_sent',
        # if any code wrote 'forms' without the '_sent' suffix, treat it as sent
        'forms': 'forms_initial_sent'
    }

    # Apply aliases if present
    if last_message_type in message_type_aliases:
        last_message_type = message_type_aliases[last_message_type]
    if status in status_aliases:
        status = status_aliases[status]

    # Auto-Inference: If status is 'pending', look at last_message_type to infer actual position
    # This handles cases where a contact recovered from a failed state or stage change
    inferred_status = None
    if status == 'pending' and last_message_type:
        inferred_status = inferred_status_map.get(last_message_type)
        if inferred_status:
            logger.info(f"[STATUS_INFERENCE] Inferring status '{inferred_status}' from last_message_type '{last_message_type}'")
            status = inferred_status



    # Handle error state - retry the last failed message after 1 hour
    if last_message_type == 'error':
        if time_since_last and time_since_last >= (1/24):  # 1 hour cooldown for retries
            # Get the last successful message type before the error
            last_successful_message = status  # Status usually tracks the last successful message

            # Map the status back to the message type that needs to be retried
            message_type_map = {
                'first_message_sent': ('campaign_main', 'campaign', 'default'),
                'first_reminder': ('reminder1', 'reminder', 'reminder1'),
                'second_reminder': ('reminder2', 'reminder', 'reminder2'),
                'forms_initial_sent': ('forms_initial', 'forms', 'initial'),
                'forms_reminder1_sent': ('forms_reminder1', 'forms', 'reminder1'),
                'forms_reminder2_sent': ('forms_reminder2', 'forms', 'reminder2'),
                'payments_initial_sent': ('payments_initial', 'payments', 'initial'),
                'payments_reminder1_sent': ('payments_reminder1', 'payments', 'reminder1'),
                'payments_reminder2_sent': ('payments_reminder2', 'payments', 'reminder2'),
                'payments_reminder3_sent': ('payments_reminder3', 'payments', 'reminder3'),
                'payments_reminder4_sent': ('payments_reminder4', 'payments', 'reminder4'),
                'payments_reminder5_sent': ('payments_reminder5', 'payments', 'reminder5'),
                'payments_reminder6_sent': ('payments_reminder6', 'payments', 'reminder6'),
            }

            if status in message_type_map:
                action_type, template_type, template_stage = message_type_map[status]
                return (action_type, template_type, template_stage, status, f'Retrying failed {action_type} message')

            # Default fallback if status doesn't match any known state
            return ('campaign_main', 'campaign', 'default', 'first_message_sent', 'Retrying initial campaign message')
        return None  # Still in cooldown period

    # Initial campaign message (no previous messages)
    if not last_message_type and status == 'pending':
        return ('campaign_main', 'campaign', 'default', 'first_message_sent', 'Sent initial campaign message')

    # First message reminders (3 days, then 4 days)
    if last_message_type == 'campaign_main' and status == 'first_message_sent':
        if time_since_last and time_since_last >= 3:
            return ('reminder1', 'reminder', 'reminder1', 'first_reminder', 'Sent first reminder (3 days after initial)')

    if last_message_type == 'reminder1' and status == 'first_reminder':
        if time_since_last and time_since_last >= 4:  # 4 days after first reminder (7 days total)
            return ('reminder2', 'reminder', 'reminder2', 'second_reminder', 'Sent second reminder (7 days after initial)')

    # Forms stage processing - only if stage is explicitly set to 'forms'
    if stage == 'forms':
        
        
        if not (status and isinstance(status, str) and status.startswith('forms_')) and not (last_message_type and isinstance(last_message_type, str) and last_message_type.startswith('forms_')):
            return ('forms_initial', 'forms', 'initial', 'forms_initial_sent', 'Sent initial forms message')

        # Robust reminder progression: derive next reminder from the canonical status when possible.
        # Only consider reminder progression when status explicitly indicates
        # a prior forms_* has been sent. This avoids progressing to a reminder
        # based on unrelated previous message types.
        try:
            if status and isinstance(status, str) and status.startswith('forms_') and status.endswith('_sent'):
                # forms_initial_sent -> forms_reminder1 (2 days)
                if status in ('forms_initial_sent', 'forms_main', 'forms_main_send'):
                    if time_since_last and time_since_last >= 2:
                        # Ensure we only transition from the initial forms message, not from reminders
                        is_later_reminder = last_message_type and 'reminder' in last_message_type

                        if not is_later_reminder:
                            return ('forms_reminder1', 'forms', 'reminder1', 'forms_reminder1_sent', 'Sent forms reminder 1 (2 days after initial)')
                        else:
                            logger.debug(f"[DETERMINE_NEXT] Forms initial status but last_message_type={last_message_type} not in expected list, falling through to legacy checks")

                # forms_reminder1_sent -> forms_reminder2 (2 days)
                m = re.match(r'^forms_reminder(\d+)_sent$', status or '')
                if m:
                    idx = int(m.group(1))
                    # Only allow sequential increment and cap at 3
                    if idx >= 1 and idx < 3:
                        # mapping for delay days to next reminder
                        delay_map = {1: 2, 2: 3}
                        needed = delay_map.get(idx, 2)
                        if time_since_last and time_since_last >= needed:
                            next_idx = idx + 1
                            return (f'forms_reminder{next_idx}', 'forms', f'reminder{next_idx}', f'forms_reminder{next_idx}_sent', f'Sent forms reminder {next_idx} (advanced)')
                        else:
                            logger.debug(f"[DETERMINE_NEXT] Forms reminder{idx}_sent status but time_since_last={time_since_last} < needed={needed}")
            else:
                logger.debug(f"[DETERMINE_NEXT] Forms stage status check failed: status={status}, starts_with_forms={status and isinstance(status, str) and status.startswith('forms_')}, ends_with_sent={status and isinstance(status, str) and status.endswith('_sent')}")

        except Exception as e:
            # Fall back to original conservative checks below if anything unexpected occurs
            logger.debug(f"[DETERMINE_NEXT] Exception in forms status-based logic: {e}")
            pass

        # Fallback: legacy per-last_message_type checks (keeps previous behaviour as safety net)
        if last_message_type == 'forms_initial' and status == 'forms_initial_sent':
            if time_since_last and time_since_last >= 2:
                return ('forms_reminder1', 'forms', 'reminder1', 'forms_reminder1_sent', 'Sent forms reminder 1 (2 days after initial)')

        if last_message_type == 'forms_reminder1' and status == 'forms_reminder1_sent':
            if time_since_last and time_since_last >= 2:
                return ('forms_reminder2', 'forms', 'reminder2', 'forms_reminder2_sent', 'Sent forms reminder 2 (4 days after initial)')

        if last_message_type == 'forms_reminder2' and status == 'forms_reminder2_sent':
            if time_since_last and time_since_last >= 3:
                return ('forms_reminder3', 'forms', 'reminder3', 'forms_reminder3_sent', 'Sent final forms reminder (7 days after initial)')

    # Payments, SEPA and RH stage processing - handle all in one flow
    if stage in ('payments', 'sepa', 'rh'):
        # If there is no prior evidence of any payments/sepa-related activity
        # prefer sending the appropriate initial message for the stage.
        # Include RH tokens when detecting prior payment activity so we don't
        # accidentally re-send an initial payment for RH contacts.
        if not (status and isinstance(status, str) and (status.startswith('payments_') or status.startswith('sepa_') or status.startswith('rh_'))) and not (last_message_type and isinstance(last_message_type, str) and (last_message_type.startswith('payments_') or last_message_type.startswith('sepa_') or last_message_type.startswith('rh_'))):
            if stage == 'sepa':
                return ('sepa_initial', 'sepa', 'initial', 'sepa_initial_sent', 'Sent SEPA initial payment message')
            if stage == 'rh':
                return ('rh_initial', 'rh', 'initial', 'rh_initial_sent', 'Sent RH initial payment message')
            return ('payments_initial', 'payments', 'initial', 'payments_initial_sent', 'Sent initial payment message')

        # Define reminder sequences. SEPA uses 2-day gaps for the first 3 reminders,
        # then falls back to the payments 4..6 cadence. Payments use the original cadence.
        if stage == 'sepa' or stage == 'rh':
            reminder_configs = [
                ('%s_reminder1' % stage, 'reminder1', 2, f'{stage}_reminder1_sent'),
                ('%s_reminder2' % stage, 'reminder2', 2, f'{stage}_reminder2_sent'),
                ('%s_reminder3' % stage, 'reminder3', 2, f'{stage}_reminder3_sent'),
                ('payments_reminder4', 'reminder4', 7, 'payments_reminder4_sent'),
                ('payments_reminder5', 'reminder5', 7, 'payments_reminder5_sent'),
                ('payments_reminder6', 'reminder6', 7, 'payments_reminder6_sent')
            ]
        else:
            reminder_configs = [
                ('payments_reminder1', 'reminder1', 2, 'payments_reminder1_sent'),
                ('payments_reminder2', 'reminder2', 2, 'payments_reminder2_sent'),
                ('payments_reminder3', 'reminder3', 3, 'payments_reminder3_sent'),
                ('payments_reminder4', 'reminder4', 7, 'payments_reminder4_sent'),
                ('payments_reminder5', 'reminder5', 7, 'payments_reminder5_sent'),
                ('payments_reminder6', 'reminder6', 7, 'payments_reminder6_sent')
            ]

        # Check each reminder in sequence and ensure the previous step was the
        # most recent payments/sepa activity before progressing.
        for i, (msg_type, template_stage, interval, next_status) in enumerate(reminder_configs):
            # The expected previous type for the first reminder should be the
            # initial token for the current stage (payments_initial/sepa_initial/rh_initial)
            prev_type = reminder_configs[i-1][0] if i > 0 else (f"{stage}_initial")
            prev_status = reminder_configs[i-1][3] if i > 0 else (f"{stage}_initial_sent")

            prev_type_candidates = [prev_type]
            prev_status_candidates = [prev_status]
            # Keep legacy aliases for the payments initial token
            if prev_type == 'payments_initial':
                prev_type_candidates.append('payment_main')
            if prev_status == 'payments_initial_sent':
                prev_status_candidates.append('payment_main')

            # Only progress when last activity matches the expected previous token
            if (last_message_type in prev_type_candidates) and (status in prev_status_candidates):
                if time_since_last and time_since_last >= interval:
                    # Choose template type: sepa reminders use 'sepa' templates for sepa_reminderX
                    template_type_choice = 'sepa' if msg_type.startswith('sepa_') else 'payments'
                    return (msg_type, template_type_choice, template_stage, next_status,
                           f'Sent payment reminder {i+1} ({interval} days after previous message)')

    return None


async def check_sender_cooldown(conn, sender_email, now):
    """Check if sender is in cooldown period"""
    # Helper: extract domain from email
    def get_sender_domain(email: str) -> Optional[str]:
        if not email or '@' not in email:
            return None
        return email.split('@', 1)[1].lower()

    # Prefer domain-level stats stored as sender_email = 'domain:example.com'
    domain = get_sender_domain(sender_email)
    domain_key = f"domain:{domain}" if domain else None

    sender_stats = None
    if domain_key:
        sender_stats = await conn.fetchrow('SELECT last_sent, cooldown FROM sender_stats WHERE sender_email = $1', domain_key)

    if not sender_stats:
        sender_stats = await conn.fetchrow('SELECT last_sent, cooldown FROM sender_stats WHERE sender_email = $1', sender_email)

    # Default domain cooldown to 90 seconds if not set
    default_cooldown = int(os.getenv('DOMAIN_COOLDOWN_SECONDS', '90'))
    cooldown_seconds = sender_stats['cooldown'] if sender_stats and sender_stats.get('cooldown') else default_cooldown

    # Enforce minimum cooldown of 30 seconds and maximum of 300 seconds
    cooldown_seconds = max(30, min(300, int(cooldown_seconds)))

    if sender_stats and sender_stats.get('last_sent'):
        last_sent = sender_stats['last_sent']
        if getattr(last_sent, 'tzinfo', None) is not None:
            last_sent = last_sent.replace(tzinfo=None)
        elapsed = (now - last_sent).total_seconds()
        if elapsed < cooldown_seconds:
            return False

    return True


async def check_duplicate_message(conn, contact_id, message_type):
    """Check if message type already exists for contact"""
    now = datetime.now(UTC).replace(tzinfo=None)
    one_hour_ago = now - timedelta(hours=1)

    # For forms_reminder family, only check pending with same exact type
    if message_type and message_type.startswith('forms_reminder'):
        existing = await conn.fetchval("""
            SELECT 1 FROM email_queue
            WHERE contact_id = $1
            AND last_message_type = $2  -- Exact match only
            AND status = 'pending'
            AND created_at > $3  -- Only recent pending
            LIMIT 1
        """, contact_id, message_type, one_hour_ago)
    else:
        # For non-reminder types, check both pending and sent
        existing = await conn.fetchval("""
            SELECT 1 FROM email_queue
            WHERE contact_id = $1
            AND last_message_type = $2  -- Exact match
            AND status IN ('sent', 'pending')
            LIMIT 1
        """, contact_id, message_type)
    
    return bool(existing)

async def send_campaign_message(conn, contact, action_type, template_type, template_stage, new_status, trigger_text, now):
    """
    Send a campaign message and update contact status with proper threading

    Args:
        conn: Database connection
        contact: Dictionary containing contact information
        action_type: Type of action (e.g., 'initial', 'reminder1', 'reminder2', 'forms_initial', etc.)
        template_type: Type of template to use (e.g., 'campaign', 'reminder', 'forms', 'payment')
        template_stage: Stage of the template (e.g., 'initial', 'reminder1', 'reminder2')
        new_status: New status to set for the contact
        trigger_text: Text to log in the trigger column
        now: Current timestamp
    """
    # Defensive access: contact may be an asyncpg.Record or dict and may not
    # contain all event-level fields (e.g. sender_email). Use .get() and
    # validate presence rather than risking KeyError.
    contact_id = contact.get('id') if hasattr(contact, 'get') else contact['id'] if 'id' in contact else None
    sender_email = contact.get('sender_email') if hasattr(contact, 'get') else (contact['sender_email'] if 'sender_email' in contact else None)
    recipient_email = contact.get('email') if hasattr(contact, 'get') else (contact['email'] if 'email' in contact else None)

    if not contact_id:
        logger.error(f"[VALIDATION] Contact record missing 'id' field: {contact}")
        return False

    # 1. Ensure all required fields are present and not empty
    required_fields = ['name', 'email', 'sender_email']
    missing_fields = [field for field in required_fields if not (contact.get(field) if hasattr(contact, 'get') else contact.get(field) if isinstance(contact, dict) else None)]
    # For clarity, if sender_email is missing, include the DEFAULT_SENDER_EMAIL fallback
    if missing_fields:
        # If only sender_email missing, try default constant before failing
        if missing_fields == ['sender_email'] and DEFAULT_SENDER_EMAIL:
            sender_email = DEFAULT_SENDER_EMAIL
            missing_fields = []

    if missing_fields:
        logger.error(f"[VALIDATION] Missing required fields {missing_fields} for contact {contact_id}")
        return False

    # 2. Check for duplicate message with stronger consistency and broader message type matching
    duplicate = await conn.fetchrow("""
        SELECT id, status
        FROM email_queue
        WHERE contact_id = $1
        AND last_message_type = $2  
        AND status IN ('pending', 'sent')
        ORDER BY created_at DESC
        LIMIT 1
        FOR UPDATE OF email_queue SKIP LOCKED
    """, contact_id, action_type)

    if duplicate:
        logger.warning(f"[DUPLICATE] Skipping {action_type} for contact {contact_id} - found existing {action_type} with status: {duplicate['status']}")
        if duplicate['status'] == 'pending':
            await conn.execute(
                "UPDATE email_queue SET status = 'skipped', "
                "error_message = 'Duplicate message detected' "
                "WHERE id = $1",
                duplicate['id']
            )
        return False

    # 3. Get threading information from the first message in the conversation
    thread_info = await conn.fetchrow('''
        SELECT conversation_id, message_id, subject FROM email_queue
        WHERE contact_id = $1 AND status = 'sent'
        ORDER BY sent_at ASC LIMIT 1
    ''', contact_id)

    # Check if this is a stage change message
    is_stage_change = template_type in ['forms', 'payments']

    # If this is the first message, we won't have thread info
    is_first_message = thread_info is None

    # Get threading info for reply detection, but don't use it for email composition
    # This allows us to track conversations while sending individual emails
    conversation_id = thread_info['conversation_id'] if thread_info else None
    in_reply_to = thread_info['message_id'] if thread_info else None
    original_subject = thread_info['subject'] if thread_info else None

    # 4. Load and validate templates
    try:
        # Ensure all required template variables are present
        required_vars = ['name']  # Add other required variables here
        for var in required_vars:
            if not contact.get(var):
                raise ValueError(f"Missing required template variable: {var}")

        # Load templates
        # If template_stage contains a reminder (e.g. 'reminder2') pass it as reminder_type
        if template_stage and template_stage.startswith('reminder'):
            subject_template = load_template(template_type, 'subject', reminder_type=template_stage)
            body_template = load_template(template_type, 'body', reminder_type=template_stage)
        else:
            subject_template = load_template(template_type, 'subject', stage=template_stage)
            body_template = load_template(template_type, 'body', stage=template_stage)

        # Prepare context with contact data and metadata
        context = {
            **contact,  # All contact fields
            'action_type': action_type,
            'template_type': template_type,
            'template_stage': template_stage,
            'current_time': now.isoformat(),
            'trigger_details': trigger_text,
            'date': now.strftime('%Y-%m-%d'),
            'time': now.strftime('%H:%M:%S')
        }

        # Expose invoice_number under friendly names for templates
        try:
            invoice_val = None
            if isinstance(contact, dict):
                invoice_val = contact.get('invoice_number') or contact.get('invoice')
            else:
                invoice_val = getattr(contact, 'invoice_number', None) or getattr(contact, 'invoice', None)
            context['invoice_number'] = invoice_val
            if 'invoice' not in context:
                context['invoice'] = invoice_val
        except Exception:
            pass

        # Add name parts for templates with prefix awareness
        # Path A: If prefix exists (Mr., Dr.) -> returns ("Mr.", "Ayman")
        #         Template: "Dear {{prefix}} {{last_name}}" → "Dear Prof. Ayman"
        # Path B: If no prefix -> returns ("Hatem", "Ayman")
        #         Template: "Dear {{greeting_name}}" → "Dear Hatem Ayman"
        raw_name = contact.get('name', '').strip()
        # CRITICAL: Capture the DB prefix safely
        db_prefix = contact.get('prefix', '').strip() if contact.get('prefix') else ""

        # 2. Initialize defaults
        final_prefix = ""
        final_last_name = raw_name

        # 3. Decision Logic
        if db_prefix:
            # --- CASE A: Database Prefix Exists (e.g. "Dr", "Prof") ---
            # Use the DB prefix, and split the name to get the valid Last Name
            final_prefix = db_prefix
            
            # Fix Punctuation (Add dot if missing, skip "Sir")
            if len(final_prefix) <= 3 and not final_prefix.endswith('.') and final_prefix.lower() not in ["sir", "madam"]:
                    final_prefix += "."

            # Extract "Low" from "test3, low" or "Kim" from "Test4 Kim"
            if ',' in raw_name:
                final_last_name = raw_name.split(',')[-1].strip()
            else:
                parts = raw_name.split()
                final_last_name = parts[-1].strip() if parts else raw_name
        else:
            # --- CASE B: No Database Prefix ---
            # Auto-extract everything from the name string
            try:
                final_prefix, final_last_name = extract_name_parts_with_prefix(raw_name)
            except Exception as e:
                logger.error(f"Extraction error for contact {contact_id}: {e}")
                final_prefix = ""
                final_last_name = raw_name

        # 4. Capitalize and Format
        final_prefix = final_prefix.title() if final_prefix else ""
        final_last_name = final_last_name.title()

        # 5. Construct Greeting Name
        if final_prefix:
            # Result: "Prof. Kim"
            greeting_name = f"{final_prefix} {final_last_name}"
        else:
            # Result: "Hatem Ayman"
            greeting_name = final_last_name

        # 6. Update Context (Overwrite with corrected values)
        context['prefix'] = final_prefix
        context['last_name'] = final_last_name
        context['greeting_name'] = greeting_name
        
        # For templates that use {{name}}, set it to greeting_name
        context['name'] = greeting_name 
        
        # For legacy support
        context['first_name'] = final_prefix if final_prefix else (final_last_name.split()[0] if ' ' in final_last_name else final_last_name)

        # Log the name extraction for debugging
        logger.info(f"[NAME EXTRACTION] Contact {contact_id}: raw_name='{raw_name}' -> prefix='{final_prefix}', last_name='{final_last_name}', greeting_name='{greeting_name}'")
        logger.debug(f"[CONTEXT VALUES] greeting_name='{context.get('greeting_name')}', last_name='{context.get('last_name')}', first_name='{context.get('first_name')}'")

        # Log the name extraction for debugging
        logger.info(f"[NAME EXTRACTION] Contact {contact_id}: raw_name='{contact.get('name')}' -> prefix='{prefix}', last_name='{last_name}', greeting_name='{context['greeting_name']}'")
        logger.debug(f"[CONTEXT VALUES] greeting_name='{context.get('greeting_name')}', last_name='{context.get('last_name')}', first_name='{context.get('first_name')}'")

        # Render templates with strict validation
        subject = render_template_strict(subject_template, context).strip()
        body = render_template_strict(body_template, context)

        # SUBJECT POLICY: Always use the template subject for outgoing mail.
        # For stage changes (forms/payments) do NOT add a stage prefix; use the
        # rendered template subject verbatim so templates control the exact text.
        if is_stage_change:
            logger.info(f"[SUBJECT] Using template subject for stage change message: {subject}")
        else:
            # Use the rendered template subject as-is for both first messages and reminders
            logger.info(f"[SUBJECT] Using template subject for message_type={action_type}: {subject}")
            logger.info(f"[THREADING] Using template subject for first message: {subject}")

        # Ensure subject is never empty
        if not subject or not subject.strip():
            subject = original_subject or "Important: Follow-up regarding your reservation"
            logger.warning(f"[WARNING] Empty subject detected, using fallback: {subject}")

        # Clean up the subject by removing any extra whitespace
        subject = ' '.join(subject.split())

        # Create detailed trigger text with timestamp and action details
        detailed_trigger = f"{now.strftime('%Y-%m-%d %H:%M:%S')} - {action_type.upper()}: {trigger_text}"

    except Exception as e:
        error_msg = f"Template rendering failed for contact {contact_id} (action: {action_type}): {str(e)}"
        logger.error(f"[CAMPAIGN] {error_msg}")
        # Update trigger with error information
        error_trigger = f"{now.strftime('%Y-%m-%d %H:%M:%S')} - ERROR in {action_type.upper()}: {str(e)}"
        await conn.execute("""
            UPDATE campaign_contacts
            SET trigger = COALESCE(trigger || E'\n', '') || $1,
                last_triggered_at = $2,
                last_message_type = 'error',
                last_error = $3
            WHERE id = $4
        """, error_trigger, now, str(e), contact_id)
        return False

        # 5. Determine CCs: IMPORTANT - do NOT use cc_store for sending.
        # cc_store is storage-only. For sending, only parse additional addresses
        # embedded in the `email` field (legacy behavior).
        cc_emails = None
        parsed = process_emails(contact.get('email') or '', validate=True)
        if parsed and len(parsed) > 1:
            cc_emails = parsed[1:]

        # 6. Queue the email with threading information
    try:
        async with conn.transaction():
            # First, insert into email_queue
            # Determine cc_recipients: prefer explicit cc_store, fallback to extra addresses in email field
            cc_recipients = None
            try:
                if contact.get('cc_store'):
                    parts = [p.strip() for p in re.split(r'[;,\s]+', contact.get('cc_store') or '') if p.strip()]
                    cc_recipients = ';'.join(parts) if parts else None
                else:
                    parsed = process_emails(contact.get('email') or '', validate=True)
                    if parsed and len(parsed) > 1:
                        cc_recipients = ';'.join([p for p in parsed[1:]])
            except Exception:
                cc_recipients = None

            # Compute due_time for this queued message. For reminder messages we
            # persist the real due timestamp so the send worker will skip rows
            # until they are actually due. Default is now (immediate send).
            try:
                # Mapping of required days since last send per message type
                delay_map = {
                    'reminder1': 3,
                    'reminder2': 4,
                    'forms_initial': 0,
                    'forms_reminder1': 2,
                    'forms_reminder2': 2,
                    'forms_reminder3': 3,
                    'payments_initial': 0,
                    'payments_reminder1': 2,
                    'payments_reminder2': 2,
                    'payments_reminder3': 3,
                    'payments_reminder4': 7,
                    'payments_reminder5': 7,
                    'payments_reminder6': 7,
                    # SEPA reminders: initial -> reminder1 (send main then wait 2 days), reminder2 after 2 days, reminder3 after 2 days
                    'sepa_initial': 0,
                    'sepa_reminder1': 2,
                    'sepa_reminder2': 2,
                    'sepa_reminder3': 2,
                    # RH reminders mirror SEPA: initial immediate, first 3 reminders every 2 days
                    'rh_initial': 0,
                    'rh_reminder1': 2,
                    'rh_reminder2': 2,
                    'rh_reminder3': 2,
                }
                required_days = delay_map.get(action_type)
                due_time = now
                if required_days is not None:
                    # Find the most recent sent_at to anchor the delay
                    ref_row = await conn.fetchrow("""
                        SELECT sent_at FROM email_queue
                        WHERE contact_id = $1 AND status = 'sent' AND sent_at IS NOT NULL
                        ORDER BY sent_at DESC LIMIT 1
                    """, contact_id)
                    if ref_row and ref_row.get('sent_at'):
                        ref_time = ref_row['sent_at']
                    else:
                        msg_row = await conn.fetchrow("""
                            SELECT sent_at FROM messages
                            WHERE contact_id = $1 AND direction = 'sent' AND sent_at IS NOT NULL
                            ORDER BY sent_at DESC LIMIT 1
                        """, contact_id)
                        if msg_row and msg_row.get('sent_at'):
                            ref_time = msg_row['sent_at']
                        else:
                            lr = await conn.fetchrow('SELECT last_triggered_at FROM campaign_contacts WHERE id = $1', contact_id)
                            ref_time = lr['last_triggered_at'] if lr and lr.get('last_triggered_at') else None

                    if ref_time:
                        if getattr(ref_time, 'tzinfo', None) is not None:
                            ref_time = ref_time.replace(tzinfo=None)
                        due_time = ref_time + timedelta(days=required_days)
                    else:
                        # No reference time available; schedule relative to now
                        due_time = now + timedelta(days=required_days)

                # Normalize tzinfo
                if getattr(due_time, 'tzinfo', None) is not None:
                    due_time = due_time.replace(tzinfo=None)
            except Exception:
                due_time = now

            # --- UK BUSINESS HOURS ENFORCEMENT ---
            # Calculate scheduled_at: the earliest we're allowed to send this email
            # scheduled_at = max(due_at, next_allowed_uk_business_time(now))
            # This ensures both reminder delays AND business hours are respected
            try:
                next_biz_time = next_allowed_uk_business_time(now)
                # scheduled_at is the later of: due_time or next_biz_time
                if due_time and getattr(due_time, 'tzinfo', None) is not None:
                    due_time = due_time.replace(tzinfo=None)
                scheduled_at = max(due_time, next_biz_time) if due_time else next_biz_time
                logger.debug(f"[BUSINESS_HOURS] Contact {contact_id}, action {action_type}: due_time={due_time}, next_biz={next_biz_time}, scheduled_at={scheduled_at}")
            except Exception as e:
                logger.warning(f"[BUSINESS_HOURS] Error calculating business hours for contact {contact_id}: {e}. Using due_time as fallback.")
                scheduled_at = due_time

            queue_result = await conn.fetchval("""
                INSERT INTO email_queue (
                    contact_id, sender_email, recipient_email, cc_recipients, subject, message,
                    last_message_type, status, created_at, due_at, scheduled_at, type,
                    conversation_id, in_reply_to,
                    forms_link, payment_link, message_type, attachment, attachment_filename, attachment_mimetype
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
                RETURNING id
            """,
                contact_id, sender_email, recipient_email, cc_recipients, subject, body,
                action_type, now, due_time, scheduled_at, action_type, conversation_id, in_reply_to,
                contact.get('forms_link'), contact.get('payment_link'), action_type,
                # Attach only for payments or payment reminders
                (contact.get('attachment') if ((template_type == 'payments') or (template_stage and template_stage.startswith('reminder') and template_type == 'payments') or (action_type and action_type.startswith('reminder') and template_type == 'payments')) else None),
                (contact.get('attachment_filename') if ((template_type == 'payments') or (template_stage and template_stage.startswith('reminder') and template_type == 'payments') or (action_type and action_type.startswith('reminder') and template_type == 'payments')) else None),
                (contact.get('attachment_mimetype') if ((template_type == 'payments') or (template_stage and template_stage.startswith('reminder') and template_type == 'payments') or (action_type and action_type.startswith('reminder') and template_type == 'payments')) else None)
            )

            if not queue_result:
                raise Exception("Failed to insert into email_queue")

            # Update contact status and tracking with only existing columns
            update_result = await conn.execute("""
                UPDATE campaign_contacts
                SET status = $1,
                    last_triggered_at = $2,
                    last_message_type = $3,
                    last_action_time = $2,
                    trigger = COALESCE(trigger || E'\n', '') || $4
                WHERE id = $5
            """, new_status, now, action_type, detailed_trigger, contact_id)

            if not update_result:
                raise Exception(f"Failed to update contact {contact_id} after queueing email")

            logger.info(f"[CAMPAIGN] Queued {action_type} for contact {contact_id} "
                      f"(status: {new_status}, conversation_id={conversation_id}, scheduled_at={scheduled_at})")
            return True


    except Exception as e:
        error_msg = f"Failed to queue email for contact {contact_id}: {str(e)}"
        logger.error(f"[CAMPAIGN] {error_msg}", exc_info=True)

        # Update error information with only existing columns
        error_trigger = f"{now.strftime('%Y-%m-%d %H:%M:%S')} - ERROR in {action_type.upper()}: {str(e)}"

        try:
            await conn.execute("""
                UPDATE campaign_contacts
                SET trigger = COALESCE(trigger || E'\n', '') || $1,
                    last_triggered_at = $2,
                    last_message_type = 'error',
                    last_action_time = $2
                WHERE id = $3
            """, error_trigger, now, contact_id)
        except Exception as update_error:
            logger.error(f"[CRITICAL] Failed to update error status for contact {contact_id}: {update_error}")

        return False

    # No additional code should be here - this function is complete

    return True

# --- Process single contact campaign (for immediate stage changes) ---
async def process_single_contact_campaign(contact_id: int):
    """Process campaign for a single contact immediately (used for stage changes)"""
    try:
        logger.info(f"[SINGLE CONTACT] Starting immediate processing for contact {contact_id}")

        async with db_pool.acquire() as conn:
            # Fetch the specific contact with event info
            contact = await conn.fetchrow('''
                SELECT c.*,
                       e.sender_email, e.org_name, e.city, e.month, e.date2, e.venue
                FROM campaign_contacts c
                JOIN event e ON c.event_id = e.id
                WHERE c.id = $1
            ''', contact_id)

            if not contact:
                logger.info(f"[SINGLE CONTACT] Contact {contact_id} not found or not eligible for processing")
                return

            # Check if contact is paused or in terminal state
            if contact['campaign_paused']:
                logger.info(f"[SINGLE CONTACT] Contact {contact_id} is paused, skipping")
                return

            if contact['status'] in ('completed', 'cancelled', 'Replied'):
                logger.info(f"[SINGLE CONTACT] Contact {contact_id} is in terminal status '{contact['status']}', skipping")
                return

            sender_email = contact['sender_email']
            if not sender_email:
                logger.warning(f"[SINGLE CONTACT] No sender email for contact {contact_id}")
                return

            # Normalize incoming stage string (accepts multi-word variants)
            raw_stage = (contact['stage'] or '').lower()
            if raw_stage:
                if 'rh' in raw_stage:
                    stage = 'rh'
                elif re.search(r"\b(payment|payments)\b", raw_stage):
                    stage = 'payments'
                elif 'sepa' in raw_stage:
                    stage = 'sepa'
                elif 'forms' in raw_stage:
                    stage = 'forms'
                else:
                    stage = raw_stage
            else:
                stage = raw_stage
            status = contact['status']
            now = datetime.now(UTC).replace(tzinfo=None)

            logger.info(f"[SINGLE CONTACT] Processing contact {contact_id} in stage '{stage}' with status '{status}'")

            # NOTE: Cooldown is NOT checked here. The send_email_worker enforces cooldown
            # when actually sending. Queuing should always proceed regardless of cooldown
            # so that payment/form messages are prioritized over campaign messages when
            # the send worker runs and respects the priority ORDER BY.

            # --- Determine next action based on stage ---
            next_type = None
            template_type = None
            template_stage = None
            send = False

            # Normalize stage to lowercase
            stage_lower = stage.lower() if stage else None
            logger.info(f"[SINGLE CONTACT] Processing stage: {stage} (normalized: {stage_lower})")

            if stage_lower and re.search(r"\bforms\b", stage_lower):
                # Check if we've sent forms messages before
                forms_msgs = await conn.fetch('''
                    SELECT * FROM email_queue WHERE contact_id = $1 AND last_message_type LIKE 'forms%' AND status = 'sent' ORDER BY sent_at DESC
                ''', contact_id)
                if not forms_msgs:
                    next_type = 'forms_main'
                    template_type = 'campaign'
                    template_stage = 'forms'
                    send = True
                    logger.info(f"[SINGLE CONTACT] Will send forms_main to contact {contact_id}")
                else:
                    logger.info(f"[SINGLE CONTACT] Forms messages already sent for contact {contact_id}")

            elif stage_lower and re.search(r"\brh\b", stage_lower):
                # RH stage: check if any RH messages were sent before
                rh_msgs = await conn.fetch('''
                    SELECT * FROM email_queue WHERE contact_id = $1 AND last_message_type LIKE 'rh%' AND status = 'sent' ORDER BY sent_at DESC
                ''', contact_id)
                if not rh_msgs:
                    # Queue RH initial message using canonical token
                    next_type = 'rh_initial'
                    template_type = 'rh'
                    template_stage = 'initial'
                    send = True
                    logger.info(f"[SINGLE CONTACT] Will send rh_initial to contact {contact_id}")
                else:
                    logger.info(f"[SINGLE CONTACT] RH messages already sent for contact {contact_id}")

            elif stage_lower and re.search(r"\b(payment|payments)\b", stage_lower):
                # Check if we've sent payment messages before
                payment_msgs = await conn.fetch('''
                    SELECT * FROM email_queue WHERE contact_id = $1 AND last_message_type LIKE 'payment%' AND status = 'sent' ORDER BY sent_at DESC
                ''', contact_id)
                if not payment_msgs:
                    # Use canonical token 'payments_initial' (was historically 'payment_main')
                    next_type = 'payments_initial'
                    template_type = 'campaign'
                    template_stage = 'payments'
                    send = True
                    logger.info(f"[SINGLE CONTACT] Will send payments_initial to contact {contact_id}")
                else:
                    logger.info(f"[SINGLE CONTACT] Payment messages already sent for contact {contact_id}")
            elif stage_lower and re.search(r"\bsepa\b", stage_lower):
                # SEPA stage: check if any SEPA messages were sent before
                sepa_msgs = await conn.fetch('''
                    SELECT * FROM email_queue WHERE contact_id = $1 AND last_message_type LIKE 'sepa%' AND status = 'sent' ORDER BY sent_at DESC
                ''', contact_id)
                if not sepa_msgs:
                    # Queue SEPA initial message using canonical token
                    next_type = 'sepa_initial'
                    template_type = 'sepa'
                    template_stage = 'initial'
                    send = True
                    logger.info(f"[SINGLE CONTACT] Will send sepa_initial to contact {contact_id}")
                else:
                    logger.info(f"[SINGLE CONTACT] SEPA messages already sent for contact {contact_id}")

            # --- Send message if needed ---
            if send and template_type and template_stage:
                try:
                    logger.info(f"[SINGLE CONTACT] Loading templates for {template_stage}")
                    
                    # CHECK FOR CUSTOM MESSAGE FIRST
                    custom_msg = await get_message_for_sending(conn, contact_id, template_type)
                    
                    if custom_msg['is_custom']:
                        logger.info(f"[CUSTOM MSG] Using custom message for contact {contact_id}: {template_type}")
                        subject = custom_msg['subject']
                        body = custom_msg['body']
                    else:
                        # Fall back to templates
                        if template_stage and template_stage.startswith('reminder'):
                            subject = load_template(template_type, 'subject', reminder_type=template_stage)
                            body = load_template(template_type, 'body', reminder_type=template_stage)
                        else:
                            subject = load_template(template_type, 'subject', stage=template_stage)
                            body = load_template(template_type, 'body', stage=template_stage)

                    # Fetch the contact again to ensure we have the latest data
                    contact = await conn.fetchrow('''
                        SELECT c.*,
                               e.sender_email, e.org_name, e.city, e.month, e.venue, e.date2
                        FROM campaign_contacts c
                        JOIN event e ON c.event_id = e.id
                        WHERE c.id = $1
                    ''', contact_id)

                    if not contact:
                        logger.error(f"[SINGLE CONTACT] Contact {contact_id} not found")
                        return

                    # Validate required fields
                    if not contact.get('name'):
                        logger.error(f"[SINGLE CONTACT] Contact {contact_id} missing required field 'name'")
                        # Update the contact status to indicate an error
                        await conn.execute('''
                            UPDATE campaign_contacts
                            SET status = 'error', notes = $1
                            WHERE id = $2
                        ''', 'Missing required field: name', contact_id)
                        return

                    # Only validate forms_link for forms stage templates
                    if 'forms' in template_stage.lower() and not contact.get('forms_link'):
                        logger.error(f"[SINGLE CONTACT] Contact {contact_id} missing required field 'forms_link' for forms template")
                        # Update the contact status to indicate an error
                        await conn.execute('''
                            UPDATE campaign_contacts
                            SET status = 'error', notes = $1
                            WHERE id = $2
                        ''', 'Missing required field: forms_link for forms template', contact_id)
                        return

                    # Build template context
                    context = dict(contact)

                    # Expose invoice_number to templates (backwards-compatible alias)
                    try:
                        invoice_val = contact.get('invoice_number') or contact.get('invoice')
                        context['invoice_number'] = invoice_val
                        if 'invoice' not in context:
                            context['invoice'] = invoice_val
                    except Exception:
                        pass

                    # Extract name parts with prefix awareness (CRITICAL for proper greeting)
                    # Path A: With prefix -> ("Prof.", "Ayman")
                    # Path B: Without prefix -> ("Hatem", "Ayman")
                    raw_name = contact.get('name', '').strip()
                    # CRITICAL: Capture the DB prefix safely
                    db_prefix = contact.get('prefix', '').strip() if contact.get('prefix') else ""

                    # 2. Initialize defaults
                    final_prefix = ""
                    final_last_name = raw_name

                    # 3. Decision Logic
                    if db_prefix:
                        # --- CASE A: Database Prefix Exists (e.g. "Dr") ---
                        # We use the DB prefix, and split the name to get the Last Name
                        final_prefix = db_prefix
                        
                        # Fix Punctuation (Add dot if missing, skip "Sir")
                        if len(final_prefix) <= 3 and not final_prefix.endswith('.') and final_prefix.lower() not in ["sir", "madam"]:
                             final_prefix += "."

                        # Extract "Low" from "test3, low"
                        if ',' in raw_name:
                            final_last_name = raw_name.split(',')[-1].strip()
                        else:
                            parts = raw_name.split()
                            final_last_name = parts[-1].strip() if parts else raw_name
                    else:
                        # --- CASE B: No Database Prefix ---
                        # Auto-extract everything from the name string
                        try:
                            final_prefix, final_last_name = extract_name_parts_with_prefix(raw_name)
                        except Exception as e:
                            logger.error(f"Extraction error: {e}")
                            final_prefix = ""
                            final_last_name = raw_name

                    # 4. Capitalize and Format
                    final_prefix = final_prefix.title() if final_prefix else ""
                    final_last_name = final_last_name.title()

                    # 5. Construct Greeting Name (e.g. "Dr. Low")
                    if final_prefix:
                        greeting_name = f"{final_prefix} {final_last_name}"
                    else:
                        greeting_name = final_last_name

                    # 6. Update Context (Overwrite with corrected values)
                    context['prefix'] = final_prefix
                    context['last_name'] = final_last_name
                    context['greeting_name'] = greeting_name
                    
                    # Ensure 'name' matches greeting_name for templates using {{name}}
                    context['name'] = greeting_name 
                    
                    # Legacy support
                    context['first_name'] = final_prefix if final_prefix else (final_last_name.split()[0] if ' ' in final_last_name else final_last_name)

                    logger.info(f"[SINGLE CONTACT] Name extraction: raw='{raw_name}' -> prefix='{final_prefix}', last_name='{final_last_name}', greeting_name='{greeting_name}'")

                    # Log the full context for debugging
                    logger.info(f"[SINGLE CONTACT] Raw contact data: {contact}")
                    logger.info(f"[SINGLE CONTACT] Template context before rendering: {context}")

                    # Check for missing required fields
                    # Name should come from campaign_contacts, other fields from event
                    if not context.get('name'):
                        logger.error(f"[SINGLE CONTACT] Contact {contact_id} missing required field 'name' from campaign_contacts table")
                        await conn.execute('''
                            UPDATE campaign_contacts
                            SET status = 'error', notes = $1
                            WHERE id = $2
                        ''', 'Missing required field: name from campaign_contacts table', contact_id)
                        return

                    # Check event-related fields
                    missing_event_fields = [field for field in ['city', 'date2', 'venue', 'month']
                                         if not context.get(field)]
                    if missing_event_fields:
                        logger.error(f"[SINGLE CONTACT] Contact {contact_id} missing required event fields: {missing_event_fields}")
                        await conn.execute('''
                            UPDATE campaign_contacts
                            SET status = 'error', notes = $1
                            WHERE id = $2
                        ''', f'Missing required event fields: {", ".join(missing_event_fields)}', contact_id)
                        return

                    # Try rendering with possibly updated context
                    try:
                        if not subject or not subject.strip():
                            raise ValueError("Subject template is empty")
                        if not body or not body.strip():
                            raise ValueError("Body template is empty")

                        subject = render_template_strict(subject, context)
                        body = render_template_strict(body, context)

                        # Validate rendered content
                        if not subject or not subject.strip():
                            raise ValueError("Rendered subject is empty")
                        if not body or not body.strip():
                            raise ValueError("Rendered body is empty")

                        logger.info(f"[SINGLE CONTACT] Successfully rendered templates for contact {contact_id}")
                        logger.debug(f"[SINGLE CONTACT] Subject: {subject[:50]}...")

                    except ValueError as ve:
                        logger.error(f"[SINGLE CONTACT] Template rendering failed for contact {contact_id}: {ve}")
                        # Update the contact status to indicate the error
                        await conn.execute('''
                            UPDATE campaign_contacts
                            SET status = 'error', notes = $1
                            WHERE id = $2
                        ''', f'Template rendering failed: {str(ve)}', contact_id)
                        return  # Don't raise, just return to prevent cascade

                    # Check for duplicatess
                    existing = await conn.fetchval('''
                        SELECT 1 FROM email_queue
                        WHERE contact_id = $1 AND last_message_type = $2 AND status = 'pending'
                    ''', contact_id, next_type)

                    if existing:
                        logger.warning(f"[SINGLE CONTACT] Duplicate prevention: Skipping {next_type} for contact_id={contact_id} (already queued)")
                        return

                    # Queue the email (include CCs from cc_store or legacy email extras)
                    logger.info(f"[SINGLE CONTACT] Queuing email: {next_type} for contact {contact_id}")
                    cc_recipients = None
                    try:
                        if contact.get('cc_store'):
                            parts = [p.strip() for p in re.split(r'[;,\s]+', contact.get('cc_store') or '') if p.strip()]
                            cc_recipients = ';'.join(parts) if parts else None
                        else:
                            parsed = process_emails(contact.get('email') or '', validate=True)
                            if parsed and len(parsed) > 1:
                                cc_recipients = ';'.join([p for p in parsed[1:]])
                    except Exception:
                        cc_recipients = None

                    # Only include attachments for payments or payments reminders
                    attach_bytes = None
                    attach_filename = None
                    attach_mimetype = None
                    try:
                        if (template_type == 'payments' or template_type == 'sepa') or (template_stage and template_stage.startswith('reminder') and template_type in ('payments','sepa')) or (next_type and next_type.startswith('reminder') and template_type in ('payments','sepa')):
                            attach_bytes = contact.get('attachment')
                            attach_filename = contact.get('attachment_filename')
                            attach_mimetype = contact.get('attachment_mimetype')
                    except Exception:
                        attach_bytes = None


                    # Ensure queued token is canonical (payments_initial)
                    queue_type = next_type
                    if queue_type == 'payment_main':
                        queue_type = 'payments_initial'

                    await conn.execute('''
                          INSERT INTO email_queue (
                                 contact_id, sender_email, recipient_email, cc_recipients, subject, message, last_message_type, status, created_at, due_at, type, attachment, attachment_filename, attachment_mimetype
                                     ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending', $8, $9, $10, $11, $12, $13)
                        ''', contact_id, sender_email, contact['email'], cc_recipients, subject, body, queue_type, now, now, queue_type, attach_bytes, attach_filename, attach_mimetype)

                    # Update contact trigger info and canonicalize stored tokens
                    contact_token = queue_type
                    contact_status = f"{queue_type}_sent" if queue_type.endswith('_initial') else queue_type
                    await conn.execute('''
                        UPDATE campaign_contacts SET trigger = $1, last_action_time = $2, last_message_type = $3, last_triggered_at = $4, status = $5
                        WHERE id = $6
                    ''', contact_token, now, contact_token, now, contact_status, contact_id)

                    logger.info(f"[SINGLE CONTACT] Queued {next_type} email for contact {contact_id}")

                except Exception as e:
                    logger.error(f"[SINGLE CONTACT] Error processing contact {contact_id}: {e}")
                    raise e
            else:
                logger.info(f"[SINGLE CONTACT] No immediate action needed for contact {contact_id} in stage '{stage}'")

    except Exception as e:
        logger.error(f"[SINGLE CONTACT] Error processing single contact {contact_id}: {e}")
        raise e

# --- Reply Checker Worker: Checks for replies and updates status ---
# In main.py, replace the existing reply_checker_worker

# In main.py, replace the entire reply_checker_worker function with this one.

async def reply_checker_worker():
    """
    Checks for email replies by matching inbox messages against the last sent email for each contact.
    This version correctly fetches threading info from the email_queue table.
    """
    import asyncio
    from collections import defaultdict
    from dateutil.parser import parse as parse_dt
    
    ADVISORY_LOCK_KEY = 90001  # Unique key for reply_checker_worker

    while True:
        lock_acquired = False
        lock_conn = None
        try:
            # Acquire advisory lock to prevent multiple instances
            try:
                lock_conn = await db_pool.acquire()
                lock_acquired = await lock_conn.fetchval(f"SELECT pg_try_advisory_lock({ADVISORY_LOCK_KEY})")
                
                if not lock_acquired:
                    logger.debug("[REPLY CHECKER] Skipped due to active lock (another instance running)")
                    await asyncio.sleep(5)
                    continue
                    
                logger.debug(f"[REPLY CHECKER] Acquired advisory lock {ADVISORY_LOCK_KEY}")
                
            except Exception as e:
                logger.error(f"[REPLY CHECKER] Error acquiring lock: {e}")
                await asyncio.sleep(5)
                continue

            try:
                await update_worker_heartbeat('reply_checker_worker', 'running')
                # Use a short-lived connection to fetch all lookup data we need for this cycle.
                # We'll release the connection before making network calls to the Graph API
                # and acquire per-sender connections later when updating the DB.
                async with db_pool.acquire() as fetch_conn:
                    logger.info("[REPLY CHECKER] Starting reply check cycle...")

                    # 1. Get all active contacts that are expecting replies.
                    active_contacts = await fetch_conn.fetch('''
                        SELECT id, email, status, stage, event_id
                        FROM campaign_contacts
                        WHERE campaign_paused = FALSE AND status NOT IN ('completed', 'cancelled', 'Replied')
                    ''')

                    if not active_contacts:
                        logger.info("[REPLY CHECKER] No active contacts to check. Sleeping.")
                        await asyncio.sleep(300) # Sleep for 5 minutes
                        continue

                    contact_ids = [c['id'] for c in active_contacts]
                    contacts_by_id = {c['id']: dict(c) for c in active_contacts}

                    # 2. NEW: Create a lookup dictionary with the correct threading info for each contact.
                    # Primary source: last sent entry from email_queue (one row per contact_id).
                    # Fallback source: messages table (checks recipient_email and cc_recipients) so
                    # contacts who were only CC'd are also tracked for reply detection.
                    last_sent_lookup = {}
                    if contact_ids:
                        last_sent_emails = await fetch_conn.fetch('''
                            WITH ranked_emails AS (
                                SELECT
                                    contact_id, subject, sent_at, message_id, conversation_id,
                                    ROW_NUMBER() OVER(PARTITION BY contact_id ORDER BY sent_at DESC) as rn
                                FROM email_queue
                                WHERE status = 'sent' AND contact_id = ANY($1::int[]) AND message_id IS NOT NULL
                            )
                            SELECT contact_id, subject, sent_at, message_id, conversation_id
                            FROM ranked_emails
                            WHERE rn = 1
                        ''', contact_ids)

                        for row in last_sent_emails:
                            last_sent_lookup[row['contact_id']] = dict(row)

                    # Build a fallback mapping from normalized recipient email -> last sent message info
                    # This lets us detect replies when the contact was included as CC (stored in cc_recipients)
                    last_sent_by_email = {}
                    contact_emails = [normalize_email(c['email']) for c in active_contacts if c.get('email')]
                    if contact_emails:
                        # Fetch recent sent messages that might contain our contacts as main recipient or in CC
                        messages_sent = await fetch_conn.fetch('''
                            SELECT recipient_email, cc_recipients, sent_at, message_id, conversation_id
                            FROM messages
                            WHERE direction = 'sent' AND message_id IS NOT NULL
                            ORDER BY sent_at DESC
                            LIMIT 100000
                        ''')

                        # Iterate recent sent messages (most recent first) and assign the first matching message
                        # as the last_sent info for any contact email found in recipient or cc_recipients.
                        for m in messages_sent:
                            rec = (m.get('recipient_email') or '').strip()
                            cc_raw = (m.get('cc_recipients') or '')
                            cc_list = [normalize_email(x) for x in re.split(r'[;,\s]+', cc_raw) if x.strip()] if cc_raw else []

                            # normalize recipient and check
                            if rec:
                                nrec = normalize_email(rec)
                                if nrec in contact_emails and nrec not in last_sent_by_email:
                                    last_sent_by_email[nrec] = {
                                        'message_id': m.get('message_id'),
                                        'conversation_id': m.get('conversation_id'),
                                        'sent_at': m.get('sent_at'),
                                        'recipient_email': rec,
                                    }

                            # check cc list
                            for cc in cc_list:
                                if cc in contact_emails and cc not in last_sent_by_email:
                                    last_sent_by_email[cc] = {
                                        'message_id': m.get('message_id'),
                                        'conversation_id': m.get('conversation_id'),
                                        'sent_at': m.get('sent_at'),
                                        'recipient_email': rec,
                                    }

                    # 3. Group contacts by the sender email account we need to check.
                    event_ids = [c['event_id'] for c in active_contacts]
                    sender_map_rows = await fetch_conn.fetch('SELECT id, sender_email FROM event WHERE id = ANY($1::int[])', event_ids)
                    sender_map = {row['id']: row['sender_email'] for row in sender_map_rows}

                    contacts_by_sender = defaultdict(list)
                    for contact_id in contact_ids:
                        contact = contacts_by_id[contact_id]
                        event_id = contact['event_id']
                        sender_email = sender_map.get(event_id)
                        if sender_email:
                            contacts_by_sender[sender_email.lower()].append(contact)

                # 4. Loop through each sender account and check its inbox.
                for sender_email, contacts_to_check in contacts_by_sender.items():
                    logger.info(f"[REPLY CHECKER] Checking inbox of {sender_email} for {len(contacts_to_check)} contacts.")
                    try:
                        inbox_messages = await asyncio.to_thread(
                            graph_email.fetch_all_inbox_messages, sender_email, max_messages=100
                        )
                    except Exception as e:
                        logger.error(f"[REPLY CHECKER] Failed to fetch inbox for {sender_email}: {e}")
                        continue

                    # Acquire a fresh DB connection for processing this sender's inbox messages.
                    async with db_pool.acquire() as conn:
                        for msg in inbox_messages:
                            graph_message_id = msg.get('id')
                            if not graph_message_id:
                                continue

                            # Check if we have already processed this email to avoid redundant work
                            if await conn.fetchval("SELECT 1 FROM messages WHERE message_id = $1", graph_message_id):
                                continue

                            # For this inbox message, see if it matches any of our contacts
                            for contact in contacts_to_check:
                                last_sent_info = last_sent_lookup.get(contact['id'])
                                # If we don't have a last_sent entry by contact_id, try fallback by email
                                if not last_sent_info:
                                    try:
                                        pending_exists = await conn.fetchval(
                                            "SELECT 1 FROM email_queue WHERE contact_id = $1 AND status = 'pending' LIMIT 1",
                                            contact['id']
                                        )
                                        if pending_exists:
                                            logger.debug(f"[REPLY CHECKER] Skipping reply check for contact {contact['id']}: pending message exists")
                                            continue

                                        contact_email_norm = normalize_email(contact.get('email') or '')
                                        fallback = last_sent_by_email.get(contact_email_norm)

                                    
                                        if fallback:
                                            fallback_time = fallback.get('sent_at')
                                            if fallback_time and fallback_time.tzinfo:
                                                fallback_time = fallback_time.replace(tzinfo=None)

                                            now_native = datetime.now(UTC).replace(tzinfo=None)
                                            time_diff = (now_native - fallback_time).total_seconds() if fallback_time else float('inf')
                                            is_recent =time_diff < (30 * 86400)

                                            status_updated_at = contact.get('status_updated_at')
                                            if status_updated_at and status_updated_at.tzinfo:
                                                status_updated_at = status_updated_at.replace(tzinfo=None)

                                            if is_recent and (not status_updated_at or fallback_time >= status_updated_at):
                                               last_sent_info = {
                                                   'contact_id': contact['id'],
                                                   'subject': None,
                                                   'sent_at': fallback.get('sent_at'),
                                                   'message_id': fallback.get('message_id'),
                                                   'conversation_id': fallback.get('conversation_id')
                                               }
                                               logger.debug(f"[REPLY CHECKER] valid fallback for contact {contact['id']}")
                                            else:
                                                logger.debug(f"[REPLY CHECKER] invalid fallback for contact {contact['id']}")
                                                continue # This contact hasn't had an email sent we can match to.
                                        else:
                                            continue 
                                    except Exception as e:
                                        logger.error(f"[REPLY CHECKER] Error processing fallback for contact {contact['id']}: {e}")
                                        continue    
                                # --- THE CRITICAL MATCHING LOGIC ---
                                is_match = False


                                # Normalize IDs for reliable comparison (removes < > brackets)
                                in_reply_to_id = (msg.get('inReplyTo') or '').strip(' <>')
                                last_message_id = (last_sent_info.get('message_id') or '').strip(' <>')

                                # Quick deterministic match: if inReplyTo maps to contact(s) in message_contact_map, use that
                                if in_reply_to_id:
                                    try:
                                        mapped = await conn.fetch('SELECT contact_id FROM message_contact_map WHERE message_id = $1', in_reply_to_id)
                                        if mapped and len(mapped) > 0:
                                            # If the mapping includes our contact, mark as match
                                            mapped_ids = [m['contact_id'] for m in mapped]
                                            if contact['id'] in mapped_ids:
                                                is_match = True
                                                logger.info(f"[REPLY CHECKER] Matched via message_contact_map for contact {contact['id']} message {in_reply_to_id}")
                                            else:
                                                # if mapping exists but for other contacts, skip this contact
                                                continue
                                    except Exception:
                                        pass

                                # Match 1: Direct reply (most reliable)
                                if in_reply_to_id and last_message_id and in_reply_to_id == last_message_id:
                                    is_match = True

                                # Match 2: Conversation threading (good fallback)
                                elif msg.get('conversationId') and last_sent_info.get('conversation_id') and \
                                     msg.get('conversationId') == last_sent_info.get('conversation_id'):
                                    is_match = True

                                # Match 3: Subject + recipient heuristic - helps catch replies where contact was CC'd
                                if not is_match:
                                    try:
                                        # Build normalized subject comparisons (strip common reply prefixes)
                                        def norm_sub(s):
                                            if not s: return ''
                                            s2 = re.sub(r'^(re|fwd)[:\s]+', '', s.lower()).strip()
                                            s2 = re.sub(r'\s+', ' ', s2)
                                            return s2

                                        msg_subject_raw = msg.get('subject', '') or ''
                                        last_subject_raw = last_sent_info.get('subject') or ''
                                        msg_sub_norm = norm_sub(msg_subject_raw)
                                        last_sub_norm = norm_sub(last_subject_raw)

                                        # Extract recipients (To + Cc)
                                        to_list = [normalize_email(r.get('emailAddress', {}).get('address', '')) for r in (msg.get('toRecipients') or [])]
                                        cc_list = [normalize_email(r.get('emailAddress', {}).get('address', '')) for r in (msg.get('ccRecipients') or [])]
                                        all_recipients = set([r for r in to_list + cc_list if r])

                                        contact_email_norm = normalize_email(contact.get('email') or '')

                                        # If subject matches (loose) and contact is present in recipients, treat as match
                                        if last_sub_norm and msg_sub_norm and last_sub_norm in msg_sub_norm and contact_email_norm in all_recipients:
                                            is_match = True
                                    except Exception:
                                        pass

                                # Heuristic fallback: if sender of the inbox message is the contact email and the
                                # message subject loosely contains the last sent subject, treat as a reply.
                                if not is_match:
                                    try:
                                        sender_addr = (msg.get('from') or {}).get('emailAddress', {}).get('address', '')
                                        sender_norm = normalize_email(sender_addr)
                                        contact_email_norm = normalize_email(contact.get('email') or '')
                                        msg_subject_raw = msg.get('subject', '') or ''
                                        def norm_sub_simple(s):
                                            return re.sub(r'^(re|fwd)[:\s]+', '', (s or '').lower()).strip()

                                        if sender_norm and contact_email_norm and sender_norm == contact_email_norm:
                                            last_sub = (last_sent_info.get('subject') or '')
                                            if last_sub:
                                                if norm_sub_simple(last_sub) in norm_sub_simple(msg_subject_raw):
                                                    is_match = True
                                                    logger.info(f"[REPLY CHECKER] Fallback sender+subject match for contact {contact['id']} sender={sender_norm} msg_id={graph_message_id}")
                                    except Exception:
                                        pass

                                if is_match:
                                    # Before treating as a reply, check if this inbox message is a bounce
                                    logger.debug(f"[REPLY CHECKER] Candidate match: msg_id={graph_message_id} contact={contact['id']} is_match={is_match} inReplyTo={in_reply_to_id} conv={msg.get('conversationId')} sender={msg.get('from')} to={[(r.get('emailAddress',{}).get('address')) for r in (msg.get('toRecipients') or [])]} cc={[(r.get('emailAddress',{}).get('address')) for r in (msg.get('ccRecipients') or [])]} subject='{msg.get('subject','')}' last_msg_id='{last_message_id}' last_subj='{last_sent_info.get('subject')}'")
                                    sender_address_raw = ''
                                    try:
                                        sender_address_raw = msg.get('from', {}).get('emailAddress', {}).get('address', '')
                                    except Exception:
                                        sender_address_raw = ''

                                    if is_bounce_email(msg.get('subject', ''), msg.get('processed_body', ''), sender_address_raw):
                                        logger.info(f"[BOUNCE DETECTED IN INBOX] Message {graph_message_id} looks like a bounce; handling.")
                                        try:
                                            await handle_bounce_email(conn, msg.get('subject', ''), msg.get('processed_body', ''), sender_address_raw)
                                        except Exception as e:
                                            logger.error(f"[BOUNCE] Error handling bounce from inbox message {graph_message_id}: {e}")
                                        # Skip treating this message as a contact reply
                                        continue

                                    # Final verification: Did the reply come from the correct person OR include contact as recipient?
                                    sender_of_reply = normalize_email(sender_address_raw)
                                    
                                    # Parse contact email field (may contain comma-separated multiple emails)
                                    contact_email_raw = (contact.get('email') or '')
                                    contact_emails = [normalize_email(e.strip()) for e in contact_email_raw.split(',') if e.strip()]
                                    
                                    # Check if contact is in To or CC recipients (for CC'd reply detection)
                                    to_list = [normalize_email(r.get('emailAddress', {}).get('address', '')) for r in (msg.get('toRecipients') or [])]
                                    cc_list = [normalize_email(r.get('emailAddress', {}).get('address', '')) for r in (msg.get('ccRecipients') or [])]
                                    all_recipients = set([r for r in to_list + cc_list if r])
                                    
                                    # Check if sender is any of the contact emails OR contact is in recipients
                                    is_direct_sender = sender_of_reply in contact_emails
                                    is_cc_recipient = any(email in all_recipients for email in contact_emails)
                                    
                                    if not is_direct_sender and not is_cc_recipient:
                                        logger.debug(f"[REPLY CHECKER] Rejected: sender={sender_of_reply} not in contact_emails={contact_emails} and contact not in recipients. to={to_list} cc={cc_list}")
                                        continue # Not a reply from our target contact or their thread

                                    logger.info(f"[REPLY DETECTED] New reply for contact {contact['id']} (Message ID: {graph_message_id}) sender={sender_of_reply} contact_emails={contact_emails} is_direct={is_direct_sender} is_cc={is_cc_recipient}")

                                    # Use a transaction to safely update the database
                                    async with conn.transaction():
                                        msg_time = parse_dt(msg['receivedDateTime']).astimezone(UTC).replace(tzinfo=None)

                                        # Update contact status to 'Replied' and pause the campaign
                                        await conn.execute("""
                                            UPDATE campaign_contacts
                                            SET status = 'Replied', campaign_paused = TRUE, last_triggered_at = $2,
                                                trigger = COALESCE(trigger || E'\n', '') || $3
                                            WHERE id = $1
                                        """, contact['id'], msg_time, f"Reply detected at {msg_time}")

                                        # Insert the new, unique reply into our messages table for history
                                        # Extract cc recipients from the incoming message headers if present
                                        cc_field = ''
                                        try:
                                            # msg may include an 'ccRecipients' or internetMessageHeaders with Cc
                                            cc_field = msg.get('ccRecipients') or ''
                                            if not cc_field:
                                                # Try headers
                                                headers = msg.get('internetMessageHeaders') or []
                                                for h in headers:
                                                    if h.get('name', '').lower() == 'cc':
                                                        cc_field = h.get('value') or ''
                                                        break
                                        except Exception:
                                            cc_field = ''

                                        await conn.execute('''
                                            INSERT INTO messages (
                                                contact_id, direction, sender_email, recipient_email, cc_recipients, subject, body,
                                                received_at, stage, message_id, in_reply_to
                                            ) VALUES ($1, 'received', $2, $3, $4, $5, $6, $7, $8, $9, $10)
                                        ''',
                                        contact['id'],
                                        sender_of_reply,
                                        sender_email,
                                        cc_field,
                                        msg.get('subject', ''),
                                        msg.get('processed_body', ''),
                                        msg_time,
                                        contact['stage'],
                                        graph_message_id,
                                        msg.get('inReplyTo'))

                                    break # Match found, move to the next inbox message
                                
            except Exception as e:
                logger.error(f'[REPLY CHECKER] Worker encountered a critical error: {e}', exc_info=True)
                await update_worker_heartbeat('reply_checker_worker', 'error', str(e))
                
            finally:
                # Always release the advisory lock
                if lock_acquired and lock_conn:
                    try:
                        await lock_conn.execute(f"SELECT pg_advisory_unlock({ADVISORY_LOCK_KEY})")
                        logger.debug(f"[REPLY CHECKER] Released advisory lock {ADVISORY_LOCK_KEY}")
                    except Exception as lock_e:
                        logger.error(f"[REPLY CHECKER] Error releasing lock: {lock_e}")
                    finally:
                        await db_pool.release(lock_conn)

        except Exception as outer_e:
            logger.error(f"[REPLY CHECKER] Unexpected outer error: {outer_e}")

        await asyncio.sleep(300) # Check for replies every 5 minutes
# Utility to generate quoted email thread block

# Second implementation of generate_quoted_block removed - using version defined above

# --- Store reply in messages table only if not already present ---
async def cleanup_duplicate_messages():
    """Clean up duplicate messages from the messages table"""
    async with db_pool.acquire() as conn:
        # Create a temporary table to store messages we want to keep
        await conn.execute(r"""
            CREATE TEMP TABLE messages_to_keep AS
            WITH RankedMessages AS (
                SELECT
                    id,
                    contact_id,
                    direction,
                    sender_email,
                    recipient_email,
                    subject,
                    body,
                    message_id,
                    sent_at,
                    received_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            LOWER(sender_email),
                            LOWER(recipient_email),
                            LOWER(subject),
                            LOWER(regexp_replace(body, '\s+', ' ', 'g'))
                        ORDER BY
                            COALESCE(sent_at, received_at) DESC,
                            id DESC
                    ) as rn
                FROM messages
            )
            SELECT id FROM RankedMessages WHERE rn = 1
        """)

        # Delete duplicates
        deleted = await conn.execute("""
            WITH deleted AS (
                DELETE FROM messages
                WHERE id NOT IN (SELECT id FROM messages_to_keep)
                RETURNING id
            )
            SELECT COUNT(*) FROM deleted
        """)

        # Drop temporary table
        await conn.execute("DROP TABLE messages_to_keep")

        return deleted

async def is_duplicate_message(conn, message: dict) -> bool:
    """
    Check if a message is a duplicate using multiple criteria.

    Args:
        conn: Database connection
        message (dict): Message to check for duplicates

    Returns:
        bool: True if message is a duplicate, False otherwise
    """
    # Normalize the message content
    sender = normalize_email(message.get('sender_email', ''))
    subject = re.sub(r'^re:\s*', '', message.get('subject', '').lower()).strip()
    body = ' '.join(message.get('body', '').lower().split())
    timestamp = message.get('sent_at') or message.get('received_at')
    message_id = message.get('message_id')

    # Check for duplicates using multiple criteria
    duplicate = await conn.fetchval(r"""
        SELECT 1 FROM messages
        WHERE
            -- Check by message_id if available
            (message_id = $1 AND message_id IS NOT NULL)
            OR
            -- Check by content similarity within a time window
            (
                LOWER(sender_email) = LOWER($2)
                AND (
                    -- Exact subject match after removing 're:'
                    regexp_replace(LOWER(subject), '^re:\s*', '') = $3
                    OR
                    -- High similarity subject
                    similarity(LOWER(subject), $3) > 0.9
                )
                AND (
                    -- Exact body match after normalization
                    regexp_replace(LOWER(body), '\s+', ' ') = $4
                    OR
                    -- High similarity body
                    similarity(LOWER(body), $4) > 0.9
                )
                -- Within 5 minute window
                AND ABS(EXTRACT(EPOCH FROM (COALESCE(sent_at, received_at) - $5))) < 300
            )
    """, message_id, sender, subject, body, timestamp)

    return bool(duplicate)

async def store_reply_in_messages_table(reply):
    """
    Store a reply in the messages table if it doesn't already exist.

    Args:
        reply (dict): The reply message to store.
    """
    async with db_pool.acquire() as conn:
        # Check if this is a bounce email first
        subject = reply.get('subject', '')
        body = reply.get('body', '')
        from_email = reply.get('sender_email', '')

        if is_bounce_email(subject, body, from_email):
            logger.info(f"[BOUNCE] Detected bounce email from {from_email}")
            await handle_bounce_email(conn, subject, body, from_email)
            # Don't store bounce emails in regular messages table
            return

        # Use our new duplicate detection function
        if await is_duplicate_message(conn, reply):
            logger.info(f"Skipping duplicate message from {reply.get('sender_email')} with subject: {reply.get('subject')}")
            return

        if existing_message:
            # Reply already exists, skip storing
            return

        # Insert the reply into the messages table
        await conn.execute(
            """
            INSERT INTO messages (
                contact_id, direction, sender_email, recipient_email, subject, body,
                message_id, in_reply_to, sent_at, received_at, stage, created_at, metadata
            ) VALUES (
                $1, $2, $3, $4, $5, $6,
                $7, $8, $9, $10, $11, $12, $13
            )
            """,
            reply.get('contact_id'),
            'received',
            reply.get('sender_email'),
            reply.get('recipient_email'),
            reply.get('subject'),
            reply.get('body'),
            reply.get('message_id'),
            reply.get('in_reply_to'),
            reply.get('sent_at'),
            reply.get('received_at'),
            reply.get('stage'),
            datetime.now(),
            reply.get('metadata')
        )

# Example usage in the reply fetching logic
async def fetch_and_store_replies():
    """
    Fetch replies and store them in the messages table, ensuring no duplicates.
    """
    replies = await fetch_replies_from_source()  # Replace with actual fetching logic

    for reply in replies:
        await store_reply_in_messages_table(reply)

# Helper function to queue stage-specific messages
async def queue_stage_message(conn, contact_id: int, stage: str, contact_row):
    """Queue a stage message (forms/payments) with proper email quoting."""
    try:
        # Load the appropriate template
        template_body = load_template(stage, 'body', None, 'initial')
        template_subject = load_template(stage, 'subject', None, 'initial')
        
        # Render the templates
        message_body = render_template_strict(template_body, contact_row)
        subject = render_template_strict(template_subject, contact_row)
        
        # Build the complete message body with quoted history
        message_body = await build_outgoing_body(contact_row, message_body)
        
        # Get any CC recipients
        cc_recipients = contact_row.get('cc_store', '').split(',') if contact_row.get('cc_store') else None
        if cc_recipients:
            cc_recipients = [cc.strip() for cc in cc_recipients if cc.strip()]
            
        # Store this message as the last sent
        await conn.execute("""
            UPDATE campaign_contacts
            SET last_sent_body = $1,
                last_sent_at = NOW()
            WHERE id = $2
        """, message_body, contact_id)
        
        # Queue the email (include created_at and due_at)
        await conn.execute("""
            INSERT INTO email_queue (
                sender_email, recipient_email, cc_recipients,
                subject, message, contact_id, type,
                status, campaign_stage, forms_link, payment_link,
                attachment, attachment_filename, attachment_mimetype,
                created_at, due_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, NOW(), NOW())
        """, DEFAULT_SENDER_EMAIL, contact_row['email'], 
            cc_recipients, subject, message_body,
            contact_id, 'campaign', 'pending', stage,
            contact_row.get('forms_link'), contact_row.get('payment_link'),
            contact_row.get('attachment'), contact_row.get('attachment_filename'), contact_row.get('attachment_mimetype'))
            
        logger.info(f"[QUEUE] Stage message queued for contact {contact_id}: {stage}")
        return True
        
    except Exception as e:
        logger.error(f"[ERROR] Failed to queue stage message for contact {contact_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to queue message: {str(e)}")
    """Queue appropriate message when stage changes"""
    try:
        logger.info(f"[QUEUE STAGE] Starting to queue {stage} message for contact_id={contact_id}")

        # Get event details
        event = await conn.fetchrow('SELECT * FROM event WHERE id = $1', contact_row['event_id'])
        if not event:
            logger.error(f"[QUEUE STAGE] Event not found for contact_id={contact_id}")
            return

        logger.info(f"[QUEUE STAGE] Found event {event['id']} for contact {contact_id}")

        # Get the first message's conversation details for threading
        first_message = await conn.fetchrow('''
            SELECT conversation_id, message_id, subject FROM email_queue
            WHERE contact_id = $1 AND status = 'sent'
            ORDER BY sent_at ASC LIMIT 1
        ''', contact_id)

        # Get threading info for reply detection, but send as individual emails
        conversation_id = first_message['conversation_id'] if first_message else None
        in_reply_to = first_message['message_id'] if first_message else None
        original_subject = first_message.get('subject') if first_message else None

        logger.info(f"[QUEUE STAGE] Individual email mode - threading info preserved for reply detection: conv={conversation_id}, reply_to={in_reply_to}")

        # Determine template based on stage
        template_type = 'campaign'
        template_stage = stage.lower()

        # Update message_type to use _initial suffix instead of _main
        if stage.lower() == 'forms':
            template_stage = 'forms'
            message_type = 'forms_initial'
        elif stage.lower() in ['payments', 'payment']:
            template_stage = 'payments'
            message_type = 'payments_initial'
        else:
            logger.warning(f"[QUEUE STAGE] Unknown stage '{stage}' for contact_id={contact_id}")
            return

        logger.info(f"[QUEUE STAGE] Using template_type={template_type}, template_stage={template_stage}, message_type={message_type}")

        # Check for duplicates by message_type AND recipient_email (more robust)
        existing = await conn.fetchval('''
            SELECT 1 FROM email_queue
            WHERE last_message_type = $1 AND recipient_email = $2 AND status IN ('pending', 'sent')
            LIMIT 1
        ''', message_type, contact_row['email'])

        if existing:
            logger.warning(f"[DUPLICATE] Message type '{message_type}' already exists for recipient {contact_row['email']}")
            return

        # Load and render templates
        try:
            if template_stage and template_stage.startswith('reminder'):
                subject_template = load_template(template_type, 'subject', reminder_type=template_stage)
                body_template = load_template(template_type, 'body', reminder_type=template_stage)
            else:
                subject_template = load_template(template_type, 'subject', stage=template_stage)
                body_template = load_template(template_type, 'body', stage=template_stage)

            logger.info(f"[QUEUE STAGE] Loaded templates for {template_stage}")

            # Prepare context for template rendering
            context = dict(contact_row)
            context.update(dict(event))

            # Log context for debugging
            logger.info(f"[QUEUE STAGE] Template context keys: {list(context.keys())}")

            # Render subject with proper fallback
            try:
                subject = render_template_strict(subject_template, context).strip()
                # Clean up whitespace in subject
                subject = ' '.join(subject.split())

                # ALWAYS ensure non-empty subject
                if not subject or not subject.strip():
                    subject = f"Follow-up regarding {event.get('event_name', 'your reservation')}"
                    logger.warning(f"[QUEUE STAGE] Empty subject, using fallback: {subject}")

                # Final validation - subject must never be empty
                if not subject or not subject.strip():
                    subject = "Follow-up regarding your reservation"
                    logger.error(f"[QUEUE STAGE] Subject still empty after fallback, using default: {subject}")

                # Keep the rendered template subject - do NOT override with original subject
                # Each stage should have its own subject from the template
                logger.info(f"[QUEUE STAGE] Using template subject for {message_type}: {subject}")

            except Exception as e:
                logger.error(f"[QUEUE STAGE] Error rendering subject: {e}")
                subject = f"Follow-up regarding {event.get('event_name', 'your reservation')}"

            # Render message body as plain text (no HTML conversion)
            body_original = render_template_strict(body_template, context)

            # Fetch full message history for quoting, including all replies
            history_rows = await conn.fetch("""
                SELECT direction, sender_email, recipient_email, subject, body,
                       sent_at, received_at, stage, in_reply_to
                FROM messages
                WHERE contact_id=$1
                ORDER BY COALESCE(sent_at, received_at) DESC
            """, contact_id)

            # NO QUOTED BLOCKS - Send individual emails without conversation history
            # Create only plain text versions without quotes
            body_plain = body_original
            final_body = body_plain

            logger.info(f"[QUEUE STAGE] Rendered templates and appended quote - subject: '{subject[:50]}...'")

        except Exception as e:
            logger.error(f"[QUEUE STAGE] Template rendering failed for contact_id={contact_id}, stage={stage}: {e}")
            return

        # Queue the message with threading information
        now = datetime.now(UTC).replace(tzinfo=None)
        # Determine cc_recipients for the stage message
        cc_recipients = None
        try:
            if contact_row.get('cc_store'):
                parts = [p.strip() for p in re.split(r'[;,\s]+', contact_row.get('cc_store') or '') if p.strip()]
                cc_recipients = ';'.join(parts) if parts else None
            else:
                parsed = process_emails(contact_row.get('email') or '', validate=True)
                if parsed and len(parsed) > 1:
                    cc_recipients = ';'.join([p for p in parsed[1:]])
        except Exception:
            cc_recipients = None

        # Determine whether to attach contact-level file: only for payments or its reminders
        attach_bytes = None
        attach_filename = None
        attach_mimetype = None
        try:
            if (message_type == 'payments') or (stage and stage.startswith('reminder') and message_type == 'payments'):
                attach_bytes = contact_row.get('attachment')
                attach_filename = contact_row.get('attachment_filename')
                attach_mimetype = contact_row.get('attachment_mimetype')
        except Exception:
            attach_bytes = None

        await conn.execute('''
            INSERT INTO email_queue (
                contact_id, event_id, sender_email, recipient_email, cc_recipients, subject, message,
                last_message_type, status, created_at, due_at, type, campaign_stage,
                conversation_id, in_reply_to, forms_link, payment_link, attachment, attachment_filename, attachment_mimetype
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'pending', $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
        ''',
        contact_id, contact_row['event_id'], event['sender_email'], contact_row['email'], cc_recipients,
        subject, body_plain, message_type, now, now, message_type, stage,
        conversation_id, in_reply_to, contact_row.get('forms_link'), contact_row.get('payment_link'), attach_bytes, attach_filename, attach_mimetype)

        # Also record into messages table for full history tracking
        await conn.execute('''
            INSERT INTO messages (contact_id, direction, sender_email, recipient_email, subject, body, stage, sent_at)
            VALUES ($1, 'sent', $2, $3, $4, $5, $6, $7)
        ''',
        contact_id, event['sender_email'], contact_row['email'], subject, body_plain, stage, now)

        logger.info(f"[QUEUE STAGE] Queued {stage} message and stored to messages for contact_id={contact_id}")

    except Exception as e:
        logger.error(f"[QUEUE STAGE] Failed to queue stage message for contact_id={contact_id}, stage={stage}: {e}")
        raise  # Re-raise to let caller handle it

# --- Event & CampaignContact Models ---
class MessageOut(BaseModel):
    id: int
    contact_id: int
    direction: str
    sender_email: Optional[str] = None
    recipient_email: Optional[str] = None
    subject: Optional[str] = None
    body: Optional[str] = None
    sent_at: Optional[datetime] = None
    received_at: Optional[datetime] = None
    stage: Optional[str] = None


# ---------- API Routes ----------

class ContactOption(BaseModel):
    id: int
    name: str
    email: str

@app.get("/contacts-options", response_model=list[ContactOption])
async def get_contacts_options(limit: int = 100, current_user: dict = Depends(get_current_user)):
    """Return a lightweight list of contacts for dropdown selection."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, email FROM campaign_contacts ORDER BY id DESC LIMIT $1",
            limit,
        )
        return [dict(r) for r in rows]


@app.get("/contacts/{contact_id}/messages", response_model=list[MessageOut])
async def get_contact_messages(contact_id: int, current_user: dict = Depends(get_current_user)):
    """Return full conversation history for a contact ordered newest oldest."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, contact_id, direction, sender_email, recipient_email, subject, body, sent_at, received_at, stage "
            "FROM messages WHERE contact_id=$1 ORDER BY COALESCE(sent_at, received_at) DESC",
            contact_id,
        )
        return [dict(r) for r in rows]


    @app.get('/contacts/{contact_id}/custom_flow')
    async def get_custom_flow(contact_id: int, current_user: dict = Depends(get_current_user)):
        """Return custom flow steps for a contact (if any)"""
        async with db_pool.acquire() as conn:
            flow = await conn.fetchrow('SELECT id, contact_id, active, created_at, updated_at FROM custom_flows WHERE contact_id = $1', contact_id)
            if not flow:
                return { 'flow_steps': [] }
            steps = await conn.fetch('SELECT id, flow_id, step_order, type, subject, body, delay_days FROM custom_flow_steps WHERE flow_id = $1 ORDER BY step_order', flow['id'])
            return { 'flow_id': flow['id'], 'flow_steps': [dict(s) for s in steps] }


    @app.post('/contacts/{contact_id}/custom_flow')
    async def create_or_update_custom_flow(contact_id: int, payload: CustomFlow = Body(...), current_user: dict = Depends(get_current_user)):
        """Create or overwrite a custom flow for the given contact. Payload contains steps."""
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # Upsert flow row
                flow = await conn.fetchrow('SELECT id FROM custom_flows WHERE contact_id = $1 FOR UPDATE', contact_id)
                now = datetime.now(UTC).replace(tzinfo=None)
                if not flow:
                    flow_row = await conn.fetchrow('INSERT INTO custom_flows (contact_id, created_at, updated_at, active) VALUES ($1, $2, $3, TRUE) RETURNING id', contact_id, now, now)
                    flow_id = flow_row['id']
                else:
                    flow_id = flow['id']
                    await conn.execute('UPDATE custom_flows SET updated_at = $1 WHERE id = $2', now, flow_id)

                # Remove existing steps (overwrite)
                await conn.execute('DELETE FROM custom_flow_steps WHERE flow_id = $1', flow_id)

                # Insert new steps with order
                for idx, s in enumerate(payload.steps or []):
                    step_order = idx + 1
                    step_type = s.type if s.type in ('email','task','notification') else 'email'
                    subject = getattr(s, 'subject', None)
                    body = getattr(s, 'body', None)
                    delay = getattr(s, 'delay_days', 0) or 0
                    await conn.execute('''
                        INSERT INTO custom_flow_steps (flow_id, step_order, type, subject, body, delay_days, created_at)
                        VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ''', flow_id, step_order, step_type, subject, body, delay, now)

                # Mark contact as custom flow
                await conn.execute("UPDATE campaign_contacts SET flow_type = 'custom' WHERE id = $1", contact_id)

                return {'flow_id': flow_id, 'message': 'Custom flow saved'}


class Event(BaseModel):
    id: Optional[int] = None
    event_name: str
    org_name: Optional[str] = None
    event_url: Optional[str] = None
    org_id: Optional[int] = None
    month: Optional[str] = None
    sender_email: Optional[str] = None
    city: Optional[str] = None
    venue: Optional[str] = None
    date2: Optional[str] = None
    expected_contact_count: Optional[int] = None

class CampaignContact(BaseModel):
    id: Optional[int] = None
    event_id: int
    name: Optional[str] = None
    email: Optional[str] = None
    status: Optional[str] = None
    stage: Optional[str] = None
    link: Optional[str] = None
    created_at: Optional[str] = None

from fastapi.responses import StreamingResponse
import csv
from io import StringIO

@app.get("/events")
async def list_events(
    page: int = 1,
    page_size: int = 50,
    search: Optional[str] = None,
    search_contacts: bool = False,  # if True, search across all events + contacts even if not showing
    current_user: dict = Depends(get_current_user)
):
    """
    List events with pagination (last 50 by default).
    
    Args:
        page: Page number (1-indexed)
        page_size: Number of events per page (default 50)
        search: Search query (searches event name, org name, contacts data, etc.)
        search_contacts: If True, returns matching contacts data even for events not in current page
    """
    offset = (page - 1) * page_size
    
    # Select event fields, prefer org_name from organizations table
    base_query = "SELECT e.*, COALESCE(e.org_name, o.name) AS org_name FROM event e LEFT JOIN organizations o ON e.org_id = o.id"
    filters = []
    params = []
    
    # Build search filter (searches across events and contacts)
    search_query = None
    if search and search.strip():
        search_query = f"%{search.strip()}%"
        params.append(search_query)
        param_num = len(params)
        # Search in event fields OR in contact data - use same parameter number for all ILIKE
        filters.append("""(
            e.event_name ILIKE $%d 
            OR e.org_name ILIKE $%d 
            OR o.name ILIKE $%d 
            OR e.sender_email ILIKE $%d 
            OR e.city ILIKE $%d 
            OR e.venue ILIKE $%d
            OR e.id IN (
                SELECT DISTINCT event_id FROM campaign_contacts 
                WHERE name ILIKE $%d 
                OR email ILIKE $%d 
                OR notes ILIKE $%d
                OR nationality ILIKE $%d
                OR workplace ILIKE $%d
                OR supplier ILIKE $%d
                OR payment_method ILIKE $%d
            )
        )""" % tuple([param_num] * 13))  # Reuse same parameter for all 13 conditions
    
    if filters:
        base_query += " WHERE " + " AND ".join(filters)
    
    # Order events newest first
    base_query += " ORDER BY e.id DESC"
    
    async with db_pool.acquire() as conn:
        # Get total count of matching events (count entire result set before pagination)
        count_query = f"SELECT COUNT(*) FROM ({base_query}) AS temp"
        total_count_result = await conn.fetchval(count_query, *params)
        
        # Get paginated events
        paginated_query = base_query + f" LIMIT ${len(params)+1} OFFSET ${len(params)+2}"
        params_with_pagination = params + [page_size, offset]
        
        events = await conn.fetch(paginated_query, *params_with_pagination)
        
        result = []
        for event in events:
            # Get all contacts for this event
            contacts = await conn.fetch(
                "SELECT id, name, email, stage, status, nationality, notes, organizer, hotel_name, supplier, workplace, payment_method, trigger, forms_link, payment_link FROM campaign_contacts WHERE event_id = $1 ORDER BY id",
                event["id"]
            )
            result.append({
                **dict(event),
                "contacts": len(contacts),
                "contacts_list": [dict(c) for c in contacts]
            })
        
        # If search_contacts=True, also fetch matching contacts from ALL events (for global search)
        all_matching_contacts = {}
        if search_query:  # Always fetch search results if there's a search query
            all_contacts = await conn.fetch("""
                SELECT id, event_id, name, email, stage, status, nationality, notes, organizer, hotel_name, 
                       supplier, workplace, payment_method, trigger, forms_link, payment_link 
                FROM campaign_contacts 
                WHERE name ILIKE $1 
                   OR email ILIKE $1 
                   OR notes ILIKE $1
                   OR nationality ILIKE $1
                   OR workplace ILIKE $1
                   OR supplier ILIKE $1
                   OR payment_method ILIKE $1
                   OR trigger ILIKE $1
                   OR organizer ILIKE $1
                   OR hotel_name ILIKE $1
                   OR forms_link ILIKE $1
                   OR payment_link ILIKE $1
                ORDER BY event_id, id
            """, search_query)
            # Group by event_id
            for contact in all_contacts:
                event_id = contact["event_id"]
                if event_id not in all_matching_contacts:
                    all_matching_contacts[event_id] = []
                all_matching_contacts[event_id].append(dict(contact))
        
        return {
            "events": result,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total_events": total_count_result or 0,
                "total_pages": (total_count_result + page_size - 1) // page_size if total_count_result else 1
            },
            "search_results": all_matching_contacts if all_matching_contacts else None
        }


 


@app.get("/events/{event_id}")
async def get_event_details(event_id: int, current_user: dict = Depends(get_current_user)):
    async with db_pool.acquire() as conn:
        # fetch the event and prefer the organization's name when event.org_name
        # is empty by joining organizations and coalescing.
        event = await conn.fetchrow(
            "SELECT e.*, COALESCE(e.org_name, o.name) AS org_name FROM event e LEFT JOIN organizations o ON e.org_id = o.id WHERE e.id = $1",
            event_id
        )
        if not event:
            raise HTTPException(status_code=404, detail="Event not found")
        # Include contact id and link so frontend components can use contact.id and contact.link
        contacts = await conn.fetch(
            "SELECT id, name, email, stage, status, nationality, link FROM campaign_contacts WHERE event_id = $1 ORDER BY id",
            event_id
        )
        return {
            "event": dict(event),
            "contacts": [dict(row) for row in contacts],
            "contacts_count": len(contacts)
        }

@app.post("/events")
async def create_event(event: Event, current_user: dict = Depends(get_current_user)):
    async with db_pool.acquire() as conn:
        # If caller provided org_id in the incoming JSON body, prefer it over org_name
        payload = event.dict()
        org_id = payload.get('org_id') or None
        
        # Handle sender_email auto-assignment
        sender_email = event.sender_email
        if sender_email and sender_email.lower() == "auto":
            # Get estimated event size (default to 0 if not available)
            event_size = getattr(event, 'expected_contact_count', None) or 0
            sender_email = await get_auto_assigned_sender(conn, event_size)
            logger.info(f"Auto-assigned sender email: {sender_email} for event with ~{event_size} contacts")
        
        # validate org_id if present
        if org_id is not None:
            org_row = await conn.fetchrow("SELECT id FROM organizations WHERE id = $1", org_id)
            if not org_row:
                raise HTTPException(status_code=400, detail="Invalid org_id")
        
        row = await conn.fetchrow(
            "INSERT INTO event (event_name, org_name, org_id, month, sender_email, city, venue, date2, event_url, expected_contact_count) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING *",
            event.event_name, event.org_name, org_id, event.month, sender_email, event.city, event.venue, event.date2, getattr(event, 'event_url', None), event.expected_contact_count or 0
        )

        # Log event creation activity
        await log_user_activity(
            conn,
            current_user,
            "CREATE_EVENT",
            f"Created new event: {event.event_name} in {event.city}",
            target_type="event",
            target_id=row['id'],
            target_name=event.event_name,
            new_values=dict(event)
        )

        return {"event_id": row["id"]}


@app.get("/events/similarity")
@app.get("/events/similarity-search")
@app.get("/events/match")
@app.get("/search/events/similarity")
@app.get("/search/similarity")
async def event_name_similarity(
    name: str,
    threshold: int = 80,
    current_user: dict = Depends(get_current_user)
):
    """Return events whose name fuzzy-matches the provided `name` with a
    similarity ratio >= `threshold` (0-100). Uses difflib.SequenceMatcher to
    compute a simple ratio good for quick client-side guidance.
    """
    from difflib import SequenceMatcher

    if not name:
        return {"matches": []}

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, event_name FROM event")
        matches = []
        n = (name or "").lower()
        for r in rows:
            ev_name = (r.get("event_name") or "").lower()
            if not ev_name:
                continue
            score = int(SequenceMatcher(None, n, ev_name).ratio() * 100)
            if score >= int(threshold):
                matches.append({"event_id": r["id"], "event_name": r["event_name"], "score": score})

        matches.sort(key=lambda x: x["score"], reverse=True)
        return {"matches": matches}


@app.patch("/events/{event_id}")
async def update_event_note(event_id: int, payload: dict = Body(...), current_user: dict = Depends(get_current_user)):
    """Allow updating mutable fields on event : event_name, org_name, month, sender_email, city, venue, date2, note."""
    if not payload:
        raise HTTPException(status_code=400, detail="No updatable fields provided")
    allowed_fields = {'event_name', 'org_name', 'month', 'sender_email', 'city', 'venue', 'date2', 'note', 'event_url'}
    updates = {k: v for k, v in payload.items() if k in allowed_fields}
    if not updates:
        raise HTTPException(status_code=400, detail="No valid updatable fields provided")
    
    async with db_pool.acquire() as conn:
        # ensure event exists
        ev = await conn.fetchrow("SELECT id FROM event WHERE id = $1", event_id)
        if not ev:
            raise HTTPException(status_code=404, detail="Event not found")
        
        # Handle sender_email auto-assignment if "auto" is provided
        if 'sender_email' in updates and updates['sender_email'] and updates['sender_email'].lower() == "auto":
            # Get event's current contacts count for sizing
            contacts_result = await conn.fetchval(
                "SELECT COUNT(*) FROM campaign_contacts WHERE event_id = $1",
                event_id
            )
            event_size = contacts_result or 0
            updates['sender_email'] = await get_auto_assigned_sender(conn, event_size)
            logger.info(f"Auto-assigned sender email: {updates['sender_email']} for event {event_id} with {event_size} contacts")
        
        set_parts = []
        vals = []
        i = 1
        for k, v in updates.items():    
            set_parts.append(f"{k} = ${i}")
            vals.append(v)
            i += 1
        vals.append(event_id)
        sql = f"UPDATE event SET {', '.join(set_parts)} WHERE id = ${i}"
        await conn.execute(sql, *vals)
        row = await conn.fetchrow("SELECT e.*, COALESCE(e.org_name, o.name) AS org_name FROM event e LEFT JOIN organizations o ON e.org_id = o.id WHERE e.id = $1", event_id)
        return {"event": dict(row)}

@app.get("/campaign_contacts/search")
async def search_contacts(
    event_id: Optional[int] = Query(None),
    query: Optional[str] = Query(None),
    current_user: dict = Depends(get_current_user)
):
    def sanitize_contact_row(row: dict) -> dict:
        # Remove raw binary attachment bytes from responses to avoid JSON encoding errors.
        d = dict(row) if row else {}
        try:
            if 'attachment' in d and isinstance(d.get('attachment'), (bytes, bytearray)):
                d['has_attachment'] = True
                # keep filename and mimetype if present
                d.pop('attachment', None)
            else:
                d['has_attachment'] = False
        except Exception:
            d.pop('attachment', None)
            d['has_attachment'] = False
        return d
    async with db_pool.acquire() as conn:
        if event_id is not None:
            rows = await conn.fetch("SELECT * FROM campaign_contacts WHERE event_id = $1 ORDER BY id", event_id)
            return {"contacts": [sanitize_contact_row(dict(r)) for r in rows]}
        elif query:
            q = (query or '').strip()
            # If user includes '+' tokens we allow matching across multiple columns.
            # Supported token order: stage + status + trigger  (trigger optional)
            if '+' in q:
                parts = [p.strip().lower() for p in q.split('+') if p.strip() != '']
                # Map parts to columns in order: stage, status, trigger
                cols = ['stage', 'status', 'trigger']
                where_clauses = []
                params = []
                for idx, part in enumerate(parts[: len(cols)]):
                    like = f"%{part}%" if part else '%'
                    where_clauses.append(f"LOWER(COALESCE(c.{cols[idx]},'')) LIKE ${len(params)+1}")
                    params.append(like)

                if where_clauses:
                    sql = f'''
                        SELECT c.*, e.event_name, e.id as event_id
                        FROM campaign_contacts c
                        JOIN event e ON c.event_id = e.id
                        WHERE {' AND '.join(where_clauses)}
                        ORDER BY c.id
                    '''
                    rows = await conn.fetch(sql, *params)
                    results = [sanitize_contact_row(dict(r)) for r in rows]
                    return {"results": results}

            # Default fallback: search by name or email
            sql = '''
                SELECT c.*, e.event_name, e.id as event_id
                FROM campaign_contacts c
                JOIN event e ON c.event_id = e.id
                WHERE LOWER(c.name) LIKE $1 OR LOWER(c.email) LIKE $1 OR LOWER(COALESCE(c.phone_number, '')) LIKE $1 
            '''
            rows = await conn.fetch(sql, f"%{q.lower()}%")
            results = [sanitize_contact_row(dict(r)) for r in rows]
            return {"results": results}
        else:
            return {"contacts": []}

@app.post("/campaign_contacts")
async def create_campaign_contact(contact: dict = Body(...), current_user: dict = Depends(get_current_user)):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            INSERT INTO campaign_contacts (name, email, cc_store, event_id, status, stage, forms_link, payment_link, campaign_paused, invoice_number)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING *
            """,
            contact.get("name"), contact.get("email"), contact.get("cc_store"), contact.get("event_id"),
            contact.get("status", "pending"), contact.get("stage", "initial"),
            contact.get("forms_link"), contact.get("payment_link"), contact.get("campaign_paused", False), contact.get("invoice_number")
        )

        # Log contact creation activity
        await log_user_activity(
            conn,
            current_user,
            "CREATE_CONTACT",
            f"Created new contact: {contact.get('name')} ({contact.get('email')})",
            target_type="contact",
            target_id=row['id'],
            target_name=contact.get('name'),
            new_values=dict(contact)
        )

        # Avoid returning raw bytes in 'attachment'
        def _sanitize(r):
            d = dict(r)
            if 'attachment' in d and isinstance(d.get('attachment'), (bytes, bytearray)):
                d.pop('attachment', None)
                d['has_attachment'] = True
            else:
                d['has_attachment'] = False
            return d

        return _sanitize(row) if row else {"error": "Failed to create contact"}

@app.api_route("/campaign_contacts/{contact_id}/links", methods=["PUT", "POST"])
async def update_contact_links(
    contact_id: int,
    forms_link: str = Form(...),
    payment_link: str = Form(...),
    attachment: UploadFile = File(None),
    invoice_number: str = Form(None),
    current_user: dict = Depends(get_current_user)
):
    print(f"[DEBUG] Received update for contact {contact_id}, forms_link={forms_link}, payment_link={payment_link}, attachment={attachment.filename if attachment else 'None'}")

    async with db_pool.acquire() as conn:
        contact = await conn.fetchrow(
            "SELECT * FROM campaign_contacts WHERE id = $1", contact_id
        )
        if not contact:
            raise HTTPException(status_code=404, detail="Contact not found")

        # If attachment provided, store bytes and metadata on the contact record
        attachment_bytes = None
        attachment_filename = None
        attachment_mimetype = None
        if attachment:
            attachment_bytes = await attachment.read()
            attachment_filename = attachment.filename
            try:
                attachment_mimetype = attachment.content_type
            except Exception:
                attachment_mimetype = None

        await conn.execute("""
            UPDATE campaign_contacts
            SET forms_link = $1, payment_link = $2,
                attachment = COALESCE($3, attachment),
                attachment_filename = COALESCE($4, attachment_filename),
                attachment_mimetype = COALESCE($5, attachment_mimetype),
                invoice_number = COALESCE($6, invoice_number)
            WHERE id = $7
        """, forms_link, payment_link, attachment_bytes, attachment_filename, attachment_mimetype, invoice_number, contact_id)

        # If we stored an attachment, propagate it to any pending queued messages for this contact
        if attachment_bytes:
            try:
                await conn.execute(
                    """
                    UPDATE email_queue
                    SET attachment = $1, attachment_filename = $2, attachment_mimetype = $3
                    WHERE contact_id = $4 AND status = 'pending'
                    """,
                    attachment_bytes, attachment_filename, attachment_mimetype, contact_id
                )
            except Exception as e:
                logger.error(f"[PROPAGATE] Failed to propagate attachment to pending email_queue rows for contact {contact_id}: {e}")

    return {"message": "Links updated successfully"}


# ============================================================================
# CONTACT CUSTOM MESSAGE ENDPOINTS
# ============================================================================

@app.get("/campaign_contacts/{contact_id}/messages")
async def get_contact_messages(
    contact_id: int,
    current_user: dict = Depends(get_current_user)
):
    """
    Get all message flows for a contact (initial + all reminders).
    Shows custom overrides and defaults from templates.
    """
    logger.info(f"[GET MESSAGES] Fetching messages for contact {contact_id}, user: {current_user.get('username', 'unknown')}")
    try:
        async with db_pool.acquire() as conn:
            flows = await get_contact_message_flows(conn, contact_id)
            logger.info(f"[GET MESSAGES] Successfully fetched {len(flows)} message flows for contact {contact_id}")
            return flows
    except ValueError as e:
        logger.error(f"[GET MESSAGES] ValueError for contact {contact_id}: {e}")
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"[GET MESSAGES] Error fetching messages for contact {contact_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to fetch messages: {str(e)}")


@app.post("/campaign_contacts/{contact_id}/messages/{message_type}")
async def save_contact_message(
    contact_id: int,
    message_type: str,
    payload: dict = Body(...),
    current_user: dict = Depends(get_current_user)
):
    """
    Save a custom message for a specific contact.
    This overrides the template for this message type.
    
    Request body:
    {
        "subject": "Custom Subject",
        "body": "Custom email body..."
    }
    """
    subject = payload.get('subject', '').strip()
    body = payload.get('body', '').strip()
    
    if not subject:
        raise HTTPException(status_code=400, detail="Subject cannot be empty")
    if not body:
        raise HTTPException(status_code=400, detail="Body cannot be empty")
    
    try:
        async with db_pool.acquire() as conn:
            result = await save_contact_custom_message(
                conn, contact_id, message_type, subject, body, current_user['username']
            )
            
            # Log activity
            await conn.execute("""
                INSERT INTO user_activity_logs (username, action_type, action_description, target_type, target_id, timestamp)
                VALUES ($1, $2, $3, $4, $5, NOW())
            """, current_user['username'], 'CUSTOM_MESSAGE_SAVED', 
                f"Saved custom message {message_type} for contact {contact_id}", 
                'contact_custom_message', contact_id)
            
            return result
    
    except Exception as e:
        logger.error(f"[SAVE MESSAGE] Error saving custom message for contact {contact_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to save message")


@app.delete("/campaign_contacts/{contact_id}/messages/{message_type}")
async def delete_contact_message(
    contact_id: int,
    message_type: str,
    current_user: dict = Depends(get_current_user)
):
    """
    Delete a custom message for a contact.
    System will revert to using templates.
    """
    try:
        async with db_pool.acquire() as conn:
            await delete_contact_custom_message(conn, contact_id, message_type)
            
            # Log activity
            await conn.execute("""
                INSERT INTO user_activity_logs (username, action_type, action_description, target_type, target_id, timestamp)
                VALUES ($1, $2, $3, $4, $5, NOW())
            """, current_user['username'], 'CUSTOM_MESSAGE_DELETED', 
                f"Deleted custom message {message_type} for contact {contact_id}", 
                'contact_custom_message', contact_id)
            
            return {"message": f"Custom message {message_type} deleted"}
    
    except Exception as e:
        logger.error(f"[DELETE MESSAGE] Error deleting custom message for contact {contact_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to delete message")


@app.get("/campaign_contacts/{contact_id}/attachment")
async def get_contact_attachment(contact_id: int, current_user: dict = Depends(get_current_user)):
    """Return the binary attachment for a campaign contact (if present).
    This endpoint streams the attachment bytes with the stored mimetype and filename.
    Requires authentication.
    """
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT attachment, attachment_filename, attachment_mimetype FROM campaign_contacts WHERE id = $1", contact_id)
        if not row:
            raise HTTPException(status_code=404, detail="Contact not found")
        attachment = row.get('attachment')
        if not attachment:
            raise HTTPException(status_code=404, detail="Attachment not found")
        filename = row.get('attachment_filename') or f'attachment_{contact_id}'
        mimetype = row.get('attachment_mimetype') or 'application/octet-stream'
        # attachment is stored as bytes (bytea). Return raw bytes with proper media type.
        return Response(content=bytes(attachment), media_type=mimetype, headers={"Content-Disposition": f'inline; filename="{filename}"'})


@app.delete("/campaign_contacts/{contact_id}/attachment")
async def delete_contact_attachment(contact_id: int, current_user: dict = Depends(get_current_user)):
    """Delete attachment bytes/metadata from a campaign contact record."""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id FROM campaign_contacts WHERE id = $1", contact_id)
        if not row:
            raise HTTPException(status_code=404, detail="Contact not found")
        await conn.execute("UPDATE campaign_contacts SET attachment = NULL, attachment_filename = NULL, attachment_mimetype = NULL WHERE id = $1", contact_id)
    return {"message": "Attachment deleted"}


@app.options("/campaign_contacts/{contact_id}")
async def options_campaign_contact(contact_id: int, request: Request):
    """Respond to CORS preflight for campaign contact endpoints."""
    # Let the middleware add CORS/security headers
    return Response(status_code=200)


@app.options("/campaign_contacts/{contact_id}/links")
async def options_campaign_contact_links(contact_id: int, request: Request):
    return Response(status_code=200)


@app.put("/campaign_contacts/{contact_id}")
async def update_campaign_contact(contact_id: int, contact: dict = Body(...), current_user: dict = Depends(get_current_user)):
    logger.info(f"[STAGE CHANGE] Updating contact {contact_id} with data: {contact}")

    # Get current contact data to detect stage changes
    async with db_pool.acquire() as conn:
        current_contact = await conn.fetchrow(
            "SELECT * FROM campaign_contacts WHERE id = $1", contact_id
        )

        if not current_contact:
            raise HTTPException(status_code=404, detail="Contact not found")

        old_stage = (current_contact['stage'] or '').lower()
        new_stage = (contact.get('stage', current_contact['stage']) or '').lower()
        stage_changing = old_stage != new_stage
        
        # Check if status is changing from 'Replied' to 'Pending' with same stage
        old_status = (current_contact['status'] or '').lower()
        new_status = (contact.get('status', current_contact['status']) or '').lower()
        is_replied_to_pending = old_status == 'replied' and new_status == 'pending' and not stage_changing

        logger.info(f"[STAGE CHANGE] Contact {contact_id}: old_stage='{old_stage}' -> new_stage='{new_stage}', changing={stage_changing}")
        if is_replied_to_pending:
            logger.info(f"[REPLIED_TO_PENDING] Contact {contact_id}: Transitioning from 'Replied' to 'Pending' with same stage '{old_stage}' - will complete flow and resume campaign")

        # Ensure any update assigns the contact to the user performing the update
        try:
            contact['assigned_to'] = current_user.get('user_id')
            logger.info(f"[CONTACT ASSIGN] Assigning campaign contact {contact_id} to user {current_user.get('user_id')}")
        except Exception:
            # safe-ignore if current_user structure is unexpected
            pass

        # Build dynamic update query
        fields = []
        values = []
        for k, v in contact.items():
            fields.append(f"{k} = ${len(values)+1}")
            values.append(v)

        # Auto-set status to "pending" when stage changes
        if stage_changing:
            # When stage changes, pause the contact so no emails are sent until
            # the operator reviews/updates the email and explicitly resumes.
            if 'status' not in contact:
                fields.append(f"status = ${len(values)+1}")
                values.append('pending')
            fields.append(f"last_triggered_at = ${len(values)+1}")
            values.append(None)
            # CRITICAL FIX: Reset last_message_type when stage changes so that the campaign
            # worker starts fresh with the appropriate initial message (e.g., forms_initial)
            # instead of continuing from the previous stage's reminders.
            # This prevents the bug where forms_reminder3 from an old campaign would skip forms_initial.
            fields.append(f"last_message_type = ${len(values)+1}")
            values.append(None)
            # Pause campaigns on stage change to prevent immediate sends
            fields.append(f"campaign_paused = ${len(values)+1}")
            values.append(True)
            logger.info(f"[AUTO-STATUS] Stage changed from '{old_stage}' to '{new_stage}', setting status to 'pending', resetting last_message_type to NULL, and pausing campaign for manual review")
        
        # Handle Replied -> Pending transition with same stage (complete flow and resume)
        # Handle Replied -> Pending transition with same stage (complete flow and resume)
        elif is_replied_to_pending:
            # 1. Resume the campaign
            fields.append(f"campaign_paused = ${len(values)+1}")
            values.append(False)

            # 2. Add trigger note
            trigger_message = f"[{datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}] Contact status changed from Replied to Pending - Flow completed and campaign resumed by {current_user.get('username') or 'system'}"
            fields.append(f"trigger = COALESCE(trigger || E'\\n', '') || ${len(values)+1}")
            values.append(trigger_message)
            
            # ------------------------------------------------------------------
            # 3. FORCE IMMEDIATE SEND (The "Time Travel" Fix)
            # We backdate the last 'sent' email by 7 days.
            # The worker will wake up, see that "7 days have passed," and trigger the next step NOW.
            # ------------------------------------------------------------------
            try:
                await conn.execute("""
                    UPDATE email_queue 
                    SET sent_at = sent_at - INTERVAL '7 days'
                    WHERE contact_id = $1 
                      AND status = 'sent' 
                      AND sent_at > (NOW() - INTERVAL '7 days')
                """, contact_id)
                logger.info(f"[FORCE RESUME] Backdated recent sent emails for contact {contact_id} to force immediate progression")
            except Exception as e:
                logger.error(f"[FORCE RESUME] Failed to backdate emails for contact {contact_id}: {e}")
            # ------------------------------------------------------------------

            logger.info(f"[REPLIED_TO_PENDING] Contact {contact_id}: Campaign resumed, flow completed. Trigger updated.")

        if not fields:
            raise HTTPException(status_code=400, detail="No fields to update")

        values.append(contact_id)
        sql = f"UPDATE campaign_contacts SET {', '.join(fields)} WHERE id = ${len(values)} RETURNING *"

        row = await conn.fetchrow(sql, *values)
          # If stage changed, remove any pending queued messages for this contact so
        # the old reminder does not get sent.
        if stage_changing:
            try:
                await conn.execute(
                    "DELETE FROM email_queue WHERE contact_id = $1 AND status = 'pending'",
                    contact_id
                )
                logger.info(f"[STAGE CHANGE] Removed pending email_queue rows for contact {contact_id} after stage change '{old_stage}' -> '{new_stage}'")
            except Exception as e:
                logger.error(f"[STAGE CHANGE] Failed to remove pending email_queue rows for contact {contact_id}: {e}")
        
        # If contact transitions from Replied to Pending, clear the reply indicators
        elif is_replied_to_pending:
            try:
                # Clear the last received reply so campaign continues fresh
                await conn.execute(
                    "UPDATE campaign_contacts SET last_reply_body = NULL, last_reply_at = NULL WHERE id = $1",
                    contact_id
                )
                logger.info(f"[REPLIED_TO_PENDING] Cleared last_reply indicators for contact {contact_id}")
            except Exception as e:
                logger.error(f"[REPLIED_TO_PENDING] Failed to clear reply indicators for contact {contact_id}: {e}")
        # Log contact update activity - use STAGE_CHANGE if stage changed, otherwise UPDATE_CONTACT
        action_type = "STAGE_CHANGE" if stage_changing else "UPDATE_CONTACT"
        action_description = f"{'Stage changed from ' + old_stage + ' to ' + new_stage if stage_changing else 'Updated contact'}: {contact.get('name') or current_contact['name']} ({current_contact['email']})"

        await log_user_activity(
            conn,
            current_user,
            action_type,
            action_description,
            target_type="contact",
            target_id=contact_id,
            target_name=current_contact['name'],
            old_values=dict(current_contact),
            new_values=dict(contact)
        )

        # If operator explicitly set status to 'ooo', ensure pending queue rows
        # are not paused and annotate them so the send worker will continue sending.
        try:
            if 'status' in contact and str(contact.get('status','')).strip().lower() == 'ooo':
                meta = {'user_set_status': 'ooo', 'by': current_user.get('username') if current_user else None, 'at': datetime.utcnow().isoformat()}
                await conn.execute(
                    """
                    UPDATE email_queue
                    SET paused = FALSE,
                        metadata = COALESCE(metadata, '{}'::jsonb) || $1
                    WHERE contact_id = $2 AND status = 'pending'
                    """,
                    json.dumps(meta), contact_id
                )
                logger.info(f"[SYNC] Operator set status=ooo for contact {contact_id}; unpaused pending email_queue rows and annotated metadata")
        except Exception as e:
            logger.error(f"[SYNC] Failed to update email_queue for contact {contact_id} after setting status=ooo: {e}")

        # If operator set status to 'ooo' we also delete the last received reply
        # from the customer so the flow proceeds without treating that reply as a
        # user response. We keep an audit via log_user_activity.
        try:
            if 'status' in contact and str(contact.get('status','')).strip().lower() == 'ooo':
                async with conn.transaction():
                    last_recv = await conn.fetchrow(
                        "SELECT id, message_id, received_at, subject FROM messages WHERE contact_id = $1 AND direction = 'received' ORDER BY received_at DESC LIMIT 1",
                        contact_id
                    )
                    if last_recv:
                        msg_id = last_recv['id']
                        msg_mid = (last_recv.get('message_id') or '').strip(' <>')
                        # Delete mapping entries if present
                        try:
                            if msg_mid:
                                await conn.execute('DELETE FROM message_contact_map WHERE message_id = $1', msg_mid)
                        except Exception:
                            pass

                        # Delete the message row
                        await conn.execute('DELETE FROM messages WHERE id = $1', msg_id)

                        # Clear last_reply columns on campaign_contacts if they match
                        try:
                            await conn.execute("""
                                UPDATE campaign_contacts
                                SET last_reply_body = NULL,
                                    last_reply_at = NULL,
                                    trigger = COALESCE(trigger || E'\n', '') || $1
                                WHERE id = $2
                            """, f"Operator set status=ooo and deleted last received message id={msg_id}", contact_id)
                        except Exception:
                            pass

                        # Log user activity for auditing
                        try:
                            await log_user_activity(
                                conn,
                                current_user,
                                'DELETE_LAST_REPLY_ON_OOO',
                                f"Operator set status=ooo and deleted last received message id={msg_id} for contact {contact_id}",
                                target_type='contact',
                                target_id=contact_id,
                                target_name=current_contact.get('name'),
                                old_values={'deleted_message': dict(last_recv)},
                                new_values={'status': 'ooo'}
                            )
                        except Exception:
                            pass
                        logger.info(f"[OOO ACTION] Deleted last received message id={msg_id} for contact {contact_id} on operator-set OOO")
        except Exception as e:
            logger.error(f"[OOO ACTION] Error deleting last received message for contact {contact_id}: {e}")

        # If the main email or cc_store changed, normalize and sync pending email_queue rows
        try:
            if 'email' in contact or 'cc_store' in contact:
                # Build normalized full email list and cc list according to upload rules
                provided_email_raw = (contact.get('email') or current_contact.get('email') or '').strip()

                # If the user provided cc_store explicitly, use it (split by comma)
                cc_from_payload = []
                if 'cc_store' in contact:
                    cc_raw = (contact.get('cc_store') or '').strip()
                    if cc_raw:
                        cc_from_payload = [normalize_email(x) for x in re.split(r'[,;]', cc_raw) if x.strip()]

                # Parse the provided email field into a list (primary + extras)
                parsed_emails = process_emails(provided_email_raw or '', validate=True)

                # If cc_store provided use that (including empty string to clear);
                # otherwise derive CCs directly from the provided email list. This
                # allows operators to remove CCs by editing the email field and not
                # specifying cc_store (i.e. deletion is respected).
                if 'cc_store' in contact:
                    cc_list = []
                    for c in cc_from_payload:
                        if c and c not in cc_list:
                            cc_list.append(c)
                else:
                    # Derive CCs from parsed_emails extras (if any). If the user removed
                    # extras from the email field, this will result in an empty cc_list,
                    # which clears cc_store.
                    cc_list = parsed_emails[1:] if len(parsed_emails) > 1 else []

                # Build final deduped full email list preserving primary first
                final_list = []
                seenf = set()
                for e in (parsed_emails + cc_list):
                    if e and e not in seenf:
                        seenf.add(e)
                        final_list.append(e)

                # Ensure primary is first element
                if parsed_emails:
                    primary_email = parsed_emails[0]
                    if final_list and final_list[0] != primary_email:
                        # move primary to front if necessary
                        final_list = [primary_email] + [x for x in final_list if x != primary_email]
                else:
                    primary_email = current_contact.get('email')

                # Serialize for DB: email column gets full comma-separated list; cc_store gets CC-only
                email_column_value = ','.join(final_list) if final_list else (current_contact.get('email') or None)
                cc_store_value = ','.join([c for c in final_list[1:]]) if len(final_list) > 1 else None

                # Append removed CCs to cc_history JSONB (store timestamp and user)
                try:
                    # Load existing history (safe default to empty list)
                    existing_history = current_contact.get('cc_history') if current_contact.get('cc_history') is not None else '[]'
                    try:
                        existing_history_list = json.loads(existing_history) if isinstance(existing_history, (str, bytes)) else existing_history
                    except Exception:
                        existing_history_list = existing_history or []

                    # Determine previous CC list
                    prev_ccs = []
                    if current_contact.get('cc_store'):
                        prev_ccs = [c.strip() for c in str(current_contact.get('cc_store')).split(',') if c.strip()]
                    else:
                        # derive from previous email field
                        prev_email_raw = current_contact.get('email') or ''
                        prev_parts = [p.strip() for p in re.split(r'[;,\s]+', prev_email_raw) if p.strip()]
                        prev_ccs = prev_parts[1:] if len(prev_parts) > 1 else []

                    # New CCs
                    new_ccs = [c for c in final_list[1:]] if len(final_list) > 1 else []

                    # Removed CCs = prev_ccs - new_ccs
                    removed = [c for c in prev_ccs if c and c not in new_ccs]
                    if removed:
                        entry = {
                            'removed_at': datetime.utcnow().isoformat(),
                            'emails': removed,
                            'by': current_user.get('username') if current_user else None
                        }
                        existing_history_list.append(entry)
                        # Persist updated cc_history JSONB
                        await conn.execute(
                            "UPDATE campaign_contacts SET cc_history = $1 WHERE id = $2",
                            json.dumps(existing_history_list), contact_id
                        )
                except Exception as e:
                    logger.error(f"[CC_HISTORY] Failed to append cc_history for contact {contact_id}: {e}")

                # Persist normalized values back to contact record
                await conn.execute(
                    "UPDATE campaign_contacts SET email = $1, cc_store = $2 WHERE id = $3",
                    email_column_value, cc_store_value, contact_id
                )

                # Update any pending queued messages to use only the first email as recipient
                cc_serialized_for_queue = ';'.join([c for c in final_list[1:]]) if len(final_list) > 1 else None
                await conn.execute(
                    """
                    UPDATE email_queue
                    SET recipient_email = $1,
                        cc_recipients = $2
                    WHERE contact_id = $3 AND status = 'pending'
                    """,
                    final_list[0] if final_list else primary_email, cc_serialized_for_queue, contact_id
                )

                logger.info(f"[SYNC] Normalized contact {contact_id} email -> '{email_column_value}' cc_store -> '{cc_store_value}' and updated pending queue entries")
        except Exception as e:
            logger.error(f"[SYNC] Failed to sync email_queue for contact {contact_id}: {e}")

    # When stage changes we DO NOT trigger immediate processing. The contact is
    # paused so an operator can update email/cc and then call the resume
    # endpoint to allow sending to continue.

        # Sanitize before returning
        if row:
            d = dict(row)
            if 'attachment' in d and isinstance(d.get('attachment'), (bytes, bytearray)):
                d.pop('attachment', None)
                d['has_attachment'] = True
            else:
                d['has_attachment'] = False
            return d
        else:
            return {"error": "Contact not found"}


@app.patch("/campaign_contacts/{contact_id}")
async def patch_campaign_contact(contact_id: int, contact: dict = Body(...), current_user: dict = Depends(get_current_user)):
    """Partial update for campaign contact - delegates to the PUT handler logic which supports partial fields."""
    # Reuse the same update logic implemented for PUT
    return await update_campaign_contact(contact_id, contact, current_user)

@app.delete("/campaign_contacts/{contact_id}")
async def delete_campaign_contact(contact_id: int, current_user: dict = Depends(get_current_user)):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            # Get contact info before deletion for logging
            contact_info = await conn.fetchrow("SELECT * FROM campaign_contacts WHERE id = $1", contact_id)

            if not contact_info:
                raise HTTPException(status_code=404, detail="Contact not found")

            # Delete from email_queue first (foreign key to campaign_contacts)
            await conn.execute("DELETE FROM email_queue WHERE contact_id = $1", contact_id)

            # Delete from messages table
            await conn.execute("DELETE FROM messages WHERE contact_id = $1", contact_id)

            # Finally delete from campaign_contacts
            row = await conn.fetchrow("DELETE FROM campaign_contacts WHERE id = $1 RETURNING id", contact_id)

            # Log contact deletion activity
            await log_user_activity(
                conn,
                current_user,
                "DELETE_CONTACT",
                f"Deleted contact: {contact_info['name']} ({contact_info['email']})",
                target_type="contact",
                target_id=contact_id,
                target_name=contact_info['name'],
                old_values=dict(contact_info)
            )

            if not row:
                raise HTTPException(status_code=404, detail="Contact not found")

        return {"deleted": True, "message": "Contact and all related records deleted successfully"}

@app.post("/campaign_contacts/upload-excel")
async def upload_excel_campaign_contacts(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
    download: bool = Query(False),
    preview: bool = Query(False, description="If true: validate the Excel and return detected matches without persisting")
):
    import pandas as pd
    added, updated, skipped = [], [], []

    try:
        # Read Excel file
        try:
            contents = await file.read()
            df = pd.read_excel(contents)
        except Exception as e:
            logger.error(f"Failed to read Excel file: {str(e)}")
            raise HTTPException(status_code=400, detail=f"Invalid Excel file: {str(e)}")

        # Replace NaN values with None
        df = df.where(pd.notna(df), None)

        # Early duplicate-check / preview: collect emails and names and prefetch candidates
        try:
            emails = []
            names = []
            for r in df.to_dict(orient='records'):
                e = (r.get('email') or '')
                if isinstance(e, str):
                    el = e.strip().lower()
                    if el and el not in emails:
                        emails.append(el)
                n = (r.get('name') or '')
                if isinstance(n, str):
                    nl = n.strip().lower()
                    if nl and nl not in names:
                        names.append(nl)

            email_eq_array = emails if emails else ['']
            email_like_array = [f'%{e}%' for e in emails] if emails else ['']
            name_eq_array = names if names else ['']

            async with db_pool.acquire() as _conn:
                # Log that we're about to run the duplicate matching query for debugging
                logger.debug(f"[EXCEL UPLOAD] Running preview duplicate-match query: emails={len(email_eq_array)} names={len(name_eq_array)} preview={preview}")
                sql = """
                    SELECT id, name, email, event_id
                    FROM campaign_contacts
                    WHERE (LOWER(email) = ANY($1) OR LOWER(email) LIKE ANY($2) OR (name IS NOT NULL AND LOWER(name) = ANY($3)))
                """
                fetched = await _conn.fetch(sql, email_eq_array, email_like_array, name_eq_array)
                fetched_list = [dict(r) for r in fetched]

            # If preview requested, build per-row validation+match results and return without persisting
            if preview:
                preview_results = []
                rows = df.to_dict(orient='records')
                for idx, r in enumerate(rows):
                    row_info = {'row': idx + 2, 'email': r.get('email'), 'name': r.get('name')}
                    raw_email = r.get('email')
                    if raw_email is None:
                        row_info.update({'status': 'skipped', 'reason': 'Missing email'})
                        preview_results.append(row_info)
                        continue

                    emails_list = process_emails(str(raw_email), validate=True)
                    if not emails_list:
                        row_info.update({'status': 'skipped', 'reason': 'No valid emails'})
                        preview_results.append(row_info)
                        continue

                    primary_email = emails_list[0]
                    # call validator (best-effort) similar to background job
                    validation_result_text = None
                    validator_info = {'code': None, 'reason': None, 'valid': False, 'raw': None}
                    try:
                        validator_info = await call_validator(primary_email)
                        validation_result_text = validator_info.get('reason') or validator_info.get('validation_result') or ("Valid" if validator_info.get('valid') else "Invalid")
                    except Exception as _:
                        validation_result_text = None

                    # find matches from prefetched candidates
                    matches = []
                    try:
                        name_val = (r.get('name') or '').strip().lower()
                        # normalize primary email for comparisons
                        p_email = (primary_email or '').strip().lower()

                        def normalize_name(n: Optional[str]) -> str:
                            if not n:
                                return ''
                            # collapse whitespace and lower
                            return re.sub(r"\s+", ' ', n).strip().lower()

                        for f in fetched_list:
                            try:
                                # Stored email(s) in DB may be a comma/semicolon-separated list.
                                # Split using the same helper we use for incoming data to ensure consistent normalization.
                                f_raw_email = f.get('email') or ''
                                f_emails = process_emails(str(f_raw_email), validate=False) if f_raw_email else []
                                f_emails_lower = [e.strip().lower() for e in f_emails if e]

                                f_name_raw = f.get('name') or ''
                                f_name_norm = normalize_name(f_name_raw)

                                matched = False
                                # Exact or contained email match against any stored address
                                if p_email:
                                    for fe in f_emails_lower:
                                        if fe == p_email or p_email in fe or fe in p_email:
                                            matches.append({'id': f.get('id'), 'name': f.get('name'), 'email': f.get('email'), 'event_id': f.get('event_id')})
                                            matched = True
                                            break
                                if matched:
                                    continue

                                # Name exact match (normalized)
                                if name_val:
                                    if f_name_norm and normalize_name(name_val) == f_name_norm:
                                        matches.append({'id': f.get('id'), 'name': f.get('name'), 'email': f.get('email'), 'event_id': f.get('event_id')})
                                        continue
                            except Exception:
                                continue
                    except Exception:
                        matches = []

                    row_info.update({'status': 'validated', 'validation_result': validation_result_text, 'validator_code': validator_info.get('code'), 'matches': matches})
                    preview_results.append(row_info)

                summary = {'total_rows': len(preview_results), 'matched_rows': sum(1 for r in preview_results if r.get('matches'))}
                logger.debug(f"[EXCEL UPLOAD] Preview summary: {summary}")
                return {'status': 'preview', 'results': preview_results, 'summary': summary}
        except Exception as e:
            logger.exception(f"Failed during preview duplicate-check: {e}")
            # fall through to normal processing (do not block upload if preview logic fails)
            pass

        # Validate required columns
        required_columns = ['email', 'event_id']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns: {', '.join(missing_columns)}"
            )

        async with db_pool.acquire() as conn:
            # Get all available columns (including dynamic ones) while we have a connection
            available_columns = await conn.fetch("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'campaign_contacts'
                AND table_schema = 'public'
            """)
            available_column_names = {col['column_name'] for col in available_columns}

            # Filter Excel columns to only include available database columns
            valid_excel_columns = [col for col in df.columns if col.lower() in available_column_names]
            ignored_columns = [col for col in df.columns if col.lower() not in available_column_names]

            if ignored_columns:
                logger.warning(f"[EXCEL UPLOAD] Ignoring columns not in database: {ignored_columns}")

            # Collect per-row result rows for optional Excel download
            result_rows = []

            for idx, row in df.iterrows():
                try:
                    # Clean and normalize the email
                    raw_email = row.get('email')
                    if raw_email is None or (isinstance(raw_email, float) and pd.isna(raw_email)):
                        skipped.append({
                            'row': idx + 2,
                            'reason': 'Missing email',
                            'data': row.to_dict()
                        })
                        continue

                    # Process emails in the email column (comma-separated)
                    emails_list = process_emails(str(raw_email), validate=True)

                    if not emails_list:
                        skipped.append({
                            'row': idx + 2,
                            'reason': 'No valid emails found',
                            'data': row.to_dict()
                        })
                        continue

                    event_id = row.get('event_id')
                    if pd.isna(event_id):
                        skipped.append({
                            'row': idx + 2,
                            'reason': 'Missing event_id',
                            'data': row.to_dict()
                        })
                        continue

                    # Prepare shared data for all contacts
                    name = row.get('name')
                    if pd.isna(name):
                        name = None

                    source = row.get('source')
                    if pd.isna(source):
                        source = None

                    speaker_type = row.get('speaker_type')
                    if pd.isna(speaker_type):
                        speaker_type = None

                    # Build full email list (preserve order). Use validate/cleanup done by process_emails
                    primary_and_others = emails_list
                    primary_email = primary_and_others[0]
                    cc_emails = primary_and_others[1:] if len(primary_and_others) > 1 else []

                    # Deduplicate while preserving order: keep first occurrence
                    seen = set()
                    full_list = []
                    for e in primary_and_others:
                        if e and e not in seen:
                            seen.add(e)
                            full_list.append(e)

                    # email column stores the full comma-separated list (main + CCs)
                    email_column_value = ','.join(full_list)

                    # cc_store stores only the CC emails (deduped)
                    CCs = []
                    for e in full_list[1:]:
                        if e and e not in CCs:
                            CCs.append(e)
                    cc_store_str = ','.join(CCs) if CCs else None

                    # Call external validator for primary email before saving (use centralized helper)
                    validation_result_text = None
                    validator_code = None
                    try:
                        v = await call_validator(primary_email)
                        validation_result_text = v.get('reason') or v.get('validation_result') or ("Valid" if v.get('valid') else "Invalid")
                        if isinstance(v.get('raw'), dict):
                            validator_code = v['raw'].get('smtp_code') or v['raw'].get('code') or v.get('code')
                    except Exception as e:
                        logger.warning(f"Validator call failed for {primary_email}: {e}")
                        validation_result_text = f"Validator error: {str(e)}"

                    # Check if a contact already exists for this event by primary email
                    existing = await conn.fetchrow(
                        "SELECT id, email, cc_store FROM campaign_contacts WHERE LOWER(email) = LOWER($1) AND event_id = $2",
                        primary_email, event_id
                    )

                    if existing:
                        # Update existing contact: update name and ensure email column contains full list
                        # Merge existing email list with new one (prefer new order)
                        try:
                            existing_emails = process_emails(existing.get('email') or '', validate=False)
                        except Exception:
                            existing_emails = []

                        merged = []
                        seen_m = set()
                        for e in full_list + existing_emails:
                            if e and e not in seen_m:
                                seen_m.add(e)
                                merged.append(e)

                        merged_email_str = ','.join(merged)

                        # Merge cc_store similarly
                        existing_cc = []
                        if existing.get('cc_store'):
                            existing_cc = [c.strip() for c in existing.get('cc_store').split(',') if c.strip()]

                        merged_cc = []
                        seen_cc = set()
                        for c in CCs + existing_cc:
                            if c and c not in seen_cc:
                                seen_cc.add(c)
                                merged_cc.append(c)

                        merged_cc_str = ','.join(merged_cc) if merged_cc else None

                        await conn.execute(
                            "UPDATE campaign_contacts SET name = $1, email = $2, cc_store = $3, validation_result = $4 WHERE id = $5",
                            name, merged_email_str, merged_cc_str, validation_result_text, existing['id']
                        )

                        updated.append({
                            'email': primary_email,
                            'email_list': merged_email_str,
                            'cc_store': merged_cc_str,
                            'cc_count': len(merged_cc) if merged_cc else 0,
                            'row': idx + 2,
                            'validation_result': validation_result_text
                        })

                        result_rows.append({
                            'row': idx + 2,
                            'email': primary_email,
                            'status': 'updated',
                            'validation_result': validation_result_text
                        })
                    else:
                        # Build dynamic INSERT query for all available columns
                        dynamic_fields = []
                        dynamic_values = []

                        # Add standard fields (store full email list and cc_store separately)
                        dynamic_fields.extend(['name', 'email', 'cc_store', 'event_id', 'source', 'speaker_type', 'status', 'stage', 'campaign_paused'])
                        dynamic_values.extend([name, email_column_value, cc_store_str, event_id, source, speaker_type, 'pending', 'initial', True])

                        # Add dynamic columns from Excel
                        for col in valid_excel_columns:
                            if col.lower() not in ['name', 'email', 'event_id', 'source', 'speaker_type', 'status', 'stage', 'campaign_paused', 'cc_store']:
                                value = row.get(col)
                                if pd.notna(value):  # Only add non-null values
                                    dynamic_fields.append(col.lower())
                                    dynamic_values.append(value)

                        # Ensure validation_result is saved
                        if 'validation_result' not in dynamic_fields:
                            dynamic_fields.append('validation_result')
                            dynamic_values.append(validation_result_text)

                        # Build the INSERT query dynamically
                        placeholders = ', '.join([f'${i+1}' for i in range(len(dynamic_values))])
                        fields_str = ', '.join(dynamic_fields)

                        insert_sql = f"""
                            INSERT INTO campaign_contacts ({fields_str})
                            VALUES ({placeholders})
                            RETURNING id
                        """

                        new_contact = await conn.fetchrow(insert_sql, *dynamic_values)

                        added.append({
                            'email': primary_email,
                            'email_list': email_column_value,
                            'cc_store': cc_store_str,
                            'cc_count': len(CCs),
                            'row': idx + 2,
                            'validation_result': validation_result_text
                        })

                        result_rows.append({
                            'row': idx + 2,
                            'email': primary_email,
                            'status': 'added',
                            'validation_result': validation_result_text
                        })

                except Exception as e:
                    logger.error(f"Database error processing row {idx + 2}: {str(e)}")
                    skipped.append({
                        'row': idx + 2,
                        'reason': f'Database error: {str(e)}',
                        'data': row.to_dict()
                    })
                    result_rows.append({
                        'row': idx + 2,
                        'email': row.get('email'),
                        'status': 'skipped',
                        'validation_result': None,
                        'reason': str(e)
                    })
                    continue

            # Log Excel upload activity
            # Count total matches
            total_matches = sum(1 for row in result_rows if row.get('matches'))
            
            await log_user_activity(
                conn,
                current_user,
                "EXCEL_UPLOAD",
                f"Uploaded Excel file with {len(added)} new contacts, {len(updated)} updated, {len(skipped)} skipped. Found {total_matches} existing matches.",
                target_type="bulk_upload",
                new_values={
                    "added_count": len(added),
                    "updated_count": len(updated),
                    "skipped_count": len(skipped),
                    "matches_count": total_matches,
                    "filename": file.filename
                }
            )
            # If client requested a validated Excel download, generate it
            if download:
                try:
                    from io import BytesIO
                    out_df = pd.DataFrame(result_rows)
                    buf = BytesIO()
                    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
                        out_df.to_excel(writer, index=False, sheet_name='validation_results')
                    buf.seek(0)
                    filename = f"validated_contacts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
                    headers = {"Content-Disposition": f"attachment; filename=\"{filename}\""}
                    return StreamingResponse(buf, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', headers=headers)
                except Exception as e:
                    logger.error(f"Failed to build validated Excel: {e}")
                    # fall back to JSON response

            # Return JSON results
            return {
                "status": "success",
                "summary": {
                    "total_rows": len(df),
                    "added": len(added),
                    "updated": len(updated),
                    "skipped": len(skipped)
                },
                "added": added,
                "updated": updated,
                "skipped": skipped
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error processing Excel file: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Error processing Excel file: {str(e)}"
        )

@app.post("/campaign_contacts/{contact_id}/pause")
async def pause_campaign_contact(contact_id: int, current_user: dict = Depends(get_current_user)):
    async with db_pool.acquire() as conn:
        now = datetime.now()
        trigger_text = f"{now.strftime('%Y-%m-%d %H:%M:%S')} - CAMPAIGN_PAUSED: Campaign manually paused by {current_user.get('username', 'system')}"

        # Get contact info for logging
        contact_info = await conn.fetchrow("SELECT * FROM campaign_contacts WHERE id = $1", contact_id)
        if not contact_info:
            raise HTTPException(status_code=404, detail="Contact not found")

        result = await conn.execute("""
            UPDATE campaign_contacts
            SET campaign_paused = TRUE,
                trigger = COALESCE(trigger || E'\n', '') || $1,
                last_triggered_at = $2,
                last_action_time = $2
            WHERE id = $3
            RETURNING id
        """, trigger_text, now, contact_id)

        # Auto-remove pending queue entries when contact is paused
        deleted_count = await conn.fetchval("""
            DELETE FROM email_queue
            WHERE contact_id = $1 AND status = 'pending'
        """, contact_id)

        # Log campaign pause activity
        await log_user_activity(
            conn,
            current_user,
            "PAUSE_CAMPAIGN",
            f"Paused campaign for contact: {contact_info['name']} ({contact_info['email']})",
            target_type="contact",
            target_id=contact_id,
            target_name=contact_info['name'],
            old_values={"campaign_paused": contact_info['campaign_paused']},
            new_values={"campaign_paused": True}
        )

        if not result:
            raise HTTPException(status_code=404, detail="Contact not found")

        logger.info(f"[CAMPAIGN] Campaign paused for contact {contact_id} by user {current_user.get('username', 'system')}. Removed {deleted_count or 0} pending queue entries.")
        return {"status": "success", "message": "Campaign paused for contact", "pending_queue_removed": deleted_count or 0}

@app.post("/campaign_contacts/{contact_id}/resume")
async def resume_campaign_contact(contact_id: int, background_tasks: BackgroundTasks, current_user: dict = Depends(get_current_user)):
    async with db_pool.acquire() as conn:
        # Get contact info for logging
        contact_info = await conn.fetchrow("SELECT * FROM campaign_contacts WHERE id = $1", contact_id)
        if not contact_info:
            raise HTTPException(status_code=404, detail="Contact not found")

        now = datetime.now()
        username = current_user.get('username', 'system')
        trigger_text = f"{now.strftime('%Y-%m-%d %H:%M:%S')} - CAMPAIGN_RESUMED: Campaign manually resumed by {username}"

        result = await conn.execute("""
            UPDATE campaign_contacts
            SET campaign_paused = FALSE,
                trigger = COALESCE(trigger || E'\n', '') || $1,
                last_triggered_at = $2,
                last_action_time = $2,
                last_message_type = NULL
            WHERE id = $3
            RETURNING id
        """, trigger_text, now, contact_id)

        # Log campaign resume activity
        await log_user_activity(
            conn,
            current_user,
            "RESUME_CAMPAIGN",
            f"Resumed campaign for contact: {contact_info['name']} ({contact_info['email']})",
            target_type="contact",
            target_id=contact_id,
            target_name=contact_info['name'],
            old_values={"campaign_paused": contact_info['campaign_paused']},
            new_values={"campaign_paused": False}
        )

        if not result:
            raise HTTPException(status_code=404, detail="Contact not found")
        # Schedule immediate processing of this contact to avoid waiting for the periodic worker
        try:
            background_tasks.add_task(process_single_contact_campaign, contact_id)
            logger.info(f"[CAMPAIGN] Scheduled immediate processing for contact {contact_id} after resume")
        except Exception as e:
            logger.error(f"[CAMPAIGN] Failed to schedule immediate processing for contact {contact_id}: {e}")

        logger.info(f"[CAMPAIGN] Campaign resumed for contact {contact_id} by user {username}")
        return {"status": "success", "message": "Campaign resumed for contact"}

@app.post("/email_queue/{contact_id}/start-campaign")
async def start_campaign_for_contact(
    contact_id: int,
    subject: str = Form(...),
    body: str = Form(...),
    forms_link: str = Form(None),
    payment_link: str = Form(None),
    attachment: UploadFile = File(None),
    current_user: dict = Depends(get_current_user)
):
    try:
        async with db_pool.acquire() as conn:
            now = datetime.now()
            username = current_user.get('username', 'system')

            # Get contact and event details
            contact = await conn.fetchrow(
                """
                SELECT cc.*, e.sender_email, e.event_name, e.city, e.date2, e.venue, e.month
                FROM campaign_contacts cc
                JOIN event e ON cc.event_id = e.id
                WHERE cc.id = $1
                """,
                contact_id
            )

            if not contact:
                raise HTTPException(status_code=404, detail="Contact not found")

            # Check for duplicate message
            duplicate = await conn.fetchrow(
                """
                SELECT id FROM email_queue
                WHERE contact_id = $1
                  AND recipient_email = $2
                  AND subject = $3
                  AND message = $4
                  AND status IN ('pending', 'sent')
                """,
                contact_id, contact['email'], subject, body
            )

            if duplicate:
                error_msg = f"Duplicate message detected for contact {contact_id} with subject: {subject}"
                logger.warning(f"[CAMPAIGN] {error_msg}")
                raise HTTPException(status_code=400, detail=error_msg)

            # Create detailed trigger text
            trigger_text = (
                f"{now.strftime('%Y-%m-%d %H:%M:%S')} - CAMPAIGN_STARTED: Campaign initiated by {username}\n"
                f"Template: campaign_default\n"
                f"Forms Link: {'Provided' if forms_link else 'Not provided'}\n"
                f"Payment Link: {'Provided' if payment_link else 'Not provided'}\n"
                f"Attachment: {'Included' if attachment else 'None'}"
            )

            # Save the first message template
            template_dir = Path("public/templates/emails")
            template_dir.mkdir(parents=True, exist_ok=True)

            # Save subject and body templates
            with open(template_dir / "campaign_default_subject.txt", "w") as f:
                f.write(subject)
            with open(template_dir / "campaign_default_body.txt", "w") as f:
                f.write(body)

            # Read attachment bytes (if provided) and store on contact so future reminders include it
            attachment_bytes = None
            attachment_filename = None
            attachment_mimetype = None
            if attachment:
                try:
                    attachment_bytes = await attachment.read()
                    attachment_filename = attachment.filename
                    attachment_mimetype = getattr(attachment, 'content_type', None)
                except Exception as e:
                    logger.error(f"[ATTACHMENT] Failed to read uploaded attachment for contact {contact_id}: {e}")

            # Update contact with campaign details and trigger
            updated_contact = await conn.fetchrow(
                """
                UPDATE campaign_contacts
                SET status = 'initial',
                    stage = 'initial',
                    trigger = COALESCE(trigger || E'\n', '') || $1,
                    last_triggered_at = $2,
                    last_action_time = $2,
                    last_message_type = 'initial',
                    campaign_paused = FALSE,
                    forms_link = COALESCE($3, forms_link),
                    payment_link = COALESCE($4, payment_link),
                    attachment = COALESCE($5, attachment),
                    attachment_filename = COALESCE($6, attachment_filename),
                    attachment_mimetype = COALESCE($7, attachment_mimetype)
                WHERE id = $5
                RETURNING *
                """,
                trigger_text,
                now,
                forms_link,
                payment_link,
                attachment_bytes,
                attachment_filename,
                attachment_mimetype,
                contact_id
            )

            if not updated_contact:
                raise HTTPException(status_code=500, detail="Failed to update contact")

            logger.info(f"[CAMPAIGN] Campaign started for contact {contact_id} by user {username}")

            # Queue the first email
            try:
                background_tasks.add_task(
                    process_single_contact_campaign,
                    contact_id=contact_id
                )
                logger.info(f"[CAMPAIGN] Successfully queued initial campaign email for contact {contact_id}")

                return {
                    "status": "success",
                    "message": "Campaign started successfully",
                    "contact_id": contact_id,
                    "started_at": now.isoformat(),
                    "initiated_by": username
                }

            except Exception as e:
                logger.error(f"[CAMPAIGN] Failed to queue initial campaign email for contact {contact_id}: {e}")
                raise HTTPException(status_code=500, detail="Failed to queue initial campaign email")

    except Exception as e:
        logger.error(f"Error in start_campaign_for_contact for contact_id={contact_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to start campaign: {str(e)}")

logger = logging.getLogger(__name__)

@app.post("/email_queue/bulk-start-campaign")
async def bulk_start_campaign(
    payload: List[dict] = Body(...),
    current_user: dict = Depends(get_current_user)
):
    now = datetime.now(UTC).replace(tzinfo=None)  # Convert to timezone-naive
    async with db_pool.acquire() as conn:
        # Log bulk campaign activity
        contact_ids = [item.get('contact_id') for item in payload]
        await log_user_activity(
            conn,
            current_user,
            "BULK_CAMPAIGN",
            f"Started bulk campaign for {len(payload)} contacts",
            target_type="campaign",
            new_values={"contact_count": len(payload), "contact_ids": contact_ids}
        )

        queued = []

        for item in payload:
            contact_id = item.get("contact_id")
            if not contact_id:
                logger.warning("Skipping item without contact_id")
                continue

            logger.info(f"Starting bulk campaign for contact_id={contact_id}")

            # Fetch contact with joined event data
            contact = await conn.fetchrow('''
                SELECT
                    cc.*,
                    e.sender_email, e.org_name, e.city, e.month, e.venue, e.date2
                    FROM campaign_contacts cc
                    JOIN event e ON cc.event_id = e.id
                WHERE cc.id = $1
            ''', contact_id)
            if not contact:
                logger.warning(f"Contact not found for ID {contact_id}")
                continue

            # Verify contact has required name
            if not contact.get('name'):
                logger.error(f"Contact {contact_id} missing required 'name' field")
                continue

            # 1. Create Base Context
            context = dict(contact)

            # =========================================================
            # START: NAME PROCESSING BLOCK (THE FIX)
            # =========================================================
            raw_name = context.get('name', '').strip()
            # Handle case where prefix might be None in DB
            db_prefix = context.get('prefix', '').strip() if context.get('prefix') else ""
            
            # Default fallbacks to prevent crashes
            final_prefix = ""
            final_last_name = raw_name

            if db_prefix:
                # CASE A: Database has a prefix (e.g. "prof")
                # Use DB prefix + Split the name to remove junk (e.g. "test4, kim" -> "kim")
                final_prefix = db_prefix
                
                # Polish punctuation (Add dot if missing, skip "Sir")
                if len(final_prefix) <= 3 and not final_prefix.endswith('.') and final_prefix.lower() not in ["sir", "madam"]:
                    final_prefix += "."

                if ',' in raw_name:
                    # "test4, kim" -> "kim"
                    final_last_name = raw_name.split(',')[-1].strip()
                else:
                    # "test4 kim" -> "kim"
                    parts = raw_name.split()
                    final_last_name = parts[-1].strip() if parts else raw_name
            else:
                # CASE B: No Database Prefix
                # Use helper function to extract everything
                try:
                    final_prefix, final_last_name = extract_name_parts_with_prefix(raw_name)
                except Exception as e:
                    logger.error(f"Name extraction failed for {contact_id}: {e}")
                    final_prefix = ""
                    final_last_name = raw_name

            # Capitalize
            final_prefix = final_prefix.title() if final_prefix else ""
            final_last_name = final_last_name.title()

            # Construct Greeting Name
            if final_prefix:
                greeting_name = f"{final_prefix} {final_last_name}"
            else:
                greeting_name = final_last_name

            # Assign Critical Variables to Context
            context['prefix'] = final_prefix
            context['last_name'] = final_last_name
            context['greeting_name'] = greeting_name
            context['name'] = greeting_name  # Ensure {{name}} uses the formatted version

            logger.info(f"Contact {contact_id}: Name Processed -> greeting_name='{greeting_name}'")
            # =========================================================
            # END: NAME PROCESSING BLOCK
            # =========================================================

            # Validate required vars (Now safe because we set them above)
            required_vars = ['name', 'city', 'date2', 'venue', 'month', 'greeting_name']
            missing_vars = [var for var in required_vars if not context.get(var)]
            if missing_vars:
                logger.error(f"Missing template vars for contact_id={contact_id}: {missing_vars}")
                continue

            # Get values
            sender_email = item.get("sender_email") or contact.get("sender_email")
            if sender_email not in ALLOWED_SENDERS:
                logger.warning(f"Invalid sender_email={sender_email}, skipping contact_id={contact_id}")
                continue

            subject = item.get("subject") or "Default Campaign Subject"
            body = item.get("body") or "Default Campaign Body"
            forms_link = item.get("forms_link") or contact.get("forms_link")
            payment_link = item.get("payment_link") or contact.get("payment_link")

            # Decode attachment
            attachment_bytes = None
            if item.get("attachment"):
                try:
                    base64_data = item["attachment"].split(",", 1)[-1]
                    attachment_bytes = base64.b64decode(base64_data)
                except Exception as e:
                    logger.error(f"Attachment decode failed for contact_id={contact_id}: {e}")

            # Render templates
            try:
                subject_rendered = render_template_strict(subject, context)
                body_rendered = render_template_strict(body, context)
                body_plain = body_rendered
                logger.info(f"Rendered templates for contact_id={contact_id}")
                
                # --- (Assuming your queueing/sending logic continues here) ---
                
            except Exception as e:
                logger.error(f"Template rendering failed for contact_id={contact_id}: {e}")
                continue

            # Prevent duplicate (same subject already sent or pending)
            existing = await conn.fetchval('''
                SELECT 1 FROM email_queue
                WHERE contact_id = $1 AND subject = $2 AND status IN ('pending', 'sent')
            ''', contact_id, subject_rendered)
            if existing:
                logger.warning(f"Duplicate prevention: Skipping contact_id={contact_id}")
                continue

            # --- Queue only (bulk) ---
            sender_stats = await conn.fetchrow('SELECT last_sent, cooldown FROM sender_stats WHERE sender_email = $1', sender_email)
            cooldown = sender_stats['cooldown'] if sender_stats else 120
            last_sent = sender_stats['last_sent'] if sender_stats else None
            # We intentionally ignore sender cooldown here; worker enforces it

            # Worker will send later; just prepare queue metadata
            conversation_id = None
            message_id = None
            logger.info(f"Prepared pending email for contact_id={contact_id}")

            # --- Insert into email_queue ---
            try:
                # Determine cc_recipients for the queued row: prefer explicit cc_store, fallback to extras in email field
                cc_recipients = None
                try:
                    if contact.get('cc_store'):
                        parts = [p.strip() for p in re.split(r'[;,\s]+', contact.get('cc_store') or '') if p.strip()]
                        cc_recipients = ';'.join(parts) if parts else None
                    else:
                        parsed = process_emails(contact.get('email') or '', validate=True)
                        if parsed and len(parsed) > 1:
                            cc_recipients = ';'.join([p for p in parsed[1:]])
                except Exception:
                    cc_recipients = None

                # Only include attachment for payments or reminders related to payments
                attach_bytes = None
                attach_filename = None
                attach_mimetype = None
                try:
                    # Infer message type: if the campaign item explicitly targets payments
                    if (item.get('type') == 'payments') or (item.get('stage') == 'payments'):
                        attach_bytes = attachment_bytes
                        # Bulk items may also provide filename/mimetype
                        attach_filename = item.get('attachment_filename') or None
                        attach_mimetype = item.get('attachment_mimetype') or None
                except Exception:
                    attach_bytes = None

                row = await conn.fetchrow('''
                    INSERT INTO email_queue (
                        contact_id, sender_email, recipient_email, subject, message,
                        created_at, due_at, type, status,
                        message_type, cc_recipients, event_id, last_message_type,
                        campaign_stage, forms_link, payment_link, attachment, attachment_filename, attachment_mimetype,
                        conversation_id, message_id
                    )
                    VALUES ($1, $2, $3, $4, $5, $6, $7, 'campaign', 'pending', $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
                    RETURNING *
                ''',
                contact_id, sender_email, contact['email'], subject_rendered, body_plain, now, now,
                'campaign_main', cc_recipients, contact.get('event_id'), 'campaign_main', 'initial', forms_link, payment_link,
                attach_bytes, attach_filename, attach_mimetype, conversation_id, message_id
                )

                # Log individual contact campaign start
                await log_user_activity(
                    conn,
                    current_user,
                    "START_INDIVIDUAL_CAMPAIGN",
                    f"Started campaign for contact: {contact['name']} ({contact['email']}) - Subject: {subject_rendered[:50]}...",
                    target_type="contact",
                    target_id=contact_id,
                    target_name=contact['name'],
                    new_values={
                        "subject": subject_rendered,
                        "queue_id": row["id"],
                        "event_name": contact.get('event_name')
                    }
                )

                queued.append(dict(row))
                logger.info(f"Queued email for contact_id={contact_id}, email_queue_id={row['id']}")
            except Exception as e:
                logger.error(f"DB insert failed for contact_id={contact_id}: {e}")
                continue

            # --- Update sender cooldown ---
            # Randomize domain cooldown on bulk queue as well
            try:
                if sender_email and '@' in sender_email:
                    domain = sender_email.split('@', 1)[1].lower()
                    domain_key = f"domain:{domain}"
                    domain_cd = random.randint(60, 180)
                    await conn.execute('''
                        INSERT INTO sender_stats (sender_email, last_sent, cooldown)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (sender_email) DO UPDATE SET last_sent = $2, cooldown = $3
                    ''', domain_key, now, domain_cd)
            except Exception:
                pass

            # Also update per-sender last_sent without forcing cooldown change
            await conn.execute('''
                INSERT INTO sender_stats (sender_email, last_sent, cooldown)
                VALUES ($1, $2, $3)
                ON CONFLICT (sender_email) DO UPDATE SET last_sent = $2
            ''', sender_email, now, cooldown)

            # --- Update contact's trigger time and unpause the campaign ---
            trigger_text = f"{now.strftime('%Y-%m-%d %H:%M:%S')} - CAMPAIGN_STARTED: Campaign automatically resumed for bulk start"
            await conn.execute('''
                UPDATE campaign_contacts
                SET trigger = COALESCE(trigger || E'\n', '') || $1,
                    last_action_time = $2,
                    last_message_type = $3,
                    campaign_paused = FALSE,
                    status = 'initial',
                    stage = 'initial'
                WHERE id = $4
            ''', trigger_text, now, 'first_message', contact_id)

        logger.info(f"Bulk campaign finished. Total sent: {len(queued)}")
        return {"queued_emails": queued}

@app.delete("/events/{event_id}")
async def delete_event(event_id: int, current_user: dict = Depends(get_current_user)):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            # First get all contact IDs associated with this event
            contact_ids = await conn.fetch(
                "SELECT id FROM campaign_contacts WHERE event_id = $1",
                event_id
            )

            # Delete from email_queue for all contacts in this event
            if contact_ids:
                contact_id_list = [row['id'] for row in contact_ids]
                await conn.execute(
                    "DELETE FROM email_queue WHERE contact_id = ANY($1::int[])",
                    contact_id_list
                )

                # Delete from messages table
                await conn.execute(
                    "DELETE FROM messages WHERE contact_id = ANY($1::int[])",
                    contact_id_list
                )

            # Delete all contacts associated with this event
            await conn.execute("DELETE FROM campaign_contacts WHERE event_id = $1", event_id)

            # Get event info before deletion for logging
            event_info = await conn.fetchrow("SELECT * FROM event WHERE id = $1", event_id)

            # Finally delete the event itself
            row = await conn.fetchrow("DELETE FROM event WHERE id = $1 RETURNING id", event_id)

            if not row:
                raise HTTPException(status_code=404, detail="Event not found")

            if not current_user["is_admin"]:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Only admins can delete events"
                )

            # Log event deletion activity
            if event_info:
                await log_user_activity(
                    conn,
                    current_user,
                    "DELETE_EVENT",
                    f"Deleted event: {event_info['event_name']} in {event_info['city']}",
                    target_type="event",
                    target_id=event_id,
                    target_name=event_info['event_name'],
                    old_values=dict(event_info)
                )

            return {"deleted": True, "message": "Event and all related records deleted successfully"}
    async with db_pool.acquire() as conn:
        result = await conn.execute("DELETE FROM event WHERE id = $1", event_id)
        if result == "DELETE 0":
            raise HTTPException(status_code=404, detail="Event not found")
        return {"message": "Event and associated contacts deleted successfully"}

@app.get("/email_queue_status")
async def get_email_queue_status():
    """Get current email queue status"""
    try:
        async with db_pool.acquire() as conn:
            current_time = datetime.now()

            # Get queue statistics
            stats = await conn.fetch("""
                SELECT
                    status,
                    COUNT(*) as count,
                    MIN(created_at) as earliest_scheduled,
                    MAX(created_at) as latest_scheduled
                FROM email_queue
                GROUP BY status
            """)

            # Get pending emails ready to send
            pending_emails = await conn.fetch("""
                SELECT sender_email, recipient_email, subject, created_at, retry_count
                FROM email_queue
                WHERE status = 'pending'
                AND created_at <= $1
                ORDER BY created_at ASC
                LIMIT 20000
            """, current_time)

            return {
                "queue_statistics": [
                    {
                        "status": stat['status'],
                        "count": stat['count'],
                        "earliest_scheduled": stat['earliest_scheduled'].isoformat() if stat['earliest_scheduled'] else None,
                        "latest_scheduled": stat['latest_scheduled'].isoformat() if stat['latest_scheduled'] else None
                    }
                    for stat in stats
                ],
                "pending_ready_to_send": [
                    {
                        "sender_email": email['sender_email'],
                        "recipient_email": email['recipient_email'],
                        "subject": email['subject'],
                        "scheduled_time": email['created_at'].isoformat(),
                        "attempts": email['retry_count']
                    }
                    for email in pending_emails
                ]
            }

    except Exception as e:
        logger.error(f"Error getting email queue status: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/monitoring/detailed_email_stats")
@app.get("/api/monitoring/detailed_email_stats")
async def get_monitoring_detailed_email_stats():
    """Unified detailed stats for monitoring dashboard (returns sent/pending/failed/replies/bounced/next_action)"""
    # Reuse the richer logic implemented for /detailed_email_stats so monitoring UI gets accurate data
    try:
        async with db_pool.acquire() as conn:
            current_time = datetime.now()
            twenty_four_hours_ago = current_time - timedelta(hours=24)

            # Get sent emails for last 24 hours (use COALESCE to handle different schemas)
            sent_last_24h = await conn.fetch("""
                SELECT
                    sender_email,
                    recipient_email,
                    subject,
                    COALESCE(created_at, sent_at, attempted_at, NOW()) AS created_at,
                    status,
                    'sent' as email_type
                FROM email_queue
                WHERE status IN ('sent','pending','failed')
                AND COALESCE(created_at, sent_at, attempted_at) >= $1
                ORDER BY COALESCE(created_at, sent_at, attempted_at) DESC
            """, twenty_four_hours_ago)

            # Get sent emails for all days before last 24h (large limit)
            sent_before_24h = await conn.fetch("""
                SELECT
                    sender_email,
                    recipient_email,
                    subject,
                    COALESCE(created_at, sent_at, attempted_at, NOW()) AS created_at,
                    status,
                    'sent' as email_type
                FROM email_queue
                WHERE status IN ('sent','pending','failed')
                AND COALESCE(created_at, sent_at, attempted_at) < $1
                ORDER BY COALESCE(created_at, sent_at, attempted_at) DESC
                LIMIT 100000
            """, twenty_four_hours_ago)

            # Get received emails (replies) using messages table
            received_last_24h = await conn.fetch("""
                SELECT
                    m.contact_id,
                    m.sender_email as from_email,
                    m.recipient_email as to_email,
                    m.subject,
                    m.received_at as created_at,
                    'received' as status
                FROM messages m
                WHERE m.direction IN ('inbound','received') OR m.direction = 'received'
                AND m.received_at >= $1
                ORDER BY m.received_at DESC
            """, twenty_four_hours_ago)

            received_before_24h = await conn.fetch("""
                SELECT
                    m.contact_id,
                    m.sender_email as from_email,
                    m.recipient_email as to_email,
                    m.subject,
                    m.received_at as created_at,
                    'received' as status
                FROM messages m
                WHERE m.direction IN ('inbound','received') OR m.direction = 'received'
                AND m.received_at < $1
                ORDER BY m.received_at DESC
                LIMIT 1000000
            """, twenty_four_hours_ago)

            # Get summary statistics
            stats_last_24h = await conn.fetchrow("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'sent') as sent_count,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
                    COUNT(*) FILTER (WHERE status = 'pending') as pending_count
                FROM email_queue
                WHERE COALESCE(created_at, sent_at, attempted_at) >= $1
            """, twenty_four_hours_ago)

            stats_before_24h = await conn.fetchrow("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'sent') as sent_count,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
                    COUNT(*) FILTER (WHERE status = 'pending') as pending_count
                FROM email_queue
                WHERE COALESCE(created_at, sent_at, attempted_at) < $1
            """, twenty_four_hours_ago)

            # Count replies using messages inbound as the source of truth
            replies_last_24h = await conn.fetchval("""
                SELECT COUNT(*) FROM messages m WHERE (m.direction IN ('inbound','received') OR m.direction = 'received') AND m.received_at >= $1
            """, twenty_four_hours_ago)

            replies_before_24h = await conn.fetchval("""
                SELECT COUNT(*) FROM messages m WHERE (m.direction IN ('inbound','received') OR m.direction = 'received') AND m.received_at < $1
            """, twenty_four_hours_ago)

            # Get sender breakdown for last 24h
            sender_breakdown_24h = await conn.fetch("""
                SELECT
                    sender_email,
                    COUNT(*) FILTER (WHERE status = 'sent') as sent_count,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed_count
                FROM email_queue
                WHERE COALESCE(created_at, sent_at, attempted_at) >= $1
                GROUP BY sender_email
                ORDER BY sent_count DESC
            """, twenty_four_hours_ago)

            sender_breakdown_before = await conn.fetch("""
                SELECT
                    sender_email,
                    COUNT(*) FILTER (WHERE status = 'sent') as sent_count,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed_count
                FROM email_queue
                WHERE COALESCE(created_at, sent_at, attempted_at) < $1
                GROUP BY sender_email
                ORDER BY sent_count DESC
            """, twenty_four_hours_ago)

            # Get bounced emails in both windows (use last_bounced_at)
            bounced_24h = await conn.fetch("""
                SELECT email, bounce_type, last_bounced_at FROM bounced_emails
                WHERE last_bounced_at >= $1
                ORDER BY last_bounced_at DESC
            """, twenty_four_hours_ago)

            bounced_before = await conn.fetch("""
                SELECT email, bounce_type, last_bounced_at FROM bounced_emails
                WHERE last_bounced_at < $1
                ORDER BY last_bounced_at DESC
                LIMIT 1000000
            """, twenty_four_hours_ago)

            # Estimate next action helper (same logic as /detailed_email_stats)
            def estimate_next_action(contact_stage, contact_status, last_message_type, last_triggered_at):
                try:
                    if not last_triggered_at:
                        return None
                    now = datetime.now()
                    lt = last_triggered_at
                    if lt.tzinfo is not None:
                        lt = lt.replace(tzinfo=None)

                    if last_message_type == 'campaign_main' and contact_status == 'first_message_sent':
                        return ('reminder1', (lt + timedelta(days=3)).isoformat())
                    if last_message_type == 'reminder1' and contact_status == 'first_reminder':
                        return ('reminder2', (lt + timedelta(days=4)).isoformat())

                    if last_message_type == 'forms_initial' and contact_status == 'forms_initial_sent':
                        return ('forms_reminder1', (lt + timedelta(days=2)).isoformat())
                    if last_message_type == 'forms_reminder1' and contact_status == 'forms_reminder1_sent':
                        return ('forms_reminder2', (lt + timedelta(days=2)).isoformat())
                    if last_message_type == 'forms_reminder2' and contact_status == 'forms_reminder2_sent':
                        return ('forms_reminder3', (lt + timedelta(days=3)).isoformat())

                    payments_map = {
                        'payments_initial': ('payments_reminder1', 2),
                        'payments_reminder1': ('payments_reminder2', 2),
                        'payments_reminder2': ('payments_reminder3', 3),
                        'payments_reminder3': ('payments_reminder4', 7),
                        'payments_reminder4': ('payments_reminder5', 7),
                        'payments_reminder5': ('payments_reminder6', 7),
                    }
                    if last_message_type in payments_map:
                        nxt, days = payments_map[last_message_type]
                        return (nxt, (lt + timedelta(days=days)).isoformat())

                    if last_message_type == 'error':
                        return ('retry', (lt + timedelta(hours=1)).isoformat())

                    return None
                except Exception:
                    return None

            def fmt_email_row(row):
                try:
                    return {
                        "from": row.get('sender_email'),
                        "to": row.get('recipient_email'),
                        "subject": row.get('subject'),
                        "timestamp": row.get('created_at').isoformat() if row.get('created_at') else None,
                        "status": row.get('status'),
                        "type": 'sent'
                    }
                except Exception:
                    return {}

            def fmt_received_row(row):
                try:
                    return {
                        "contact_id": row.get('contact_id'),
                        "from": row.get('from_email') or row.get('recipient_email'),
                        "to": row.get('to_email') or row.get('sender_email'),
                        "subject": row.get('subject'),
                        "timestamp": row.get('created_at').isoformat() if row.get('created_at') else None,
                        "status": row.get('status') or 'received'
                    }
                except Exception:
                    return {}

            return {
                "last_24_hours": {
                    "summary": {
                        "sent": stats_last_24h['sent_count'] or 0,
                        "failed": stats_last_24h['failed_count'] or 0,
                        "pending": stats_last_24h['pending_count'] or 0,
                        "received_replies": replies_last_24h or 0
                    },
                    "sent_emails": [fmt_email_row(email) for email in sent_last_24h],
                    "received_emails": [fmt_received_row(email) for email in received_last_24h],
                    "sender_breakdown": [
                        {
                            "sender_email": sender['sender_email'],
                            "sent_count": sender['sent_count'] or 0,
                            "failed_count": sender['failed_count'] or 0
                        }
                        for sender in sender_breakdown_24h
                    ],
                    "bounced": [
                        {"email": b['email'], "bounce_type": b['bounce_type'], "timestamp": b['created_at'].isoformat() if b.get('created_at') else None}
                        for b in bounced_24h
                    ]
                },
                "all_previous_days": {
                    "summary": {
                        "sent": stats_before_24h['sent_count'] or 0,
                        "failed": stats_before_24h['failed_count'] or 0,
                        "pending": stats_before_24h['pending_count'] or 0,
                        "received_replies": replies_before_24h or 0
                    },
                    "sent_emails": [fmt_email_row(email) for email in sent_before_24h],
                    "received_emails": [fmt_received_row(email) for email in received_before_24h],
                    "sender_breakdown": [
                        {
                            "sender_email": sender['sender_email'],
                            "sent_count": sender['sent_count'] or 0,
                            "failed_count": sender['failed_count'] or 0
                        }
                        for sender in sender_breakdown_before
                    ],
                    "bounced": [
                        {"email": b['email'], "bounce_type": b['bounce_type'], "timestamp": b['created_at'].isoformat() if b.get('created_at') else None}
                        for b in bounced_before
                    ]
                },
                "generated_at": current_time.isoformat()
            }
    except Exception as e:
        logger.error(f"Error getting detailed email stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/monitoring/failed_sends')
@app.get('/api/monitoring/failed_sends')
async def get_monitoring_failed_sends(page: int = 1, page_size: int = 100, q: Optional[str] = None):
    """Return paginated failed sends with contact info, error message and timestamp."""
    try:
        offset = (page - 1) * page_size
        async with db_pool.acquire() as conn:
            base_where = "eq.status = 'failed'"
            params = []
            if q:
                # search contact name, contact email or subject
                params.append(f"%{q.lower()}%")
                where_clause = base_where + " AND (LOWER(cc.name) LIKE $1 OR LOWER(cc.email) LIKE $1 OR LOWER(eq.subject) LIKE $1)"
                rows = await conn.fetch(f"""
                    SELECT
                        eq.id as queue_id,
                        eq.contact_id,
                        eq.sender_email,
                        eq.recipient_email,
                        eq.subject,
                        eq.error_message,
                        COALESCE(eq.attempted_at, eq.sent_at, eq.created_at) as failed_at,
                        eq.retry_count,
                        cc.name as contact_name,
                        cc.email as contact_email,
                        cc.stage as contact_stage,
                        cc.status as contact_status
                    FROM email_queue eq
                    LEFT JOIN campaign_contacts cc ON eq.contact_id = cc.id
                    WHERE """ + where_clause + """
                    ORDER BY failed_at DESC
                    LIMIT $2 OFFSET $3
                """, *params, page_size, offset)
            else:
                rows = await conn.fetch("""
                    SELECT
                        eq.id as queue_id,
                        eq.contact_id,
                        eq.sender_email,
                        eq.recipient_email,
                        eq.subject,
                        eq.error_message,
                        COALESCE(eq.attempted_at, eq.sent_at, eq.created_at) as failed_at,
                        eq.retry_count,
                        cc.name as contact_name,
                        cc.email as contact_email,
                        cc.stage as contact_stage,
                        cc.status as contact_status
                    FROM email_queue eq
                    LEFT JOIN campaign_contacts cc ON eq.contact_id = cc.id
                    WHERE eq.status = 'failed'
                    ORDER BY failed_at DESC
                    LIMIT $1 OFFSET $2
                """, page_size, offset)

            result = []
            for r in rows:
                result.append({
                    "queue_id": r.get('queue_id'),
                    "contact_id": r.get('contact_id'),
                    "contact_name": r.get('contact_name'),
                    "contact_email": r.get('contact_email'),
                    "contact_stage": r.get('contact_stage'),
                    "contact_status": r.get('contact_status'),
                    "from": r.get('sender_email'),
                    "to": r.get('recipient_email'),
                    "subject": r.get('subject'),
                    "error_message": r.get('error_message'),
                    "failed_at": r.get('failed_at').isoformat() if r.get('failed_at') else None,
                    "retry_count": r.get('retry_count')
                })

            # total count for pagination
            total = await conn.fetchval("SELECT COUNT(*) FROM email_queue WHERE status = 'failed'")

            return {
                "page": page,
                "page_size": page_size,
                "total": total,
                "results": result
            }
    except Exception as e:
        logger.error(f"Error getting failed sends: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/detailed_email_stats")
async def get_detailed_email_stats(current_user: dict = Depends(get_current_user)):
    """Get detailed email statistics for last 24h and all previous days with sender/receiver info"""
    try:
        async with db_pool.acquire() as conn:
            current_time = datetime.now()
            twenty_four_hours_ago = current_time - timedelta(hours=24)

            # Get sent emails for last 24 hours
            sent_last_24h = await conn.fetch("""
                SELECT
                    sender_email,
                    recipient_email,
                    subject,
                    created_at,
                    status,
                    'sent' as email_type
                FROM email_queue
                WHERE status = 'sent'
                AND created_at >= $1
                ORDER BY created_at DESC
            """, twenty_four_hours_ago)

            # Get sent emails for all days before last 24h
            sent_before_24h = await conn.fetch("""
                SELECT
                    sender_email,
                    recipient_email,
                    subject,
                    created_at,
                    status,
                    'sent' as email_type
                FROM email_queue
                WHERE status = 'sent'
                AND created_at < $1
                ORDER BY created_at DESC
                LIMIT 100000
            """, twenty_four_hours_ago)

            # Get received emails (replies) for last 24 hours
            # Prefer actual inbound messages from `messages` table, fallback to campaign_contacts
            received_last_24h = await conn.fetch("""
                SELECT
                    m.contact_id,
                    m.sender_email as from_email,
                    m.recipient_email as to_email,
                    m.subject,
                    m.received_at as created_at,
                    'received' as status
                FROM messages m
                WHERE m.direction = 'inbound'
                AND m.received_at >= $1
                ORDER BY m.received_at DESC
            """, twenty_four_hours_ago)

            # Get received emails (replies) for all days before last 24h
            received_before_24h = await conn.fetch("""
                SELECT
                    m.contact_id,
                    m.sender_email as from_email,
                    m.recipient_email as to_email,
                    m.subject,
                    m.received_at as created_at,
                    'received' as status
                FROM messages m
                WHERE m.direction = 'inbound'
                AND m.received_at < $1
                ORDER BY m.received_at DESC
                LIMIT 100000
            """, twenty_four_hours_ago)

            # Get summary statistics
            stats_last_24h = await conn.fetchrow("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'sent') as sent_count,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
                    COUNT(*) FILTER (WHERE status = 'pending') as pending_count
                FROM email_queue
                WHERE created_at >= $1
            """, twenty_four_hours_ago)

            stats_before_24h = await conn.fetchrow("""
                SELECT
                    COUNT(*) FILTER (WHERE status = 'sent') as sent_count,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed_count,
                    COUNT(*) FILTER (WHERE status = 'pending') as pending_count
                FROM email_queue
                WHERE created_at < $1
            """, twenty_four_hours_ago)

            # Count replies using messages inbound as more reliable source
            replies_last_24h = await conn.fetchval("""
                SELECT COUNT(*) FROM messages m WHERE m.direction = 'inbound' AND m.received_at >= $1
            """, twenty_four_hours_ago)

            replies_before_24h = await conn.fetchval("""
                SELECT COUNT(*) FROM messages m WHERE m.direction = 'inbound' AND m.received_at < $1
            """, twenty_four_hours_ago)

            # Get sender breakdown for last 24h
            sender_breakdown_24h = await conn.fetch("""
                SELECT
                    sender_email,
                    COUNT(*) FILTER (WHERE status = 'sent') as sent_count,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed_count
                FROM email_queue
                WHERE created_at >= $1
                GROUP BY sender_email
                ORDER BY sent_count DESC
            """, twenty_four_hours_ago)

            # Get sender breakdown for all previous days
            sender_breakdown_before = await conn.fetch("""
                SELECT
                    sender_email,
                    COUNT(*) FILTER (WHERE status = 'sent') as sent_count,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed_count
                FROM email_queue
                WHERE created_at < $1
                GROUP BY sender_email
                ORDER BY sent_count DESC
            """, twenty_four_hours_ago)

            # Get bounced emails in both windows
            bounced_24h = await conn.fetch("""
                SELECT email, bounce_type, created_at FROM bounced_emails
                WHERE created_at >= $1
                ORDER BY created_at DESC
            """, twenty_four_hours_ago)

            bounced_before = await conn.fetch("""
                SELECT email, bounce_type, created_at FROM bounced_emails
                WHERE created_at < $1
                ORDER BY created_at DESC
                LIMIT 1000000
            """, twenty_four_hours_ago)

            # Helper to compute next action estimate for a contact
            def estimate_next_action(contact_stage, contact_status, last_message_type, last_triggered_at):
                try:
                    if not last_triggered_at:
                        return None
                    now = datetime.now()
                    lt = last_triggered_at
                    if lt.tzinfo is not None:
                        lt = lt.replace(tzinfo=None)

                    # Map next intervals in days based on last_message_type
                    if last_message_type == 'campaign_main' and contact_status == 'first_message_sent':
                        return ('reminder1', (lt + timedelta(days=3)).isoformat())
                    if last_message_type == 'reminder1' and contact_status == 'first_reminder':
                        return ('reminder2', (lt + timedelta(days=4)).isoformat())

                    # Forms
                    if last_message_type == 'forms_initial' and contact_status == 'forms_initial_sent':
                        return ('forms_reminder1', (lt + timedelta(days=2)).isoformat())
                    if last_message_type == 'forms_reminder1' and contact_status == 'forms_reminder1_sent':
                        return ('forms_reminder2', (lt + timedelta(days=2)).isoformat())
                    if last_message_type == 'forms_reminder2' and contact_status == 'forms_reminder2_sent':
                        return ('forms_reminder3', (lt + timedelta(days=3)).isoformat())

                    # Payments sequence
                    payments_map = {
                        'payments_initial': ('payments_reminder1', 2),
                        'payments_reminder1': ('payments_reminder2', 2),
                        'payments_reminder2': ('payments_reminder3', 3),
                        'payments_reminder3': ('payments_reminder4', 7),
                        'payments_reminder4': ('payments_reminder5', 7),
                        'payments_reminder5': ('payments_reminder6', 7),
                    }
                    if last_message_type in payments_map:
                        nxt, days = payments_map[last_message_type]
                        return (nxt, (lt + timedelta(days=days)).isoformat())

                    # Error retry after 1 hour
                    if last_message_type == 'error':
                        return ('retry', (lt + timedelta(hours=1)).isoformat())

                    return None
                except Exception:
                    return None

            def fmt_email_row(row):
                try:
                    return {
                        "id": row.get('queue_id'),
                        "contact_id": row.get('contact_id'),
                        "from": row.get('sender_email'),
                        "to": row.get('recipient_email'),
                        "subject": row.get('subject'),
                        "timestamp": row.get('created_at').isoformat() if row.get('created_at') else None,
                        "status": row.get('status'),
                        "type": 'sent',
                        "contact_stage": row.get('contact_stage'),
                        "contact_status": row.get('contact_status'),
                        "last_triggered_at": row.get('last_triggered_at').isoformat() if row.get('last_triggered_at') else None,
                        "last_message_type": row.get('last_message_type'),
                        "next_action": estimate_next_action(row.get('contact_stage'), row.get('contact_status'), row.get('last_message_type'), row.get('last_triggered_at'))
                    }
                except Exception:
                    return {}

            def fmt_received_row(row):
                try:
                    return {
                        "contact_id": row.get('contact_id'),
                        "from": row.get('from_email') or row.get('recipient_email'),
                        "to": row.get('to_email') or row.get('sender_email'),
                        "subject": row.get('subject'),
                        "timestamp": row.get('created_at').isoformat() if row.get('created_at') else None,
                        "status": row.get('status') or 'received'
                    }
                except Exception:
                    return {}

            return {
                "last_24_hours": {
                    "summary": {
                        "sent": stats_last_24h['sent_count'] or 0,
                        "failed": stats_last_24h['failed_count'] or 0,
                        "pending": stats_last_24h['pending_count'] or 0,
                        "received_replies": replies_last_24h or 0
                    },
                    "sent_emails": [fmt_email_row(email) for email in sent_last_24h],
                    "received_emails": [fmt_received_row(email) for email in received_last_24h],
                    "sender_breakdown": [
                        {
                            "sender_email": sender['sender_email'],
                            "sent_count": sender['sent_count'] or 0,
                            "failed_count": sender['failed_count'] or 0
                        }
                        for sender in sender_breakdown_24h
                    ],
                    "bounced": [
                        {"email": b['email'], "bounce_type": b['bounce_type'], "timestamp": b['created_at'].isoformat() if b.get('created_at') else None}
                        for b in bounced_24h
                    ]
                },
                "all_previous_days": {
                    "summary": {
                        "sent": stats_before_24h['sent_count'] or 0,
                        "failed": stats_before_24h['failed_count'] or 0,
                        "pending": stats_before_24h['pending_count'] or 0,
                        "received_replies": replies_before_24h or 0
                    },
                    "sent_emails": [fmt_email_row(email) for email in sent_before_24h],
                    "received_emails": [fmt_received_row(email) for email in received_before_24h],
                    "sender_breakdown": [
                        {
                            "sender_email": sender['sender_email'],
                            "sent_count": sender['sent_count'] or 0,
                            "failed_count": sender['failed_count'] or 0
                        }
                        for sender in sender_breakdown_before
                    ],
                    "bounced": [
                        {"email": b['email'], "bounce_type": b['bounce_type'], "timestamp": b['created_at'].isoformat() if b.get('created_at') else None}
                        for b in bounced_before
                    ]
                },
                "generated_at": current_time.isoformat()
            }

    except Exception as e:
        logger.error(f"Error getting detailed email stats: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/debug/templates")
async def debug_templates(current_user: dict = Depends(get_current_user)):
    """Debug endpoint to check template files"""
    import os
    import glob

    template_dir = "public/templates/emails"
    result = {
        "template_directory": template_dir,
        "directory_exists": os.path.exists(template_dir),
        "files": [],
        "template_tests": {}
    }

    if os.path.exists(template_dir):
        # List all files in template directory
        files = glob.glob(os.path.join(template_dir, "*.txt"))
        result["files"] = [os.path.basename(f) for f in files]

        # Test loading each template type
        test_cases = [
            ("campaign", "subject", "payments"),
            ("campaign", "body", "payments"),
            ("campaign", "subject", "forms"),
            ("campaign", "body", "forms"),
            ("campaign", "subject", None),
            ("campaign", "body", None),
            ("reminder", "subject", None),
            ("reminder", "body", None)
        ]

        for template_type, part, stage in test_cases:
            key = f"{template_type}_{part}_{stage or 'default'}"
            try:
                content = load_template(template_type, part, stage=stage)
                result["template_tests"][key] = {
                    "status": "success",
                    "content_length": len(content),
                    "preview": content[:100] + "..." if len(content) > 100 else content
                }
            except Exception as e:
                result["template_tests"][key] = {
                    "status": "error",
                    "error": str(e)
                }

    return result

async def verify_monitoring_token(request: Request):
    """Verify JWT token for monitoring endpoints"""
    auth_header = request.headers.get('authorization')
    if not auth_header or not auth_header.startswith('Bearer '):
        raise HTTPException(status_code=401, detail="Missing authorization")

    token = auth_header.split(' ')[1]
    try:
        import jwt
        # Ensure you are using the same secret key as in your create_access_token function
        SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-here-change-in-production')
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        username = payload.get("sub")

        # This check is specific to your monitoring login, ensure it's what you want
        if username != 'hatem':
            raise HTTPException(status_code=403, detail="Access denied")

        # You might need to fetch user details from DB if other parts of the function need it
        # For now, returning the payload is sufficient for authentication.
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

# ... other code ...

# THE FIX IS HERE: Change `Depends(get_current_user)` to `Depends(verify_monitoring_token)`
@app.get("/user-activity-logs")
async def get_user_activity_logs(
    request: Request, # Add the request parameter
    limit: int = 10000,
    offset: int = 0,
    action_type: str = None,
    username: str = None,
    date_from: str = None,
    date_to: str = None,
    # Use the dependency that is known to work for your monitoring dashboard
    current_user: dict = Depends(verify_monitoring_token)
):
    """Get user activity logs with filtering options"""
    try:
        # The rest of your function logic remains exactly the same
        async with db_pool.acquire() as conn:
            # Build WHERE clause based on filters
            where_conditions = []
            params = []
            param_count = 0

            if action_type:
                param_count += 1
                where_conditions.append(f"action_type = ${param_count}")
                params.append(action_type)

            if username:
                param_count += 1
                where_conditions.append(f"username ILIKE ${param_count}")
                params.append(f"%{username}%")

            if date_from:
                param_count += 1
                where_conditions.append(f"timestamp >= ${param_count}")
                params.append(date_from)

            if date_to:
                param_count += 1
                where_conditions.append(f"timestamp <= ${param_count}")
                params.append(date_to)

            where_clause = " AND ".join(where_conditions) if where_conditions else "TRUE"

            # Get total count
            total_query = f"SELECT COUNT(*) FROM user_activity_logs WHERE {where_clause}"
            total = await conn.fetchval(total_query, *params)

            # Get paginated logs
            # Create a separate list of params for the main query to avoid mutation issues
            logs_params = list(params)
            logs_params.append(limit)
            logs_params.append(offset)

            logs_query = f"""
                SELECT id, username, action_type, action_description, target_type,
                       target_id, target_name, old_values, new_values, ip_address,
                       user_agent, timestamp
                FROM user_activity_logs
                WHERE {where_clause}
                ORDER BY timestamp DESC
                LIMIT ${len(logs_params)-1} OFFSET ${len(logs_params)}
            """

            logs = await conn.fetch(logs_query, *logs_params)

            return {
                "logs": [dict(log) for log in logs],
                "total": total,
                "limit": limit,
                "offset": offset
            }

    except Exception as e:
        logger.error(f"Error fetching user activity logs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# Server startup (router registration is handled earlier in the file)
if __name__ == "__main__":
    import uvicorn
    logger.info("[MAIN] Starting FastAPI server...")
    uvicorn.run(app, host="0.0.0.0", port=9009)
