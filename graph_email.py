import os
import logging
import requests
from msal import ConfidentialClientApplication
from dotenv import load_dotenv
load_dotenv()

# Setup logging to both file and console with detailed output
try:
    from logging.handlers import RotatingFileHandler
    log_handler = RotatingFileHandler('email_campaign.log', maxBytes=2*1024*1024, backupCount=5)
except ImportError:
    # Fallback to basic FileHandler if RotatingFileHandler is not available
    log_handler = logging.FileHandler('email_campaign.log')
console_handler = logging.StreamHandler()
log_formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(message)s')
log_handler.setFormatter(log_formatter)
console_handler.setFormatter(log_formatter)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(log_handler)
logger.addHandler(console_handler)

# Read up to 3 sets of credentials from environment variables
SENDERS = []
for i in range(1, 4):
    client_id = os.getenv(f'AZURE_CLIENT_ID_{i}')
    client_secret = os.getenv(f'AZURE_CLIENT_SECRET_{i}')
    tenant_id = os.getenv(f'AZURE_TENANT_ID_{i}')
    sender_email = os.getenv(f'GRAPH_SENDER_EMAIL_{i}')
    if client_id and client_secret and tenant_id and sender_email:
        SENDERS.append({
            'client_id': client_id,
            'client_secret': client_secret,
            'tenant_id': tenant_id,
            'sender_email': sender_email.lower(),
            'msal_app': None
        })
# Fallback/default sender
DEFAULT_CLIENT_ID = os.getenv('AZURE_CLIENT_ID')
DEFAULT_CLIENT_SECRET = os.getenv('AZURE_CLIENT_SECRET')
DEFAULT_TENANT_ID = os.getenv('AZURE_TENANT_ID')
DEFAULT_SENDER_EMAIL = os.getenv('GRAPH_SENDER_EMAIL')
DEFAULT_SENDER = {
    'client_id': DEFAULT_CLIENT_ID,
    'client_secret': DEFAULT_CLIENT_SECRET,
    'tenant_id': DEFAULT_TENANT_ID,
    'sender_email': DEFAULT_SENDER_EMAIL.lower() if DEFAULT_SENDER_EMAIL else None,
    'msal_app': None
}

GRAPH_API_BASE = 'https://graph.microsoft.com/v1.0'
SCOPE = ['https://graph.microsoft.com/.default']

def get_sender_config(sender_email):
    if not sender_email:
        raise Exception("No sender_email provided to get_sender_config")
    sender_email = sender_email.lower()
    # Debug logging for sender matching
    loaded_senders = [s['sender_email'] for s in SENDERS]
    logger.debug(f"get_sender_config: requested sender_email={sender_email}")
    logger.debug(f"get_sender_config: loaded SENDERS={loaded_senders}")
    logger.debug(f"get_sender_config: DEFAULT_SENDER={DEFAULT_SENDER['sender_email']}")
    for sender in SENDERS:
        if sender['sender_email'] == sender_email:
            # Ensure all required fields are present
            if not sender['tenant_id'] or sender['tenant_id'].lower() == 'none':
                raise Exception(f"Missing or invalid tenant_id for sender {sender_email}")
            return sender
    # fallback
    if DEFAULT_SENDER['sender_email'] == sender_email:
        if not DEFAULT_SENDER['tenant_id'] or DEFAULT_SENDER['tenant_id'].lower() == 'none':
            raise Exception(f"Missing or invalid tenant_id for default sender {sender_email}")
        return DEFAULT_SENDER
    raise Exception(f"No Azure config found for sender {sender_email}")

def get_msal_app(sender_config):
    if sender_config['msal_app'] is None:
        sender_config['msal_app'] = ConfidentialClientApplication(
            sender_config['client_id'],
            authority=f"https://login.microsoftonline.com/{sender_config['tenant_id']}",
            client_credential=sender_config['client_secret']
        )
    return sender_config['msal_app']

def get_access_token(sender_email):
    sender_config = get_sender_config(sender_email)
    app = get_msal_app(sender_config)
    result = app.acquire_token_for_client(scopes=SCOPE)
    if 'access_token' in result:
        return result['access_token']
    else:
        logger.error(f"MSAL token error for {sender_email}: {result.get('error')}, {result.get('error_description')}")
        raise RuntimeError(f"Could not obtain access token for {sender_email}: {result}")

def send_graph_email(
    sender_email,
    to_email,
    subject,
    body,
    content_type="HTML",
    cc_emails=None,
    attachments=None,
    in_reply_to=None,
    references=None,
    test_mode=False,
    conversation_id=None,
    attachment_bytes=None,
    attachment_filename=None,
    attachment_mimetype=None
):
    """
    Production-grade function to send emails via Microsoft Graph API with guaranteed success confirmation.
    
    🎯 Returns ONLY "sent" status if:
    1. Graph API accepts the request (202 or 204)
    2. Email appears in Sent Items folder during retry loop (up to 10 seconds)
    
    Args:
        sender_email (str): Sender's email address. REQUIRED.
        to_email (str or list): Recipient email(s). Can be comma-separated or list.
        subject (str): Email subject line.
        body (str): Email body (plain text or HTML based on content_type).
        content_type (str): "HTML" or "TEXT" (default: "HTML").
        cc_emails (str or list, optional): CC recipient(s).
        attachments (list, optional): List of dicts with 'filename', 'content' (bytes), 'mimetype'.
        in_reply_to (str, optional): Message-ID to reply to.
        references (str, optional): Message-ID references for threading.
        test_mode (bool): If True, logs payload without sending.
        conversation_id (str, optional): Conversation ID for threading (output parameter, not used in sending).
        attachment_bytes (bytes, optional): Single attachment content (alternative to attachments list).
        attachment_filename (str, optional): Single attachment filename.
        attachment_mimetype (str, optional): Single attachment MIME type.
    
    Returns:
        {
            "status": "sent",
            "message_id": "...",
            "conversation_id": "..."
        }
        OR
        {
            "status": "failed",
            "error_message": "...",
            "code": <HTTP status code>
        }
    """
    import time
    import base64
    
    # ============================================================================
    # 1. VALIDATION
    # ============================================================================
    if not sender_email:
        error_msg = "sender_email is required and cannot be empty"
        logger.error(f"[SEND_EMAIL] {error_msg}")
        return {
            "status": "failed",
            "error_message": error_msg,
            "code": 400
        }
    
    sender_email = sender_email.lower().strip()
    
    # Validate content_type
    content_type = content_type.upper()
    if content_type not in ["HTML", "TEXT"]:
        error_msg = f"Invalid content_type '{content_type}'. Must be 'HTML' or 'TEXT'."
        logger.error(f"[SEND_EMAIL] {error_msg}")
        return {
            "status": "failed",
            "error_message": error_msg,
            "code": 400
        }
    
    # Parse TO recipients
    if isinstance(to_email, str):
        to_recipients = [e.strip() for e in to_email.split(',') if e.strip()]
    elif isinstance(to_email, list):
        to_recipients = [str(e).strip() for e in to_email if e]
    else:
        to_recipients = [str(to_email).strip()]
    
    if not to_recipients:
        error_msg = "to_email cannot be empty"
        logger.error(f"[SEND_EMAIL] {error_msg}")
        return {
            "status": "failed",
            "error_message": error_msg,
            "code": 400
        }
    
    # Parse CC recipients
    cc_recipients = []
    if cc_emails:
        if isinstance(cc_emails, str):
            cc_recipients = [e.strip() for e in cc_emails.split(',') if e.strip()]
        elif isinstance(cc_emails, list):
            cc_recipients = [str(e).strip() for e in cc_emails if e]
    
    # Convert individual attachment parameters to attachments list format
    if attachment_bytes and attachment_filename:
        if attachments is None:
            attachments = []
        attachments.append({
            'filename': attachment_filename,
            'content': attachment_bytes,
            'mimetype': attachment_mimetype or 'application/octet-stream'
        })
    
    # ============================================================================
    # 2. BUILD MESSAGE PAYLOAD
    # ============================================================================
    try:
        payload = {
            "message": {
                "subject": subject,
                "body": {
                    "contentType": content_type,
                    "content": body
                },
                "toRecipients": [
                    {"emailAddress": {"address": email}}
                    for email in to_recipients
                ]
            },
            "saveToSentItems": True  # CRITICAL: Ensure email is saved to Sent Items
        }
        
        # Add CC recipients if present
        if cc_recipients:
            payload["message"]["ccRecipients"] = [
                {"emailAddress": {"address": email}}
                for email in cc_recipients
            ]
        
        # Add threading headers if provided
        if in_reply_to or references:
            payload["message"]["internetMessageHeaders"] = []
            if in_reply_to:
                payload["message"]["internetMessageHeaders"].append({
                    "name": "In-Reply-To",
                    "value": in_reply_to
                })
            if references:
                payload["message"]["internetMessageHeaders"].append({
                    "name": "References",
                    "value": references
                })
        
        # Handle attachments
        if attachments and isinstance(attachments, list):
            payload["message"]["attachments"] = []
            for att in attachments:
                if not isinstance(att, dict) or 'filename' not in att or 'content' not in att:
                    logger.warning("[SEND_EMAIL] Skipping invalid attachment format")
                    continue
                
                try:
                    filename = att['filename']
                    content_bytes = att['content']
                    mimetype = att.get('mimetype', 'application/octet-stream')
                    
                    # Encode to base64
                    content_b64 = base64.b64encode(content_bytes).decode('ascii')
                    
                    file_att = {
                        "@odata.type": "#microsoft.graph.fileAttachment",
                        "name": filename,
                        "contentType": mimetype,
                        "contentBytes": content_b64
                    }
                    payload["message"]["attachments"].append(file_att)
                    logger.info(f"[SEND_EMAIL] Attached file: {filename} ({len(content_bytes)} bytes)")
                except Exception as e:
                    logger.error(f"[SEND_EMAIL] Failed to process attachment '{att.get('filename')}': {e}")
                    return {
                        "status": "failed",
                        "error_message": f"Attachment processing failed: {str(e)}",
                        "code": 400
                    }
        
    except Exception as e:
        logger.error(f"[SEND_EMAIL] Failed to build payload: {e}")
        return {
            "status": "failed",
            "error_message": f"Payload construction failed: {str(e)}",
            "code": 400
        }
    
    # ============================================================================
    # 3. TEST MODE
    # ============================================================================
    if test_mode:
        logger.info(f"[SEND_EMAIL] [TEST MODE] Email payload: {payload}")
        return {
            "status": "test",
            "payload": payload
        }
    
    # ============================================================================
    # 4. GET ACCESS TOKEN
    # ============================================================================
    try:
        access_token = get_access_token(sender_email)
        logger.info(f"[SEND_EMAIL] Obtained access token for {sender_email}")
    except Exception as e:
        error_msg = f"Failed to obtain access token: {str(e)}"
        logger.error(f"[SEND_EMAIL] {error_msg}")
        return {
            "status": "failed",
            "error_message": error_msg,
            "code": 401
        }
    
    # ============================================================================
    # 5. SEND EMAIL VIA GRAPH API
    # ============================================================================
    url = f"{GRAPH_API_BASE}/users/{sender_email}/sendMail"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    try:
        logger.info(f"[SEND_EMAIL] Sending to {','.join(to_recipients)} from {sender_email}")
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        
        # Check for success response codes
        if response.status_code not in [202, 204]:
            # Extract error details from Graph response
            try:
                error_data = response.json()
                error_code = error_data.get('error', {}).get('code', 'UNKNOWN')
                error_message = error_data.get('error', {}).get('message', response.text)
            except Exception:
                error_code = 'UNKNOWN'
                error_message = response.text
            
            logger.error(
                f"[SEND_EMAIL] Graph API returned {response.status_code}: "
                f"code={error_code}, message={error_message}"
            )
            
            return {
                "status": "failed",
                "error_message": error_message,
                "code": response.status_code
            }
        
        logger.info(f"[SEND_EMAIL] Graph API accepted email (HTTP {response.status_code})")
        
    except requests.exceptions.Timeout:
        error_msg = "Graph API request timed out (30 seconds)"
        logger.error(f"[SEND_EMAIL] {error_msg}")
        return {
            "status": "failed",
            "error_message": error_msg,
            "code": 504
        }
    except requests.exceptions.RequestException as e:
        error_msg = f"Network error sending to Graph API: {str(e)}"
        logger.error(f"[SEND_EMAIL] {error_msg}")
        return {
            "status": "failed",
            "error_message": error_msg,
            "code": 0
        }
    except Exception as e:
        error_msg = f"Unexpected error during send: {str(e)}"
        logger.error(f"[SEND_EMAIL] {error_msg}")
        return {
            "status": "failed",
            "error_message": error_msg,
            "code": 0
        }
    
    # ============================================================================
    # 6. VERIFY EMAIL IN SENT ITEMS (CRITICAL FOR SUCCESS GUARANTEE)
    # ============================================================================
    sent_details = _verify_email_in_sent_items(
        sender_email,
        subject,
        to_recipients[0] if to_recipients else None,
        max_retries=3,
        retry_delay_sec=2
    )
    
    if sent_details:
        logger.info(
            f"[SEND_EMAIL] ✓ SUCCESS: Email confirmed in Sent Items. "
            f"message_id={sent_details.get('message_id')}, "
            f"conversation_id={sent_details.get('conversation_id')}"
        )
        return {
            "status": "sent",
            "message_id": sent_details.get('message_id'),
            "conversation_id": sent_details.get('conversation_id')
        }
    else:
        # Email accepted by Graph but NOT found in Sent Items after retries
        error_msg = (
            "Email accepted by Graph API but NOT confirmed in Sent Items folder "
            "after retry attempts. This may indicate a mailbox issue or permission problem."
        )
        logger.error(f"[SEND_EMAIL] ✗ FAILED: {error_msg}")
        return {
            "status": "failed",
            "error_message": error_msg,
            "code": 422  # 422 Unprocessable Entity (semantically correct for this case)
        }


def _verify_email_in_sent_items(sender_email, subject, recipient_email, max_retries=3, retry_delay_sec=2):
    """
    Verify that an email appears in the sender's Sent Items folder.
    
    Returns:
        {"message_id": "...", "conversation_id": "..."} if found
        None if not found after all retries
    """
    import time
    
    if not sender_email:
        logger.error("[VERIFY_SENT] sender_email is required")
        return None
    
    try:
        access_token = get_access_token(sender_email)
    except Exception as e:
        logger.error(f"[VERIFY_SENT] Failed to get access token: {e}")
        return None
    
    url = f"{GRAPH_API_BASE}/users/{sender_email}/mailFolders/SentItems/messages"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # Query parameters: get recent messages, sorted by most recent first
    params = {
        "$select": "id,internetMessageId,conversationId,subject,toRecipients,sentDateTime",
        "$orderby": "sentDateTime desc",
        "$top": 20
    }
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                f"[VERIFY_SENT] Checking Sent Items (attempt {attempt}/{max_retries}) "
                f"for subject='{subject}', to='{recipient_email}'"
            )
            
            response = requests.get(url, headers=headers, params=params, timeout=10)
            
            if response.status_code != 200:
                logger.warning(
                    f"[VERIFY_SENT] Graph API returned {response.status_code} "
                    f"when checking Sent Items: {response.text}"
                )
                if attempt < max_retries:
                    time.sleep(retry_delay_sec)
                continue
            
            messages = response.json().get('value', [])
            logger.debug(f"[VERIFY_SENT] Found {len(messages)} messages in Sent Items")
            
            # Search for matching message
            for msg in messages:
                msg_subject = msg.get('subject', '').strip()
                msg_to_recipients = msg.get('toRecipients', [])
                msg_to_addresses = [
                    r.get('emailAddress', {}).get('address', '').lower()
                    for r in msg_to_recipients
                ]
                
                # Match: subject and recipient email
                subject_match = msg_subject.lower() == subject.lower()
                recipient_match = (
                    recipient_email and
                    recipient_email.lower() in msg_to_addresses
                )
                
                if subject_match and recipient_match:
                    message_id = msg.get('internetMessageId')
                    conversation_id = msg.get('conversationId')
                    logger.info(
                        f"[VERIFY_SENT] ✓ Match found: message_id={message_id}, "
                        f"conversation_id={conversation_id}"
                    )
                    return {
                        "message_id": message_id,
                        "conversation_id": conversation_id
                    }
            
            logger.debug(
                f"[VERIFY_SENT] No matching message found in this batch. "
                f"Attempting retry in {retry_delay_sec} seconds..."
            )
            
            if attempt < max_retries:
                time.sleep(retry_delay_sec)
        
        except requests.exceptions.Timeout:
            logger.warning(
                f"[VERIFY_SENT] Timeout checking Sent Items (attempt {attempt}). "
                f"Retrying..."
            )
            if attempt < max_retries:
                time.sleep(retry_delay_sec)
        except Exception as e:
            logger.error(
                f"[VERIFY_SENT] Error checking Sent Items (attempt {attempt}): {e}"
            )
            if attempt < max_retries:
                time.sleep(retry_delay_sec)
    
    logger.error(
        f"[VERIFY_SENT] Email NOT confirmed in Sent Items after {max_retries} attempts"
    )
    return None


def fetch_all_inbox_messages(sender_email, max_messages=500):
    """
    Fetch all messages from the inbox for the given sender using Microsoft Graph API (with paging).
    Returns a list of message dicts with internetMessageHeaders and inReplyTo fields.
    """
    access_token = get_access_token(sender_email)
    # Request all necessary fields including the message body
    select_fields = "id,subject,from,receivedDateTime,internetMessageHeaders,body,bodyPreview,uniqueBody"
    url = f"{GRAPH_API_BASE}/users/{sender_email}/mailFolders/inbox/messages"
    url += f"?$select={select_fields}&$orderby=receivedDateTime desc&$top=50"
    
    headers = {
        "Authorization": f"Bearer {access_token}"
    }
    
    messages = []
    fetched = 0
    
    try:
        while url and fetched < max_messages:
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                logger.error(f"Failed to fetch inbox messages for {sender_email}: {response.status_code} {response.text}")
                break
                
            data = response.json()
            batch = data.get('value', [])
            
            # Process each message to extract inReplyTo from headers
            for msg in batch:
                msg_headers = msg.get('internetMessageHeaders', [])
                # Extract inReplyTo from headers if available
                for h in msg_headers:
                    if h.get('name', '').lower() in ('in-reply-to', 'x-in-reply-to'):
                        msg['inReplyTo'] = h.get('value', '')
                        break
                
            # Process and log debug info for each message
            import re
            def strip_html(html_content):
                """Simple HTML to text conversion"""
                if not html_content:
                    return ''
                # Remove style and script tags and their contents
                html_content = re.sub(r'<style.*?</style>', '', html_content, flags=re.DOTALL)
                html_content = re.sub(r'<script.*?</script>', '', html_content, flags=re.DOTALL)
                # Replace <br> and </p> with newlines
                html_content = re.sub(r'<br\s*/?>|</p>', '\n', html_content)
                # Remove all other HTML tags
                html_content = re.sub(r'<[^>]+>', '', html_content)
                # Fix whitespace
                html_content = re.sub(r'\s+', ' ', html_content)
                return html_content.strip()

            for msg in batch:
                # Extract message body from different possible sources
                body_content = ''
                if 'body' in msg:
                    body_content = msg['body'].get('content', '')
                    content_type = msg['body'].get('contentType', '').lower()
                    if content_type == 'html':
                        try:
                            body_content = strip_html(body_content)
                        except Exception as e:
                            logger.error(f"[GRAPH] Failed to parse HTML body: {e}")
                
                # Use uniqueBody if available (strips out quoted text)
                unique_body = msg.get('uniqueBody', {}).get('content', '')
                if unique_body:
                    try:
                        unique_body = strip_html(unique_body)
                    except Exception as e:
                        logger.error(f"[GRAPH] Failed to parse unique body: {e}")
                
                # Fall back to bodyPreview if needed
                preview = msg.get('bodyPreview', '')
                
                # Use the best available content
                final_body = unique_body or body_content or preview
                msg['processed_body'] = final_body.strip()
                
                logger.debug(f"[GRAPH] Message {msg.get('id', 'unknown')}:")
                logger.debug(f"[GRAPH] - Subject: {msg.get('subject', '')}")
                logger.debug(f"[GRAPH] - Content Type: {msg.get('body', {}).get('contentType', 'unknown')}")
                logger.debug(f"[GRAPH] - Has body: {bool(body_content)}")
                logger.debug(f"[GRAPH] - Has unique body: {bool(unique_body)}")
                logger.debug(f"[GRAPH] - Final body length: {len(final_body)}")
                if final_body:
                    logger.debug(f"[GRAPH] - Preview: {final_body[:100]}...")
            
            messages.extend(batch)
            fetched += len(batch)
            url = data.get('@odata.nextLink') if batch and fetched < max_messages else None
            
    except Exception as e:
        logger.error(f"Error fetching messages: {str(e)}", exc_info=True)
        
    logger.debug(f"[GRAPH] Fetched {len(messages)} messages with headers")
    return messages[:max_messages]