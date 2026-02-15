"""
Contact Relations Module
Handles tracking when contacts appear in multiple events
"""

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from typing import Optional, List
from pydantic import BaseModel
import logging
import inspect

logger = logging.getLogger(__name__)

# Models for request/response
class ContactEventRelation(BaseModel):
    event_id: int
    contact_id: int
    event_name: str
    status: Optional[str] = None
    stage: Optional[str] = None

class ContactRelationsResponse(BaseModel):
    email: str
    total_events: int
    relations: List[ContactEventRelation]

def create_contact_relations_router():
    """Factory function to create the contact relations router"""
    router = APIRouter()
    
    def get_db_pool():
        """Get the DB pool from main module"""
        import main
        return main.get_db_pool()
    
    async def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(HTTPBearer())):
        """Get current user from main module"""
        import main
        user = main.get_current_user(credentials)
        if inspect.isawaitable(user):
            user = await user
        return user
    
    @router.get("/campaign_contacts/{contact_id}/relations", response_model=ContactRelationsResponse)
    async def get_contact_relations(
        contact_id: int,
        current_user: dict = Depends(get_current_user)
    ):
        """
        Get all events where this contact appears with their status/stage in each event.
        """
        pool = get_db_pool()
        if not pool:
            raise HTTPException(status_code=503, detail="Database pool not available")
        
        try:
            async with pool.acquire() as conn:
                # Get the contact's email
                contact = await conn.fetchrow("""
                    SELECT email FROM campaign_contacts WHERE id = $1
                """, contact_id)
                
                if not contact:
                    raise HTTPException(status_code=404, detail="Contact not found")
                
                contact_email = contact['email']
                
                # Get all events where this contact appears
                relations = await conn.fetch("""
                    SELECT 
                        cc.id as contact_id,
                        cc.event_id,
                        e.event_name,
                        cc.status,
                        cc.stage
                    FROM campaign_contacts cc
                    JOIN event e ON cc.event_id = e.id
                    WHERE cc.email = $1
                    ORDER BY e.id DESC
                """, contact_email)
                
                if not relations:
                    return ContactRelationsResponse(
                        email=contact_email,
                        total_events=0,
                        relations=[]
                    )
                
                formatted_relations = [
                    ContactEventRelation(
                        event_id=r['event_id'],
                        contact_id=r['contact_id'],
                        event_name=r['event_name'] or f"Event {r['event_id']}",
                        status=r['status'],
                        stage=r['stage']
                    )
                    for r in relations
                ]
                
                logger.info(f"[RELATIONS] Contact {contact_id} ({contact_email}) found in {len(formatted_relations)} events")
                
                return ContactRelationsResponse(
                    email=contact_email,
                    total_events=len(formatted_relations),
                    relations=formatted_relations
                )
        
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"[RELATIONS] Error fetching relations for contact {contact_id}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to fetch contact relations: {str(e)}")
    
    @router.get("/campaign_contacts/email-relations/{email}", response_model=List[ContactEventRelation])
    async def get_email_relations(
        email: str,
        current_user: dict = Depends(get_current_user)
    ):
        """
        Get all events where a specific email appears.
        """
        if not email or not email.strip():
            raise HTTPException(status_code=400, detail="Email is required")
        
        pool = get_db_pool()
        if not pool:
            raise HTTPException(status_code=503, detail="Database pool not available")
        
        try:
            async with pool.acquire() as conn:
                relations = await conn.fetch("""
                    SELECT 
                        cc.id as contact_id,
                        cc.event_id,
                        e.event_name,
                        cc.status,
                        cc.stage
                    FROM campaign_contacts cc
                    JOIN event e ON cc.event_id = e.id
                    WHERE cc.email ILIKE $1
                    ORDER BY e.id DESC
                """, email.strip())
                
                if not relations:
                    return []
                
                return [
                    ContactEventRelation(
                        event_id=r['event_id'],
                        contact_id=r['contact_id'],
                        event_name=r['event_name'] or f"Event {r['event_id']}",
                        status=r['status'],
                        stage=r['stage']
                    )
                    for r in relations
                ]
        
        except Exception as e:
            logger.error(f"[RELATIONS] Error fetching relations for email {email}: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to fetch email relations: {str(e)}")
    
    return router

# Export the router
router = create_contact_relations_router()
