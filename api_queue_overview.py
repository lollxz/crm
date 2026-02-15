from fastapi import APIRouter, HTTPException, Request
import asyncio
import logging
import re

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get('/api/queue/overview')
async def get_queue_overview(request: Request):
    """Aggregate queue stage counts into explicit buckets used by the UI.

    Returns overview with keys:
      - initial: first_message_sent, first_reminder, second_reminder, total
      - forms: forms_main, forms_reminder1_sent, forms_reminder2_sent, forms_reminder3_sent, total
      - payments: payments_initial, payments_reminder1_sent .. payments_reminder6_sent, total
      - custom_flow: step1..N + total
      - errors: total_items, contacts, by_error_message (only errors from the last 30 days)
    """
    try:
        pool = None
        try:
            pool = getattr(request.app.state, 'db_pool', None)
        except Exception:
            pool = None

        if pool is None:
            raise HTTPException(status_code=503, detail='db_pool is not initialized')

        conn = await pool.acquire()
        try:
            stage_rows = await conn.fetch("""
                -- Prefer authoritative counts from campaign_contacts.status (one row per contact)
                -- Exclude finalized contacts
                SELECT LOWER(COALESCE(cc.status, '')) AS status, LOWER(COALESCE(cc.stage, 'initial')) AS stage, COUNT(DISTINCT cc.id) AS cnt
                FROM campaign_contacts cc
                WHERE cc.status != 'finalized'
                GROUP BY LOWER(COALESCE(cc.status, '')), LOWER(COALESCE(cc.stage, 'initial'))
            """)

            # Also include pending email_queue entries for contacts that are not in campaign_contacts
            eq_rows = await conn.fetch("""
                SELECT LOWER(COALESCE(eq.campaign_stage, eq.last_message_type, '')) AS status, COUNT(DISTINCT eq.contact_id) AS cnt
                FROM email_queue eq
                WHERE eq.status = 'pending' AND eq.contact_id NOT IN (SELECT id FROM campaign_contacts)
                GROUP BY LOWER(COALESCE(eq.campaign_stage, eq.last_message_type, ''))
            """)

            cf_rows = await conn.fetch("""
                 SELECT s.step_order, COUNT(DISTINCT cf.contact_id) AS cnt
                 FROM custom_flows cf
                 JOIN custom_flow_steps s ON s.flow_id = cf.id
                 WHERE cf.active IS TRUE
                 GROUP BY s.step_order
                 ORDER BY s.step_order
             """)

            # Limit error summaries to the last 30 days
            # Note: this assumes the email_queue table has a timestamp column named `created_at`.
            # If your schema uses a different column name, adjust the WHERE clause accordingly.
            try:
                failed_items = await conn.fetchval(
                    "SELECT COUNT(*) FROM email_queue WHERE status = 'failed' AND created_at >= now() - INTERVAL '30 days'"
                )
            except Exception:
                failed_items = 0
            try:
                failed_contacts = await conn.fetchval(
                    "SELECT COUNT(DISTINCT contact_id) FROM email_queue WHERE status = 'failed' AND created_at >= now() - INTERVAL '30 days'"
                )
            except Exception:
                failed_contacts = 0
            try:
                reason_rows = await conn.fetch("""
                    SELECT COALESCE(error_message, '(unknown)') AS error_message, COUNT(*) AS cnt
                    FROM email_queue
                    WHERE status = 'failed' AND created_at >= now() - INTERVAL '30 days'
                    GROUP BY error_message
                    ORDER BY cnt DESC
                    LIMIT 50
                """)
            except Exception:
                reason_rows = []
        finally:
            release = getattr(pool, 'release', None)
            if release:
                try:
                    res = release(conn)
                    if asyncio.iscoroutine(res):
                        await res
                except Exception:
                    pass
            else:
                close_fn = getattr(conn, 'close', None)
                if close_fn:
                    try:
                        res = close_fn()
                        if asyncio.iscoroutine(res):
                            await res
                    except Exception:
                        pass

        # Initialize explicit buckets the UI expects
        overview = {
            'initial': {
                'first_message_sent': 0,
                'first_reminder': 0,
                'second_reminder': 0,
                'total': 0
            },
            'forms': {
                'forms_main': 0,
                'forms_reminder1_sent': 0,
                'forms_reminder2_sent': 0,
                'forms_reminder3_sent': 0,
                'total': 0
            },
            'payments': {
                'payments_initial': 0,
                'payments_reminder1_sent': 0,
                'payments_reminder2_sent': 0,
                'payments_reminder3_sent': 0,
                'payments_reminder4_sent': 0,
                'payments_reminder5_sent': 0,
                'payments_reminder6_sent': 0,
                'total': 0
            },
            'custom_flow': {'total': 0}
        }

        def map_status(status_str: str):
            # Map contact status / queued message types to (category, key)
            if not status_str:
                return ('initial', 'first_message_sent')
            s = status_str.lower()

            # Direct canonical keys
            if s in ('first_message_sent', 'first_reminder', 'second_reminder'):
                return ('initial', s)

            # Initial / campaign main variants
            if 'campaign_main' in s or 'campaign_main_sent' in s or 'first_message' in s:
                return ('initial', 'first_message_sent')
            if 'reminder1' in s and 'forms' not in s and 'payments' not in s:
                return ('initial', 'first_reminder')
            if 'reminder2' in s and 'forms' not in s and 'payments' not in s:
                return ('initial', 'second_reminder')

            # Forms mapping: normalize variants to forms_main
            if 'forms_initial_sent' in s or 'forms_initial' in s or 'forms_main' in s or s == 'forms':
                return ('forms', 'forms_main')
            if 'forms_reminder1' in s or 'forms_reminder1_sent' in s:
                return ('forms', 'forms_reminder1_sent')
            if 'forms_reminder2' in s or 'forms_reminder2_sent' in s:
                return ('forms', 'forms_reminder2_sent')
            if 'forms_reminder3' in s or 'forms_reminder3_sent' in s:
                return ('forms', 'forms_reminder3_sent')

            # Payments mapping: normalize payment_main/variants to payments_initial
            if 'payment_main' in s or 'payment_main_sent' in s or 'payments_main' in s:
                return ('payments', 'payments_initial')
            for i in range(1, 7):
                if f'payments_reminder{i}' in s or f'payments_reminder{i}_sent' in s:
                    return ('payments', f'payments_reminder{i}_sent')
            if 'payments_initial' in s or 'payments_initial_sent' in s or (s.startswith('payments') and ('initial' in s or 'first' in s)):
                return ('payments', 'payments_initial')
            if 'payments' in s or 'payment' in s:
                return ('payments', 'payments_initial')

            # Custom flow indicators: step-#, custom-step-# etc
            if 'step-' in s or 'custom-step' in s or s.startswith('step'):
                return ('custom_flow', s)

            # Fallback: put unknown statuses under initial with original key
            return ('initial', status_str)

        # Map campaign_contacts status rows into explicit buckets
        for r in (stage_rows or []):
            status_val = r.get('status') if isinstance(r, dict) else (getattr(r, 'get', lambda k: None)('status'))
            cnt_val = r.get('cnt') if isinstance(r, dict) else (getattr(r, 'get', lambda k: 0)('cnt'))
            try:
                cnt = int(cnt_val or 0)
            except Exception:
                cnt = 0

            cat, key = map_status(status_val)
            # Ensure the key exists in overview cat, otherwise create it
            if key not in overview.get(cat, {}):
                overview.setdefault(cat, {})[key] = 0
            overview[cat][key] = overview[cat].get(key, 0) + cnt
            overview[cat]['total'] = overview[cat].get('total', 0) + cnt

        # Include pending email_queue entries for contacts not in campaign_contacts
        for r in (eq_rows or []):
            status_val = r.get('status') if isinstance(r, dict) else (getattr(r, 'get', lambda k: None)('status'))
            cnt_val = r.get('cnt') if isinstance(r, dict) else (getattr(r, 'get', lambda k: 0)('cnt'))
            try:
                cnt = int(cnt_val or 0)
            except Exception:
                cnt = 0
            cat, key = map_status(status_val)
            if key not in overview.get(cat, {}):
                overview.setdefault(cat, {})[key] = 0
            overview[cat][key] = overview[cat].get(key, 0) + cnt
            overview[cat]['total'] = overview[cat].get('total', 0) + cnt

        # Aggregate custom flow steps
        total_cf = 0
        for r in (cf_rows or []):
            try:
                step_order = int(r.get('step_order') or 0) if isinstance(r, dict) else int(getattr(r, 'get', lambda k: 0)('step_order'))
            except Exception:
                step_order = 0
            try:
                cnt = int(r.get('cnt') or 0) if isinstance(r, dict) else int(getattr(r, 'get', lambda k: 0)('cnt'))
            except Exception:
                cnt = 0
            key = f"step{step_order + 1}"
            overview['custom_flow'][key] = cnt
            total_cf += cnt
        overview['custom_flow']['total'] = total_cf

        # Attach errors summary
        try:
            ei = int(failed_items or 0)
        except Exception:
            ei = 0
        try:
            ec = int(failed_contacts or 0)
        except Exception:
            ec = 0
        errors_map = {}
        for rr in (reason_rows or []):
            key = rr.get('error_message') if isinstance(rr, dict) else getattr(rr, 'get', lambda k: None)('error_message')
            cnt = rr.get('cnt') if isinstance(rr, dict) else getattr(rr, 'get', lambda k: 0)('cnt')
            try:
                errors_map[key] = int(cnt or 0)
            except Exception:
                errors_map[key] = 0

        overview['errors'] = {
            'total_items': ei,
            'contacts': ec,
            'by_error_message': errors_map
        }

        return overview

    except HTTPException:
        raise
    except Exception as e:
        logger.exception('Error in queue overview')
        raise HTTPException(status_code=500, detail=str(e))
