# JSON Parse Error Fix - 404 Error Solved

## Problem
You were seeing:
```
Error loading messages: SyntaxError: Unexpected token '<', "<!doctype "... is not valid JSON
```

This meant the backend was returning **HTML** (error page) instead of **JSON** data.

## Root Cause
The `CustomMessagesModal` component was using plain `fetch()` instead of the `authFetch()` utility that includes the authentication token. When no valid token was sent:

1. Backend rejected the request (403 Unauthorized)
2. Returned an HTML error page instead of JSON
3. Frontend tried to parse HTML as JSON → SyntaxError

## Solution ✅

### Updated: `src/components/CustomMessagesModal.tsx`

**Import authFetch:**
```typescript
import { authFetch } from '../utils/authFetch';
```

**Replace all fetch() calls with authFetch():**

Before:
```typescript
const response = await fetch(
  `/api/campaign_contacts/${contactId}/messages`
);
```

After:
```typescript
const response = await authFetch(
  `/campaign_contacts/${contactId}/messages`
);
```

**All three API calls updated:**
1. ✅ `loadMessages()` - GET messages
2. ✅ `handleSave()` - POST custom message
3. ✅ `handleDelete()` - DELETE custom message

**Note:** Also removed `/api` prefix from URLs because `authFetch` automatically adds it based on VITE_API_URL configuration.

## How authFetch Works

The `authFetch` utility (from `src/utils/authFetch.ts`):
- Reads JWT token from `localStorage.getItem('token')`
- Adds `Authorization: Bearer {token}` header
- Handles relative/absolute URLs correctly
- Sets proper Content-Type for JSON requests

## Why This Fixes It

✅ **Now with authFetch:**
1. Browser sends authentication token
2. Backend validates token → OK
3. Backend returns valid JSON
4. Frontend parses JSON successfully
5. Modal displays messages

❌ **Before (plain fetch):**
1. No auth token sent
2. Backend returns 403 HTML error
3. Frontend tries to parse as JSON
4. SyntaxError crash

## Testing the Fix

1. **Restart frontend** (reload page or dev server)
2. **Click ✎ icon** on any contact
3. **Should see:**
   - Modal opens
   - Messages load successfully (no error)
   - List of message flows displayed
   - Able to edit and save

4. **Check browser console:**
   - No more "Unexpected token '<'" error
   - Clean network requests with Authorization header

## Files Modified

| File | Change |
|------|--------|
| `src/components/CustomMessagesModal.tsx` | Added `authFetch` import, replaced all 3 fetch() calls |
| `contact_messages.py` | Simplified template loading (returns empty strings instead of trying to load files) |
| `main.py` | Added table creation to init_db() |

## Related Files (Not Changed)
- `src/utils/authFetch.ts` - Already correctly configured ✅
- Backend endpoints - Already working correctly ✅
- Database schema - Already created on startup ✅

## Next Steps

If you still see errors:

1. **Check browser Network tab:**
   - Should see `Authorization: Bearer xxx` header
   - Response should be JSON array, not HTML

2. **Check backend logs:**
   ```
   [GET MESSAGES] Error fetching messages for contact 25: ...
   ```

3. **Verify token is stored:**
   ```javascript
   localStorage.getItem('token')  // Should return JWT token
   ```

4. **Test endpoint manually:**
   ```bash
   curl -H "Authorization: Bearer YOUR_TOKEN" \
     http://localhost:8000/api/campaign_contacts/25/messages
   ```
