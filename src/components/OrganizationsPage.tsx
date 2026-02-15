import React, { useEffect, useState } from 'react';
import { authFetch } from '@/utils/authFetch';
import OrgSelector from './OrgSelector';
import { ContactsTable } from './ContactsTable';

// --- Small inline component for event name (same behavior as MainApp) ---
function EventNameCell({ name }: { name: string }) {
  const [expanded, setExpanded] = useState(false);
  if (!name) return null;
  const copyToClipboard = () => {
    navigator.clipboard.writeText(name);
    try { /* toast not imported here; keep silent */ } catch {}
  };
  if (name.length <= 40) return <span>{name}</span>;
  return (
    <div className="flex flex-col max-w-xs">
      <span className="break-words">{expanded ? name : `${name.slice(0,40)}...`}</span>
      <div className="flex gap-2 mt-1">
        <button onClick={() => setExpanded(!expanded)} className="text-blue-600 text-xs hover:underline">
          {expanded ? 'Show Less' : 'Read More'}
        </button>
        <button onClick={copyToClipboard} className="text-green-600 text-xs hover:underline">Copy</button>
      </div>
    </div>
  );
}

// --- Small inline component for event note (simplified, uses authFetch) ---
function EventNoteCell({ event, onSaved }: { event: any; onSaved?: () => void }) {
  const [editing, setEditing] = useState<boolean>(false);
  const [value, setValue] = useState<string>(event.note || '');
  const [saving, setSaving] = useState<boolean>(false);
  const [file, setFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState<boolean>(false);
  const [attachmentUrl, setAttachmentUrl] = useState<string | null>(event.attachment_url || null);

  const save = async () => {
    setSaving(true);
    try {
      const API_BASE = import.meta.env.VITE_API_URL || 'http://161.97.90.139:9000';
      const res = await authFetch(`${API_BASE}/events/${event.id}`, { method: 'PATCH', body: JSON.stringify({ note: value }) });
      if (!res.ok) throw new Error('Save failed');
      setEditing(false);
      if (onSaved) onSaved();
    } catch (e) {
      console.error('Failed to save note', e);
    } finally {
      setSaving(false);
    }
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files && e.target.files[0];
    setFile(f || null);
  };

  const uploadAttachment = async () => {
    if (!file) return;
    setUploading(true);
    try {
      const API_BASE = import.meta.env.VITE_API_URL || 'http://161.97.90.139:9000';
      const fd = new FormData();
      fd.append('file', file as Blob, file.name);
      const res = await authFetch(`${API_BASE}/events/${event.id}/attachment`, { method: 'POST', body: fd });
      if (!res.ok) throw new Error('Upload failed');
      const data = await res.json();
      setAttachmentUrl(data.attachment_url);
      setFile(null);
      if (onSaved) onSaved();
    } catch (e) {
      console.error('Failed to upload attachment', e);
    } finally {
      setUploading(false);
    }
  };

  return (
    <div>
      <div className="flex flex-col gap-1">
        <div className="text-sm text-gray-800">
          <div className="truncate max-w-xs">{event.note || '-'}</div>
          <div className="flex gap-2 mt-1 items-center">
            <button onClick={() => setEditing(true)} className="text-xs text-blue-600 hover:underline">Edit</button>
            {attachmentUrl ? (<a className="text-xs text-green-600 hover:underline" href={`${import.meta.env.VITE_API_URL || ''}${attachmentUrl}`} target="_blank" rel="noreferrer">View Attachment</a>) : null}
          </div>
        </div>

        {editing && (
          <div className="flex flex-col gap-2 items-start">
            <div className="flex gap-2 items-center">
              <input value={value} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setValue(e.target.value)} className="border px-2 py-1 rounded w-56 text-sm" placeholder="Add a note" />
              <button onClick={save} disabled={saving} className="px-2 py-1 bg-green-600 text-white rounded text-xs">Save</button>
              <button onClick={() => { setEditing(false); setValue(event.note || ''); }} className="px-2 py-1 bg-gray-200 text-xs rounded">Cancel</button>
            </div>

            <div className="flex gap-2 items-center">
              <input type="file" onChange={handleFileChange} className="text-xs" />
              <button onClick={uploadAttachment} disabled={uploading || !file} className="px-2 py-1 bg-blue-600 text-white rounded text-xs">{uploading ? 'Uploading...' : 'Upload Attachment'}</button>
              {attachmentUrl && (<a className="text-xs text-gray-600" href={`${import.meta.env.VITE_API_URL || ''}${attachmentUrl}`} target="_blank" rel="noreferrer">Open in new tab</a>)}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}

// Helper to display shortened event URL (domain) similar to MainApp
function getDisplayUrl(rawUrl?: string | null) {
  if (!rawUrl) return '';
  try {
    let url = rawUrl.trim();
    if (!/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(url)) url = 'http://' + url;
    const u = new URL(url);
    return u.hostname.replace(/^www\./, '');
  } catch (e) {
    return rawUrl.length > 50 ? rawUrl.slice(0, 47) + '...' : rawUrl;
  }
}

export default function OrganizationsPage() {
  const [orgs, setOrgs] = useState<any[]>([]);
  const [isAdmin, setIsAdmin] = useState<boolean>(false);
  const [editingNoteFor, setEditingNoteFor] = useState<number | null>(null);
  const [noteDrafts, setNoteDrafts] = useState<Record<number, string>>({});
  const [openOrg, setOpenOrg] = useState<number | null>(null);
  const [openEvent, setOpenEvent] = useState<number | null>(null);
  const [editingOrg, setEditingOrg] = useState<any | null>(null);
  const [editName, setEditName] = useState<string>('');
  const [editLoading, setEditLoading] = useState<boolean>(false);
  const [showDeleteModal, setShowDeleteModal] = useState<boolean>(false);
  const [deleteTargetOrg, setDeleteTargetOrg] = useState<number | null>(null);
  const [deleteLoading, setDeleteLoading] = useState<boolean>(false);
  const [eventsByOrg, setEventsByOrg] = useState<Record<number, any[]>>({});
  const [eventsCount, setEventsCount] = useState<Record<number, number | null>>({});
  const [eventContactsCount, setEventContactsCount] = useState<Record<number, number>>({});
  const [searchQuery, setSearchQuery] = useState<string>('');
  const [searchResults, setSearchResults] = useState<any | null>(null);
  const [eventsLoading, setEventsLoading] = useState<Record<number, boolean>>({});
  const [, setLoading] = useState<boolean>(false);
  const [selectedFiles, setSelectedFiles] = useState<Record<number, File | null>>({});
  const [previewURLs, setPreviewURLs] = useState<Record<number, { url: string; type: string } | null>>({});
  const [uploading, setUploading] = useState<Record<number, boolean>>({});

  useEffect(() => { fetchOrgs(); }, []);

  useEffect(() => {
    try {
      const token = localStorage.getItem('token');
      if (token) {
        const payload = JSON.parse(atob(token.split('.')[1]));
        setIsAdmin(Boolean(payload.is_admin));
      }
    } catch (e) {
      // ignore
    }
  }, []);

  async function fetchOrgs() {
    setLoading(true);
    try {
      const API_BASE = import.meta.env.VITE_API_URL || 'http://161.97.90.139:9000';
      const res = await authFetch(`${API_BASE}/organizations/`);
      if (!res.ok) {
        let detail = '';
        try {
          const clone = res.clone();
          const j = await clone.json();
          detail = j.detail || JSON.stringify(j);
        } catch {
          try { detail = await res.text(); } catch { detail = `HTTP ${res.status}`; }
        }
        throw new Error(`Failed to load organizations: ${detail}`);
      }
      let data: any = null;
      try {
        data = await res.json();
      } catch {
        const text = await res.text().catch(() => '<unreadable response>');
        console.error('Organizations endpoint returned non-JSON response:', text);
        throw new Error('Organizations endpoint returned invalid JSON');
      }
      setOrgs(data || []);
      // Fetch event counts for all organizations in a single call
      try {
        const API_BASE = import.meta.env.VITE_API_URL || 'http://161.97.90.139:9000';
        const countsRes = await authFetch(`${API_BASE}/organizations/event_counts`);
        if (countsRes.ok) {
          const countsData = await countsRes.json();
          const map: Record<number, number> = {};
          if (Array.isArray(countsData)) {
            countsData.forEach((r: any) => { map[r.id] = typeof r.event_count === 'number' ? r.event_count : Number(r.event_count || 0); });
          }
          setEventsCount(map);
        } else {
          // leave eventsCount empty; frontend will fallback to fetching per-org when expanded
        }
      } catch (e) {
        console.error('Failed to fetch organization event counts:', e);
      }
    } catch (e) {
      console.error('Failed to fetch organizations:', e);
    } finally { setLoading(false); }
  }

  async function fetchEventsForOrg(orgId: number) {
    setEventsLoading((prev) => ({ ...prev, [orgId]: true }));
    try {
      const API_BASE = import.meta.env.VITE_API_URL || 'http://161.97.90.139:9000';
      const res = await authFetch(`${API_BASE}/organizations/${orgId}/events`);
      if (!res.ok) {
        console.error('Failed to fetch events for org', orgId, res.status);
        setEventsByOrg((prev) => ({ ...prev, [orgId]: [] }));
        return;
      }
      const data = await res.json();
      setEventsByOrg((prev) => ({ ...prev, [orgId]: data }));
  // Also update count
  setEventsCount((prev) => ({ ...prev, [orgId]: Array.isArray(data) ? data.length : 0 }));
      // Also fetch contacts count per event (best-effort) so we can show the badge
      try {
        const countsMap: Record<number, number> = {};
        await Promise.allSettled((data || []).map(async (ev: any) => {
          try {
            const cRes = await authFetch(`${API_BASE}/organizations/events/${ev.id}/contacts`);
            if (!cRes.ok) {
              countsMap[ev.id] = 0;
              return;
            }
            const cData = await cRes.json();
            countsMap[ev.id] = Array.isArray(cData) ? cData.length : (Array.isArray(cData.contacts) ? cData.contacts.length : 0);
          } catch (e) {
            countsMap[ev.id] = 0;
          }
        }));
        setEventContactsCount((prev) => ({ ...prev, ...countsMap }));
      } catch (e) {
        // ignore
      }
    } catch (e) {
      console.error('fetchEventsForOrg error', e);
      setEventsByOrg((prev) => ({ ...prev, [orgId]: [] }));
    } finally {
      setEventsLoading((prev) => ({ ...prev, [orgId]: false }));
    }
  }

  const deleteTargetOrgName = deleteTargetOrg ? (orgs.find((o: any) => o.id === deleteTargetOrg)?.name ?? '') : '';

  async function handleDeleteEvent(eventId: number, orgId: number) {
    if (!window.confirm('Delete this event and all its contacts? This cannot be undone.')) return;
    try {
      const API_BASE = import.meta.env.VITE_API_URL || 'http://161.97.90.139:9000';
      const res = await authFetch(`${API_BASE}/events/${eventId}`, { method: 'DELETE' });
      if (!res.ok) throw new Error('Delete failed');
      // Refresh events for this org
      await fetchEventsForOrg(orgId);
    } catch (e) {
      console.error('Failed to delete event', e);
    }
  }

  

  function toggleOrg(orgId: number) {
    if (openOrg === orgId) { setOpenOrg(null); return; }
    setOpenOrg(orgId);
    fetchEventsForOrg(orgId).catch(() => {});
  }

  function deleteOrg(orgId: number) {
    // Open confirmation modal instead of immediate delete
    setDeleteTargetOrg(orgId);
    setShowDeleteModal(true);
  }

  async function confirmDeleteOrg() {
    if (!deleteTargetOrg) return;
    if (!window.confirm) {
      // defensive, should not happen
    }
    setDeleteLoading(true);
    try {
      const API_BASE = import.meta.env.VITE_API_URL || 'http://161.97.90.139:9000';
      const res = await authFetch(`${API_BASE}/organizations/${deleteTargetOrg}`, { method: 'DELETE' });
      if (!res.ok) {
        const txt = await res.text().catch(() => '<unreadable>');
        console.error('Failed to delete organization', deleteTargetOrg, res.status, txt);
        alert('Failed to delete organization');
        return;
      }
      // Refresh list
      await fetchOrgs();
      setShowDeleteModal(false);
      setDeleteTargetOrg(null);
    } catch (e) {
      console.error('confirmDeleteOrg error', e);
      alert('Failed to delete organization');
    } finally {
      setDeleteLoading(false);
    }
  }

  function cancelDelete() {
    setShowDeleteModal(false);
    setDeleteTargetOrg(null);
  }
  function openEditOrgModal(orgId: number) {
    const current = orgs.find((o: any) => o.id === orgId);
    if (!current) return;
    setEditingOrg(current);
    setEditName(current.name || '');
  }

  function closeEditOrgModal() {
    setEditingOrg(null);
    setEditName('');
    setEditLoading(false);
  }

  async function saveEditOrg() {
    if (!editingOrg) return;
    const trimmed = String(editName || '').trim();
    if (!trimmed) return alert('Name cannot be empty');
    setEditLoading(true);
    try {
      const API_BASE = import.meta.env.VITE_API_URL || 'http://161.97.90.139:9000';
      const res = await authFetch(`${API_BASE}/organizations/${editingOrg.id}`, { method: 'PATCH', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ name: trimmed }) });
      if (!res.ok) {
        const txt = await res.text().catch(() => '<unreadable>');
        console.error('Failed to edit organization', editingOrg.id, res.status, txt);
        alert('Failed to save organization');
        return;
      }
      const updated = await res.json();
      setOrgs((prev) => prev.map((o) => o.id === editingOrg.id ? { ...o, name: updated.name ?? trimmed } : o));
      closeEditOrgModal();
    } catch (e) {
      console.error('saveEditOrg error', e);
      alert('Failed to save organization');
    } finally {
      setEditLoading(false);
    }
  }

  function onSelectSearchOrg(orgId: number) {
    setOpenOrg(orgId);
    fetchEventsForOrg(orgId).catch(() => {});
    const el = document.querySelector(`tr[data-org-id-rows='${orgId}']`);
    if (el) (el as HTMLElement).scrollIntoView({ behavior: 'smooth', block: 'center' });
  }

  function handleFileChange(orgId: number, e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (!file) return;
    const url = URL.createObjectURL(file);
    setSelectedFiles((prev) => ({ ...prev, [orgId]: file }));
    setPreviewURLs((prev) => ({ ...prev, [orgId]: { url, type: file.type } }));
  }

  function removePreview(orgId: number) {
    const prev = previewURLs[orgId];
    if (prev && prev.url) URL.revokeObjectURL(prev.url);
    setPreviewURLs((prev) => {
      const copy = { ...prev };
      delete copy[orgId];
      return copy;
    });
    setSelectedFiles((prev) => ({ ...prev, [orgId]: null }));
  }

  async function uploadAttachment(orgId: number) {
    const file = selectedFiles[orgId];
    if (!file) {
      console.warn('No file selected for upload', orgId);
      return;
    }
    try {
      setUploading((prev) => ({ ...prev, [orgId]: true }));
      const API_BASE = import.meta.env.VITE_API_URL || 'http://161.97.90.139:9000';
      const url = `${API_BASE}/organizations/${orgId}/attachment`;
      const form = new FormData();
      form.append('file', file);
      const res = await authFetch(url, { method: 'POST', body: form });
      if (!res.ok) {
        const txt = await res.text().catch(() => '<unreadable>');
        console.error('Attachment upload failed', orgId, res.status, txt);
        return;
      }
      let data: any = null;
      try { data = await res.json(); } catch { data = null; }
      if (data && (data.attachment_url || data.attachmentUrl)) {
        const urlReturned = data.attachment_url || data.attachmentUrl;
        setOrgs((prev) => prev.map((o) => o.id === orgId ? { ...o, attachment_url: urlReturned } : o));
      }
      removePreview(orgId);
    } catch (e) {
      console.error('Upload exception', e);
    } finally {
      setUploading((prev) => ({ ...prev, [orgId]: false }));
    }
  }

  function toggleEvent(evId: number) {
    if (openEvent === evId) { setOpenEvent(null); return; }
    setOpenEvent(evId);
  }

  // Client-side search function
  function performSearch(query: string) {
    if (!query.trim()) {
      setSearchResults(null);
      return;
    }

    const lowerQuery = query.toLowerCase().trim();
    
    // Search organizations
    const matchingOrgs = orgs.filter((org: any) => 
      org.name?.toLowerCase().includes(lowerQuery) ||
      org.note?.toLowerCase().includes(lowerQuery)
    );

    // Search events across all organizations
    const matchingEvents: any[] = [];
    Object.values(eventsByOrg).forEach((events: any[]) => {
      events.forEach((event: any) => {
        if (
          event.event_name?.toLowerCase().includes(lowerQuery) ||
          event.org_name?.toLowerCase().includes(lowerQuery) ||
          event.sender_email?.toLowerCase().includes(lowerQuery) ||
          event.city?.toLowerCase().includes(lowerQuery) ||
          event.venue?.toLowerCase().includes(lowerQuery) ||
          event.month?.toLowerCase().includes(lowerQuery) ||
          event.date2?.toLowerCase().includes(lowerQuery) ||
          event.note?.toLowerCase().includes(lowerQuery) ||
          event.event_url?.toLowerCase().includes(lowerQuery)
        ) {
          matchingEvents.push(event);
        }
      });
    });

    setSearchResults({
      organizations: matchingOrgs,
      events: matchingEvents,
      count: matchingOrgs.length + matchingEvents.length
    });
  }

  // Update search results when query changes
  useEffect(() => {
    performSearch(searchQuery);
  }, [searchQuery, orgs, eventsByOrg]);

  // Helper to filter organizations based on search query
  const getFilteredOrgs = () => {
    if (!searchQuery.trim()) return orgs;
    return orgs.filter((org: any) => 
      org.name?.toLowerCase().includes(searchQuery.toLowerCase().trim()) ||
      org.note?.toLowerCase().includes(searchQuery.toLowerCase().trim())
    );
  };

  return (
    <div className="p-6">
      <h1 className="text-2xl font-semibold mb-4">Analysis</h1>

      <div className="mb-4 flex items-center gap-3">
        <input
          placeholder="Search organizations, events, contacts..."
          className="border rounded px-3 py-2 w-full"
          value={searchQuery}
          onChange={(e) => setSearchQuery(e.target.value)}
        />
      </div>

      {searchResults && (
        <div className="mb-4 bg-white rounded shadow p-3">
          <h2 className="font-semibold mb-2">Search results ({searchResults.count})</h2>
          <div className="grid grid-cols-2 gap-4">
            <div>
              <h3 className="font-medium mb-2">Organizations ({searchResults.organizations.length})</h3>
              <ul className="space-y-1">
                {(searchResults.organizations || []).length === 0 ? (
                  <li className="text-sm text-gray-500">No organizations found</li>
                ) : (
                  (searchResults.organizations || []).map((o: any) => (
                    <li key={o.id} className="py-1">
                      <button 
                        className="text-left text-blue-600 underline hover:text-blue-800" 
                        onClick={() => {
                          setOpenOrg(o.id);
                          fetchEventsForOrg(o.id).catch(() => {});
                          const el = document.querySelector(`tr[data-org-id-rows='${o.id}']`);
                          if (el) (el as HTMLElement).scrollIntoView({ behavior: 'smooth', block: 'center' });
                        }}
                      >
                        {o.name}
                      </button>
                      {o.note && <div className="text-xs text-gray-500">{o.note}</div>}
                    </li>
                  ))
                )}
              </ul>
            </div>
            <div>
              <h3 className="font-medium mb-2">Events ({searchResults.events.length})</h3>
              <ul className="space-y-1">
                {(searchResults.events || []).length === 0 ? (
                  <li className="text-sm text-gray-500">No events found</li>
                ) : (
                  (searchResults.events || []).map((ev: any) => (
                    <li key={ev.id} className="py-1 text-sm">
                      <div className="text-gray-800 font-medium">{ev.event_name}</div>
                      <div className="text-xs text-gray-500">
                        {ev.org_name && <span>{ev.org_name} • </span>}
                        {ev.sender_email}
                      </div>
                      <div className="text-xs text-gray-500">
                        {ev.city && <span>{ev.city} • </span>}
                        {ev.venue}
                      </div>
                    </li>
                  ))
                )}
              </ul>
            </div>

          </div>
        </div>
      )}

      {/* Edit organization modal */}
      {editingOrg && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-white rounded p-4 shadow w-full max-w-md">
            <h3 className="text-lg font-semibold mb-2">Edit organization</h3>
            <input
              value={editName}
              onChange={(e) => setEditName(e.target.value)}
              className="border rounded px-2 py-1 w-full mb-3"
              placeholder="Organization name"
            />
            <div className="flex justify-end gap-2">
              <button onClick={closeEditOrgModal} className="px-3 py-1 bg-gray-200 rounded">Cancel</button>
              <button onClick={saveEditOrg} disabled={editLoading} className="px-3 py-1 bg-green-600 text-white rounded">{editLoading ? 'Saving...' : 'Save'}</button>
            </div>
          </div>
        </div>
      )}

      {/* Delete confirmation modal */}
      {showDeleteModal && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-white rounded p-4 shadow w-full max-w-md">
            <h3 className="text-lg font-semibold mb-2">Delete organization</h3>
            <p className="text-sm text-gray-700 mb-4">Are you sure you want to permanently delete "{deleteTargetOrgName || 'this organization'}"? This cannot be undone. Events that referenced this organization will keep their records but lose the association.</p>
            <div className="flex justify-end gap-2">
              <button onClick={cancelDelete} className="px-3 py-1 bg-gray-200 rounded">Cancel</button>
              <button onClick={confirmDeleteOrg} disabled={deleteLoading} className="px-3 py-1 bg-red-600 text-white rounded">{deleteLoading ? 'Deleting...' : 'Delete'}</button>
            </div>
          </div>
        </div>
      )}

      <div className="bg-white rounded shadow">
        <table className="min-w-full">
          <thead>
            <tr className="text-left bg-gray-50">
              <th className="px-4 py-2">#</th>
              <th className="px-4 py-2">Name</th>
              <th className="px-4 py-2">Note</th>
              <th className="px-4 py-2">Actions</th>
            </tr>
          </thead>
          <tbody>
            {getFilteredOrgs().map((org: any, idx: number) => (
              <React.Fragment key={org.id}>
                <tr className="border-t hover:bg-gray-50" data-org-id-rows={org.id}>
                  <td className="px-4 py-3 font-medium">{orgs.length - idx}</td>
                  <td className="px-4 py-3 font-medium">
                    <div className="flex items-center gap-2">
                      <span>{org.name}</span>
                      {/* compact baby-blue badge with calendar icon + count */}
                      <span className="inline-flex items-center justify-center ml-2 px-2 py-0.5 rounded-full text-blue-700 text-xs font-medium" style={{ backgroundColor: '#d6f0ff' }} title="Number of events">
                        <svg xmlns="http://www.w3.org/2000/svg" className="w-3 h-3 mr-1 flex-shrink-0" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={1.5} strokeLinecap="round" strokeLinejoin="round">
                          <rect x="3" y="4" width="18" height="18" rx="2" ry="2" />
                          <path d="M16 2v4M8 2v4M3 10h18" />
                        </svg>
                        {typeof eventsCount[org.id] === 'number'
                          ? eventsCount[org.id]
                          : (Array.isArray(eventsByOrg[org.id]) ? eventsByOrg[org.id].length : 'â€”')}
                      </span>
                    </div>
                  </td>
                  <td className="px-4 py-3">
                    {editingNoteFor === org.id ? (
                      <div className="flex items-center gap-2">
                        <input
                          className="border rounded px-2 py-1"
                          value={noteDrafts[org.id] ?? org.note ?? ''}
                          onChange={(e) => setNoteDrafts((p) => ({ ...p, [org.id]: e.target.value }))}
                        />
                        <button className="btn btn-green" onClick={async () => {
                          const payload = { note: noteDrafts[org.id] ?? '' };
                          try {
                            const API_BASE = import.meta.env.VITE_API_URL || 'http://161.97.90.139:9000';
                            const res = await authFetch(`${API_BASE}/organizations/${org.id}`, { method: 'PATCH', body: JSON.stringify(payload) });
                            if (!res.ok) {
                              const txt = await res.text().catch(() => '<unreadable>');
                              console.error('Failed to save note', res.status, txt);
                              return;
                            }
                            const updated = await res.json();
                            setOrgs((prev) => prev.map((o) => o.id === org.id ? { ...o, note: updated.note } : o));
                            setEditingNoteFor(null);
                          } catch (e) {
                            console.error('Save note error', e);
                          }
                        }}>Save</button>
                        <button className="btn" onClick={() => { setEditingNoteFor(null); setNoteDrafts((p) => ({ ...p, [org.id]: org.note || '' })); }}>Cancel</button>
                      </div>
                    ) : (
                      <div onClick={() => { setEditingNoteFor(org.id); setNoteDrafts((p) => ({ ...p, [org.id]: org.note || '' })); }} style={{ cursor: 'pointer' }}>
                        {org.note || '-'}
                      </div>
                    )}
                  </td>

                  <td className="px-4 py-3">
                    <div className="flex items-center gap-2">
                      <button className="btn btn-blue" onClick={() => toggleOrg(org.id)}>
                        {openOrg === org.id ? 'Hide Events' : 'View Events'}
                      </button>
                      {isAdmin && (
                        <>
                          <button className="btn btn-yellow" onClick={() => openEditOrgModal(org.id)}>Edit</button>
                          <button className="btn btn-red" onClick={() => deleteOrg(org.id)}>Delete</button>
                        </>
                      )}
                    </div>
                  </td>
                </tr>

                {openOrg === org.id && (
                  <tr>
                    <td colSpan={4} className="bg-gray-50 p-4">
                      <div>
                        {/* Modern Compact Attachment Section */}
                        <div className="mb-4 bg-gray-50 rounded-xl border border-gray-200 p-4 w-fit">
                          <div className="flex items-center justify-between mb-2">
                            <h4 className="font-medium text-gray-700 flex items-center gap-2">
                              <svg xmlns="http://www.w3.org/2000/svg" className="h-5 w-5 text-blue-500" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M15.172 7l-6.586 6.586a2 2 0 002.828 2.828L18 9.828M8 7h8" />
                              </svg>
                              Attachment
                            </h4>
                            {uploading[org.id] && <span className="text-xs text-blue-600">Uploading...</span>}
                          </div>

                          {org.attachment_url ? (
                            <a
                              href={org.attachment_url.startsWith('http') ? org.attachment_url : `${import.meta.env.VITE_API_URL?.replace(/\/$/, '')}${org.attachment_url}`}
                              target="_blank"
                              rel="noreferrer"
                              className="text-blue-600 hover:text-blue-800 text-sm underline block mb-3"
                            >
                              View current attachment
                            </a>
                          ) : (
                            <div className="text-sm text-gray-500 mb-3">No attachment uploaded</div>
                          )}

                          <div className="flex items-center gap-3">
                            <label className="cursor-pointer text-sm bg-white border border-gray-300 rounded-lg px-3 py-1.5 hover:bg-gray-100 transition">
                              Choose File
                              <input type="file" accept="image/*,application/pdf" className="hidden" onChange={(e) => handleFileChange(org.id, e)} />
                            </label>

                            <button
                              onClick={() => uploadAttachment(org.id)}
                              disabled={!!uploading[org.id]}
                              className="text-sm bg-blue-600 text-white px-3 py-1.5 rounded-lg hover:bg-blue-700 disabled:opacity-50"
                            >
                              Upload
                            </button>

                            <button
                              onClick={() => removePreview(org.id)}
                              className="text-sm text-gray-600 border border-gray-300 px-3 py-1.5 rounded-lg hover:bg-gray-100"
                            >
                              Clear
                            </button>
                          </div>

                          {previewURLs[org.id] && (
                            <div className="mt-3 rounded-lg overflow-hidden border border-gray-200 bg-white shadow-sm">
                              {previewURLs[org.id]?.type === 'application/pdf' ? (
                                <object data={previewURLs[org.id]!.url} type="application/pdf" width="100%" height={250}>
                                  <a href={previewURLs[org.id]!.url} target="_blank" rel="noreferrer" className="text-blue-600 underline p-3 block">
                                    Open PDF
                                  </a>
                                </object>
                              ) : (
                                <img src={previewURLs[org.id]!.url} alt="preview" className="max-h-48 object-contain w-full" />
                              )}
                            </div>
                          )}
                        </div>

                        {/* Events table (styled like MainApp) */}
                        <div className="overflow-x-auto">
                          <table className="min-w-[1100px] w-full border-collapse bg-white shadow rounded-xl overflow-hidden">
                            <thead className="bg-gray-50">
                              <tr className="text-left text-sm font-semibold text-gray-900">
                                
                                <th className="px-3 py-3 border-b border-r border-gray-200">ID</th>
                                <th className="px-3 py-3 border-b border-r border-gray-200">Event Name</th>
                                <th className="px-3 py-3 border-b border-r border-gray-200">Org Name</th>
                                <th className="px-3 py-3 border-b border-r border-gray-200">Month</th>
                                <th className="px-3 py-3 border-b border-r border-gray-200">Sender Email</th>
                                <th className="px-3 py-3 border-b border-r border-gray-200">City</th>
                                <th className="px-3 py-3 border-b border-r border-gray-200">Venue</th>
                                <th className="px-3 py-3 border-b border-r border-gray-200">Date</th>
                                <th className="px-3 py-3 border-b border-r border-gray-200">Note</th>
                                <th className="px-3 py-3 border-b border-gray-200">Contacts</th>
                              </tr>
                            </thead>
                            <tbody className="text-sm text-gray-800">
                                  {((eventsByOrg[org.id] || []) as any[]).map((ev: any) => (
                                <React.Fragment key={ev.id}>
                                    <tr className="odd:bg-white even:bg-gray-50 hover:bg-blue-50/60 transition-colors">
                                    <td className="px-3 py-3 align-top border-b border-r border-gray-100">{ev.id}</td>
                                    <td className="px-3 py-3 align-top border-b border-r border-gray-100">
                                      <EventNameCell name={ev.event_name} />
                                      <div className="mt-1 text-xs text-gray-500">{ev.event_url ? (<a href={ev.event_url} target="_blank" rel="noreferrer" className="text-blue-600 underline">{ev.event_url}</a>) : (ev.org_name || (ev.org_id ? `Org #${ev.org_id}` : 'Unassigned'))}</div>
                                    </td>
                                    <td className="px-3 py-3 align-top border-b border-r border-gray-100">{ev.org_name || (ev.org_id ? `Org #${ev.org_id}` : 'Unassigned')}</td>
                                    <td className="px-3 py-3 align-top border-b border-r border-gray-100">{ev.month}</td>
                                    <td className="px-3 py-3 align-top border-b border-r border-gray-100"><a href={`mailto:${ev.sender_email}`} className="underline-offset-2 hover:underline">{ev.sender_email}</a></td>
                                    <td className="px-3 py-3 align-top border-b border-r border-gray-100">{ev.city}</td>
                                    <td className="px-3 py-3 align-top border-b border-gray-100">{ev.venue}</td>
                                    <td className="px-3 py-3 align-top border-b border-r border-gray-100">{ev.date2}</td>
                                    <td className="px-3 py-3 align-top border-b border-r border-gray-100"><EventNoteCell event={ev} onSaved={() => fetchEventsForOrg(org.id)} /></td>
                                    <td className="px-3 py-3 align-top border-b border-gray-100">
                                      <div className="flex items-center gap-2">
                                        <span className="inline-flex items-center gap-1 rounded-full border border-gray-200 px-2 py-0.5 text-xs text-gray-800">{eventContactsCount[ev.id] ?? 0}</span>
                                        <button onClick={() => toggleEvent(ev.id)} className="inline-flex items-center rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white shadow-sm hover:bg-blue-700 transition" title="View Contacts">{openEvent === ev.id ? 'Hide Contacts' : 'View Contacts'}</button>
                                      </div>
                                    </td>
                                  </tr>

                                  {openEvent === ev.id && (
                                    <tr className="bg-blue-50">
                                      <td colSpan={10} className="p-0">
                                        <ContactsTable eventId={ev.id} onRefresh={() => fetchEventsForOrg(org.id)} globalSearch={''} onSingleCampaign={undefined} onUpload={undefined} />
                                      </td>
                                    </tr>
                                  )}
                                </React.Fragment>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    </td>
                  </tr>
                )}
              </React.Fragment>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
