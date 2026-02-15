import React, { useEffect, useState } from "react";
import { toast } from "react-toastify";
import { Plus, Download, ListTodo, Trash2, Users, Edit } from "lucide-react";
import { Link, useNavigate } from "react-router-dom";
import * as XLSX from "xlsx";
import "react-toastify/dist/ReactToastify.css";

import { SearchBar } from "./SearchBar";
import { CustomerForm } from "./CustomerForm";
import { ExcelUpload } from "./ExcelUpload";
import { MatchResults } from "./MatchResults";
import { ContactsTable } from "./ContactsTable";
import { Event, CampaignContact } from "../types";
import { useAuth } from "../contexts/AuthContext";
import { usePreviewResults } from '../contexts/PreviewResultsContext';
import {
  getAllEvents,
  getContactsForEvent,
  addContact,
  updateContact,
  deleteContact,
  pauseCampaignContact,
  resumeCampaignContact,
  uploadContactsExcel,
  startCampaign,
  handleExcelUpload as handleExcelUploadApi,
} from "../db";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { CreateEventModal } from './CreateEventModal';
import SingleEmailValidation from './SingleEmailValidation';
import GlobalUploadResultsModal from './GlobalUploadResultsModal';
import { LoadingOverlay } from './LoadingOverlay';

const API_URL = import.meta.env.VITE_API_URL || 'https://conferencecare.org/api';

// ------------ Small inline component for event name ------------
function EventNameCell({ name }: { name: string }) {
  const [expanded, setExpanded] = useState(false);

  if (!name) return null;

  const copyToClipboard = () => {
    navigator.clipboard.writeText(name);
    toast.info("Full event name copied!");
  };

  if (name.length <= 40) {
    return <span>{name}</span>;
  }

  return (
    <div className="flex flex-col max-w-xs">
      <span className="break-words">
        {expanded ? name : `${name.slice(0, 40)}...`}
      </span>
      <div className="flex gap-2 mt-1">
        <button
          onClick={() => setExpanded(!expanded)}
          className="text-blue-600 text-xs hover:underline"
        >
          {expanded ? "Show Less" : "Read More"}
        </button>
        <button onClick={copyToClipboard} className="text-green-600 text-xs hover:underline">
          Copy
        </button>
      </div>
    </div>
  );
}

// ------------ Small inline component for event note  ------------
function EventNoteCell({ event, onSaved }: { event: Event & { note?: string, attachment_url?: string }; onSaved?: () => void }) {
  const { token } = useAuth();
  const [editing, setEditing] = useState<boolean>(false);
  const [value, setValue] = useState<string>(event.note || "");
  const [saving, setSaving] = useState<boolean>(false);
  const [file, setFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState<boolean>(false);
  const [viewerOpen, setViewerOpen] = useState<boolean>(false);
  const [attachmentUrl, setAttachmentUrl] = useState<string | null>(event.attachment_url || null);

  const save = async () => {
    if (!token) return;
    setSaving(true);
    try {
      const API_BASE = import.meta.env.VITE_API_URL || 'https://www.conferencecare.org/api';
      const res = await fetch(`${API_BASE}/events/${event.id}`, {
        method: 'PATCH',
        headers: {
          'Content-Type': 'application/json',
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({ note: value }),
      });
      if (!res.ok) throw new Error('Save failed');
      toast.success('Note saved');
      setEditing(false);
      if (onSaved) onSaved();
    } catch (e) {
      console.error('Failed to save note', e);
      toast.error('Failed to save note');
    } finally {
      setSaving(false);
    }
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const f = e.target.files && e.target.files[0];
    setFile(f || null);
  };

  const uploadAttachment = async () => {
    if (!token || !file) return;
    setUploading(true);
    try {
      const API_BASE = import.meta.env.VITE_API_URL || 'https://www.conferencecare.org/api';
      const fd = new FormData();
      fd.append('file', file as Blob, file.name);
      const res = await fetch(`${API_BASE}/events/${event.id}/attachment`, {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${token}`,
        },
        body: fd,
      });
      if (!res.ok) throw new Error('Upload failed');
      const data = await res.json();
      setAttachmentUrl(data.attachment_url);
      toast.success('Attachment uploaded');
      setFile(null);
      if (onSaved) onSaved();
    } catch (e) {
      console.error('Failed to upload attachment', e);
      toast.error('Failed to upload attachment');
    } finally {
      setUploading(false);
    }
  };

  const openViewer = () => {
    if (!attachmentUrl) return;
    setViewerOpen(true);
  };

  return (
    <div>
      <div className="flex flex-col gap-1">
        <div className="text-sm text-gray-800">
          <div className="truncate max-w-xs">{event.note || '-'}</div>
          <div className="flex gap-2 mt-1 items-center">
            <button
              onClick={() => setEditing(true)}
              className="text-xs text-blue-600 hover:underline"
            >
              Edit
            </button>
            {attachmentUrl ? (
              <button onClick={openViewer} className="text-xs text-green-600 hover:underline">View Attachment</button>
            ) : null}
          </div>
        </div>

        {editing && (
          <div className="flex flex-col gap-2 items-start">
            <div className="flex gap-2 items-center">
              <input
                value={value}
                onChange={(e: React.ChangeEvent<HTMLInputElement>) => setValue(e.target.value)}
                className="border px-2 py-1 rounded w-56 text-sm"
                placeholder="Add a note"
              />
              <button
                onClick={save}
                disabled={saving}
                className="px-2 py-1 bg-green-600 text-white rounded text-xs"
              >
                Save
              </button>
              <button
                onClick={() => { setEditing(false); setValue(event.note || ''); }}
                className="px-2 py-1 bg-gray-200 text-xs rounded"
              >
                Cancel
              </button>
            </div>

            <div className="flex gap-2 items-center">
              <input type="file" onChange={handleFileChange} className="text-xs" />
              <button onClick={uploadAttachment} disabled={uploading || !file} className="px-2 py-1 bg-blue-600 text-white rounded text-xs">
                {uploading ? 'Uploading...' : 'Upload Attachment'}
              </button>
              {attachmentUrl && (<a className="text-xs text-gray-600" href={`${import.meta.env.VITE_API_URL || ''}${attachmentUrl}`} target="_blank" rel="noreferrer">Open in new tab</a>)}
            </div>
          </div>
        )}
      </div>

      {/* Viewer modal */}
      {viewerOpen && attachmentUrl && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black bg-opacity-50">
          <div className="bg-white rounded p-4 max-w-4xl w-full max-h-[90vh] overflow-auto">
            <div className="flex justify-between items-center mb-2">
              <div className="font-semibold">Attachment Preview</div>
              <button onClick={() => setViewerOpen(false)} className="text-sm px-2 py-1 bg-gray-200 rounded">Close</button>
            </div>
            <div className="w-full h-[70vh]">
              {/* Use iframe for PDFs and img for images */}
              {attachmentUrl.endsWith('.pdf') ? (
                <iframe src={`${import.meta.env.VITE_API_URL || ''}${attachmentUrl}`} className="w-full h-full" title="attachment-pdf" />
              ) : (
                <img src={`${import.meta.env.VITE_API_URL || ''}${attachmentUrl}`} alt="attachment" className="max-w-full max-h-full object-contain" />
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ===================== MainApp =====================
export function MainApp() {
  const { user, token, logout } = useAuth();
  const navigate = useNavigate();
  const previewCtx = usePreviewResults();  // Get context at component level

  const [events, setEvents] = useState<Event[]>([]);
  const [selectedEvent, setSelectedEvent] = useState<Event | null>(null);

  // ---- Global search + counts + per-event contacts (for cross-entity filtering)
  const [globalSearch, setGlobalSearch] = useState("");
  const [searchColumns, setSearchColumns] = useState<string[]>(["all"]);
  const [contactsCount, setContactsCount] = useState<{ [eventId: number]: number }>({});
  const [eventContacts, setEventContacts] = useState<{ [eventId: number]: CampaignContact[] }>({});

  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [displayError, setDisplayError] = useState<string | null>(null);
  // Debug snapshots to help diagnose missing org_name
  // debug state removed

  // Loading overlay states for async operations
  const [isSearchLoading, setIsSearchLoading] = useState(false);
  const [isContactActionLoading, setIsContactActionLoading] = useState(false);
  const [loadingMessage, setLoadingMessage] = useState("Loading...");

  // Form states
  const [showForm, setShowForm] = useState(false);
  const [editingCustomer, setEditingCustomer] = useState<Partial<CampaignContact> | null>(null);

  // Matches + Excel states
  const [matches, setMatches] = useState<CampaignContact[]>([]);
  const [showMatches, setShowMatches] = useState(false);
  const [pendingExcelData, setPendingExcelData] = useState<Partial<CampaignContact>[]>([]);
  const [isProcessing, setIsProcessing] = useState(false);

  // Notification / reply states
  const [notification, setNotification] = useState<any>(null);
  const [showReplyInput, setShowReplyInput] = useState(false);
  const [replyText, setReplyText] = useState("");
  const [isReplying, setIsReplying] = useState(false);
  const audioRef = React.useRef<HTMLAudioElement>(null);

  // Campaign (bulk)
  const [showCampaignModal, setShowCampaignModal] = useState(false);
  const [campaignContactIds, setCampaignContactIds] = useState<number[]>([]);
  const [campaignSubject, setCampaignSubject] = useState("");
  const [campaignMessage, setCampaignMessage] = useState("");
  const [isCampaignLoading, setIsCampaignLoading] = useState(false);

  // Single email validation modal
  const [showSingleEmailValidationModal, setShowSingleEmailValidationModal] = useState(false);

  //Edit Event Modal
  const [showEditEventModal, setShowEditEventModal] = useState(false);
  const [editingEvent, seteditingEvent] = useState<Event | null>(null);
  const [editingEventData, setEditingEventData] = useState({
    event_name: "",
    org_name: "",
    month1: "",
    month2: "",
    sender_email: "",
    city: "",
    venue: "",
    date2: "",
  });
  const [isEditingEvent, setIsEditingEvent] = useState(false);


  // Helper: extract a compact display form for event URLs (domain only if possible)
  const getDisplayUrl = (rawUrl?: string | null) => {
    if (!rawUrl) return '';
    try {
      let url = rawUrl.trim();
      // If scheme missing, add http:// to allow URL parsing
      if (!/^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(url)) url = 'http://' + url;
      const u = new URL(url);
      return u.hostname.replace(/^www\./, '');
    } catch (e) {
      // Fallback: truncate long urls to a short preview
      return rawUrl.length > 50 ? rawUrl.slice(0, 47) + '...' : rawUrl;
    }
  };

  // Default templates (fetched at runtime)
  const [defaultSubject, setDefaultSubject] = useState("");
  const [defaultMessage, setDefaultMessage] = useState("");

  // Add Event modal
  const [showAddEvent, setShowAddEvent] = useState(false);
  const [newEvent, setNewEvent] = useState({
    event_name: "",
    org_name: "",
    month: "",
    sender_email: "",
    city: "",
    venue: "",
    date2: "",
  });
  const [addEventError, setAddEventError] = useState("");
  const [isAddingEvent, setIsAddingEvent] = useState(false);

  // Per-customer campaign modal
  const [showSingleCampaignModal, setShowSingleCampaignModal] = useState(false);
  const [singleCampaignContact, setSingleCampaignContact] = useState<CampaignContact | null>(null);
  const [singleSubject, setSingleSubject] = useState("");
  const [singleBody, setSingleBody] = useState("");
  const [singleFormsLink, setSingleFormsLink] = useState("");
  const [singlePaymentLink, setSinglePaymentLink] = useState("");
  const [singleAttachment, setSingleAttachment] = useState<File | null>(null);
  const [singleLoading, setSingleLoading] = useState(false);

  // Attachments (bulk)
  const [attachmentFile, setAttachmentFile] = useState<File | null>(null);
  const [attachmentUploading, setAttachmentUploading] = useState(false);
  const [attachmentError, setAttachmentError] = useState<string | null>(null);

  // --------- Load default templates ---------
  useEffect(() => {
    fetch("/templates/emails/campaign_default_subject.txt")
      .then((res) => res.text())
      .then(setDefaultSubject)
      .catch(() => setDefaultSubject("Accommodation in {{city}} - {{month}} - {{venue}}"));

    fetch("/templates/emails/campaign_default_body.txt")
      .then((res) => res.text())
      .then(setDefaultMessage)
      .catch(() => setDefaultMessage("Dear {{name}},\nWe are pleased to invite you to {{event_name}} in {{city}}..."));
  }, []);

  // --------- Load events + contacts ---------
  const loadEvents = async () => {
    if (!token) return;

    try {
      setError(null);
      setDisplayError(null);
      const data = await getAllEvents();

      // Fetch organizations to map org_id -> org_name for events that lack org_name
      let orgMap: { [id: number]: string } = {};
      try {
        const API_BASE = import.meta.env.VITE_API_URL || 'https://www.conferencecare.org/api';
        const orgRes = await fetch(`${API_BASE}/organizations/`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        if (orgRes.ok) {
          const orgs = await orgRes.json();
          (orgs || []).forEach((o: any) => { orgMap[o.id] = o.name; });
        }
      } catch (e) {
        // ignore org fetch errors; we can still show events without names
      }

      // Ensure each event has org_name populated when possible.
      // Support both shapes:
      // - events that include `org_id`
      // - events where `org_name` contains the org id (numeric) instead of the name
      for (const ev of data) {
        const a: any = ev;
        // prefer explicit org_id property
        let orgId: number | undefined = a.org_id;

        // if org_id missing, check if org_name is actually an id (number or numeric string)
        if (!orgId && typeof a.org_name !== 'undefined' && a.org_name !== null) {
          if (typeof a.org_name === 'number') {
            orgId = a.org_name;
          } else if (typeof a.org_name === 'string' && /^\d+$/.test(a.org_name.trim())) {
            orgId = parseInt(a.org_name.trim(), 10);
          }
        }

        if (orgId) {
          // If we have a mapping, use it. Otherwise, if org_name was a numeric id string,
          // clear it to avoid showing an id instead of a human name.
          const mapped = orgMap[orgId];
          if (mapped) {
            a.org_name = mapped;
          } else if (typeof a.org_name === 'string' && /^\d+$/.test(a.org_name.trim())) {
            a.org_name = '';
          }
        } else {
          // keep any existing non-empty org_name (assumed to be a proper name); ensure defined
          if (!a.org_name) a.org_name = '';
        }
      }
  setEvents(data);

  // Add debugging logs to verify orgMap and events
    console.log('Fetched organizations map:', orgMap);
    console.log('Events returned from getAllEvents():', data);

      const counts: { [eventId: number]: number } = {};
      const contactsMap: { [eventId: number]: CampaignContact[] } = {};

      await Promise.all(
        data.map(async (event: Event) => {
          try {
            const contactsForEvent = await getContactsForEvent(event.id);
            counts[event.id] = contactsForEvent.length;
            contactsMap[event.id] = contactsForEvent;
          } catch {
            counts[event.id] = 0;
            contactsMap[event.id] = [];
          }
        })
      );

      setContactsCount(counts);
      setEventContacts(contactsMap);
    } catch (err: any) {
      const errorMsg = err?.message || 'Unknown error occurred';
      setError(`Failed to load events: ${errorMsg}`);
      setDisplayError(`⚠️ Failed to load events: ${errorMsg}`);
      toast.error("Failed to load events");
      console.error("Error loading events:", err);
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    loadEvents();
  }, [token]);

  // --------- Poll for notifications ---------
  useEffect(() => {
    if (!token || !user) return;
    if (user.username === "admin") {
      setNotification(null);
      return;
    }
    let interval: any;
    const fetchNotifications = async () => {
      try {
        const response = await fetch(`${API_URL}/notifications?unread_only=true`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        const data = await response.json();
        if (data.notifications && data.notifications.length > 0) {
          const userNotification = data.notifications.find((n: any) => n.user_id === user.id);
          if (userNotification) {
            setNotification(userNotification);
          } else {
            setNotification(null);
          }
        } else {
          setNotification(null);
        }
      } catch {
        // ignore fetch errors silently
      }
    };
    fetchNotifications();
    interval = setInterval(fetchNotifications, 30000);
    return () => clearInterval(interval);
  }, [token, user]);

  // --------- Play sound when notification appears ---------
  useEffect(() => {
    if (notification && audioRef.current) {
      audioRef.current.currentTime = 0;
      audioRef.current.play();
    }
  }, [notification]);

  // --------- Search handler (SearchBar passes (query, columns)) ---------
  // Uses backend search endpoint for filtering
  const handleSearch = (query: string, columns: string[]): void => {
    // Normalize query: trim leading/trailing whitespace and collapse multiple spaces
    // into a single space. This prevents trailing spaces from breaking matches.
    const normalized = (query || '').toString().trim().replace(/\s+/g, ' ');
    setGlobalSearch(normalized);
    setSearchColumns(columns);
    
    // Use backend search for filtering
    if (!token) return;
    
    setIsSearchLoading(true);
    setLoadingMessage("Searching...");
    setDisplayError(null);
    (async () => {
      try {
        const response = await fetch(
          `${API_URL}/search/advanced?query=${encodeURIComponent(normalized)}&columns=${columns.join(',')}`,
          {
            method: 'POST',
            headers: { Authorization: `Bearer ${token}` }
          }
        );
        
        if (!response.ok) {
          const errorData = await response.json().catch(() => ({}));
          throw new Error(errorData.detail || `Search failed: ${response.status}`);
        }
        
        const data = await response.json();
        setEvents(data.events);
        setContactsCount(
          data.events.reduce((acc: any, event: any) => {
            acc[event.id] = (event.contacts || []).length;
            return acc;
          }, {})
        );
        setEventContacts(
          data.events.reduce((acc: any, event: any) => {
            acc[event.id] = event.contacts || [];
            return acc;
          }, {})
        );
      } catch (error: any) {
        const errorMsg = error?.message || 'Search failed';
        console.error('Search error:', error);
        setDisplayError(`⚠️ ${errorMsg}`);
        toast.error(errorMsg);
      } finally {
        setIsSearchLoading(false);
      }
    })();
  };

  // --------- Add/Update/Delete contacts (unchanged behavior) ---------
  const handleAddCustomer = async (customer: Partial<CampaignContact>) => {
    setIsContactActionLoading(true);
    setLoadingMessage("Adding contact...");
    setDisplayError(null);
    try {
      // Normalize email and cc_store
      const emails = (customer.email || '').split(',').map(s => s.trim()).filter(Boolean);
      const payload: any = { ...customer };
      if (emails.length) {
        payload.email = emails[0];
        if (emails.length > 1) payload.cc_store = emails.slice(1).join(', ');
      }
      await addContact(payload);
      toast.success("Contact added successfully");
      setShowForm(false);
      loadEvents();
    } catch (err: any) {
      const errorMsg = err?.message || 'Failed to add contact';
      setDisplayError(`⚠️ Error: ${errorMsg}`);
      toast.error(errorMsg);
      console.error('Add contact error:', err);
    } finally {
      setIsContactActionLoading(false);
    }
  };

  const handleUpdateCustomer = async (customer: CampaignContact) => {
    setIsContactActionLoading(true);
    setLoadingMessage("Updating contact...");
    setDisplayError(null);
    try {
      const emails = (customer.email || '').split(',').map(s => s.trim()).filter(Boolean);
      const payload: any = { ...customer };
      if (emails.length) {
        payload.email = emails[0];
        if (emails.length > 1) payload.cc_store = emails.slice(1).join(', ');
      }
      await updateContact(payload);
      toast.success("Contact updated successfully");
      setShowForm(false);
      loadEvents();
    } catch (err: any) {
      const errorMsg = err?.message || 'Failed to update contact';
      setDisplayError(`⚠️ Error: ${errorMsg}`);
      toast.error(errorMsg);
      console.error('Update contact error:', err);
    } finally {
      setIsContactActionLoading(false);
    }
  };

  const handleDeleteCustomer = async (id: number): Promise<void> => {
    setIsContactActionLoading(true);
    setLoadingMessage("Deleting contact...");
    setDisplayError(null);
    try {
      if (window.confirm("Are you sure you want to delete this contact?")) {
        await deleteContact(id);
        toast.success("Contact deleted successfully");
        loadEvents();
      }
    } catch (err: any) {
      const errorMsg = err?.message || 'Failed to delete contact';
      setDisplayError(`⚠️ Error: ${errorMsg}`);
      toast.error(errorMsg);
      console.error('Delete contact error:', err);
    } finally {
      setIsContactActionLoading(false);
    }
  };

  const handleBatchDelete = async (customers: CampaignContact[]): Promise<void> => {
    setIsContactActionLoading(true);
    setLoadingMessage("Deleting contacts...");
    setDisplayError(null);
    try {
      for (const customer of customers) {
        await deleteContact(customer.id);
      }
      toast.success("Selected contacts deleted successfully");
      loadEvents();
    } catch (err: any) {
      const errorMsg = err?.message || 'Failed to delete contacts';
      setDisplayError(`⚠️ Error: ${errorMsg}`);
      toast.error(errorMsg);
      console.error('Batch delete error:', err);
    } finally {
      setIsContactActionLoading(false);
    }
  };

  // --------- Excel upload ---------
  // Excel upload - uses new optimized streaming endpoint in ContactsTable
  const handleExcelFileUpload = async (file: File): Promise<void> => {
    setIsProcessing(true);
    setDisplayError(null);
    try {
      // Use the context that's already available at component level
      const token = localStorage.getItem('token');
      if (!token) {
        throw new Error('Authentication required. Please log in.');
      }
      const formData = new FormData();
      formData.append('file', file);
      const apiUrl = (import.meta.env.VITE_API_URL || '') + `/campaign_contacts/upload-excel-job`;
      const resp = await fetch(apiUrl, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${token}`
        },
        body: formData
      });
      if (!resp.ok) {
        const errText = await resp.text();
        const errorData = await resp.json().catch(() => ({}));
        throw new Error(errorData.detail || `Upload failed: ${resp.status} ${errText}`);
      }
      const data = await resp.json();
      const jobId = data.job_id;

      console.log('[UPLOAD] Job created:', jobId);
      toast.info('Upload started. Validating rows...');

      // Open results modal with null (loading state)
      previewCtx.setOpen(true);
      previewCtx.setPreviewResults(null);

      // Poll results endpoint every 1s to stream in results as they complete
      const statusUrl = (import.meta.env.VITE_API_URL || '') + `/campaign_contacts/upload-job/${jobId}/status`;
      const resultsUrl = (import.meta.env.VITE_API_URL || '') + `/campaign_contacts/upload-job/${jobId}/results`;

      let done = false;
      let pollCount = 0;
      const maxPolls = 600; // 10 minutes timeout
      let lastResultsCount = 0;

      while (!done && pollCount < maxPolls) {
        await new Promise(r => setTimeout(r, 1000));
        pollCount++;

        try {
          // Fetch status with timeout
          const sresp = await fetch(statusUrl, {
            headers: { 'Authorization': `Bearer ${token}` },
            signal: AbortSignal.timeout(5000)
          });
          if (!sresp.ok) {
            console.warn(`[UPLOAD POLL ${pollCount}] Status fetch failed: ${sresp.status}`);
            continue;
          }
          const statusJson = await sresp.json();
          console.log(`[UPLOAD POLL ${pollCount}] Status:`, statusJson.status, 'Processed:', statusJson.processed_rows, 'Total:', statusJson.total_rows);

          // Fetch results to stream
          try {
            const rresp = await fetch(resultsUrl, {
              headers: { 'Authorization': `Bearer ${token}` },
              signal: AbortSignal.timeout(5000)
            });
            if (rresp.ok) {
              const resultsJson = await rresp.json();
              const newResults = resultsJson.results || [];
              console.log(`[UPLOAD POLL ${pollCount}] Results available:`, newResults.length, 'rows');

              if (newResults.length > 0) {
                console.log(`[UPLOAD] Updating modal with ${newResults.length} results`);
                // Create a new array reference to force React update
                previewCtx.setPreviewResults(newResults.map((r: any) => ({ ...r })));
                lastResultsCount = newResults.length;
              }
            } else {
              console.debug(`[UPLOAD POLL ${pollCount}] Results fetch returned ${rresp.status}`);
            }
          } catch (e) {
            console.debug(`[UPLOAD POLL ${pollCount}] Results fetch error:`, e);
          }

          // Check if job is complete
          if (statusJson.status === 'finished') {
            console.log('[UPLOAD] Job finished! Fetching final results...');
            try {
              const finalResp = await fetch(resultsUrl, {
                headers: { 'Authorization': `Bearer ${token}` },
                signal: AbortSignal.timeout(5000)
              });
              if (finalResp.ok) {
                const finalJson = await finalResp.json();
                const finalResults = finalJson.results || [];
                console.log('[UPLOAD] Final results count:', finalResults.length);
                previewCtx.setPreviewResults(finalResults.map((r: any) => ({ ...r })));
              }
            } catch (e) {
              console.error('[UPLOAD] Error fetching final results:', e);
            }

            toast.success('Validation complete! All results shown above.');
            done = true;
            loadEvents();
          } else if (statusJson.status === 'failed') {
            const error = statusJson.error || 'unknown error';
            console.error('[UPLOAD] Job failed:', error);
            setDisplayError(`⚠️ Upload validation failed: ${error}`);
            toast.error(`Validation job failed: ${error}`);
            done = true;
            loadEvents();
          } else if (statusJson.total_rows > 0 && statusJson.processed_rows >= statusJson.total_rows && lastResultsCount > 0) {
            // All rows processed and we have results - job is effectively done
            console.log('[UPLOAD] All rows processed (processed >= total). Marking as done.');
            toast.success('Validation complete! All results shown above.');
            done = true;
            loadEvents();
          }
        } catch (e) {
          if ((e as Error).name === 'AbortError') {
            console.warn(`[UPLOAD POLL ${pollCount}] Request timeout`);
          } else {
            console.warn(`[UPLOAD POLL ${pollCount}] Polling error:`, e);
          }
        }
      }

      if (pollCount >= maxPolls) {
        const timeoutMsg = 'Upload validation took too long. Please check the job status manually.';
        console.error('[UPLOAD] Polling timeout - job took too long');
        setDisplayError(`⚠️ ${timeoutMsg}`);
        toast.error(timeoutMsg);
      }

    } catch (err: any) {
      const errorMsg = err.message || err.toString();
      console.error('[UPLOAD ERROR]', errorMsg, err);
      setDisplayError(`⚠️ Upload Error: ${errorMsg}`);
      toast.error("Failed to save contacts: " + errorMsg);
      loadEvents();
    } finally {
      setIsProcessing(false);
    }
  };

  const saveExcelData = async (data: Partial<CampaignContact>[]) => {
    try {
      // Normalize each row to split email and cc_store
      const normalized = data.map(row => {
        const emails = (row.email || '').toString().split(',').map((s:any) => s.trim()).filter(Boolean);
        const out: any = { ...row };
        if (emails.length) {
          out.email = emails[0];
          if (emails.length > 1) out.cc_store = emails.slice(1).join(', ');
        }
        return out;
      });
  await handleExcelUploadApi(normalized);
      toast.success("All contacts added successfully");
      loadEvents();
      setPendingExcelData([]);
    } catch (err: any) {
      if (err.message && err.message.includes("Missing required")) {
        setError("Some rows were skipped: " + err.message);
        toast.error("Some rows were skipped: " + err.message);
      } else {
        setError("Failed to save contacts: " + (err.message || err));
        toast.error("Failed to save contacts");
      }
    }
  };

  const handleCloseMatches = async () => {
    setShowMatches(false);
    setMatches([]);
    if (pendingExcelData.length > 0) {
      await saveExcelData(pendingExcelData);
    }
  };

  // --------- Export to Excel ---------
  const handleExportToExcel = () => {
    const worksheet = XLSX.utils.json_to_sheet(events);
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, worksheet, "Events");
    XLSX.writeFile(workbook, "events.xlsx");
  };

  // --------- Notification interactions ---------
  const handleThankYou = async () => {
    if (!notification) return;
    try {
      await fetch(`${API_URL}/notifications/${notification.id}/read`, {
        method: "PUT",
        headers: { Authorization: `Bearer ${token}` },
      });
      setNotification(null);
      setShowReplyInput(false);
      setReplyText("");
    } catch {
      toast.error("Failed to mark notification as read");
    }
  };

  const handleReply = async () => {
    if (!notification || !replyText.trim()) return;
    setIsReplying(true);
    try {
      await fetch(`${API_URL}/notifications/${notification.id}/reply`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ reply_text: replyText }),
      });
      setNotification(null);
      setShowReplyInput(false);
      setReplyText("");
      toast.success("Reply sent");
    } catch {
      toast.error("Failed to send reply");
    } finally {
      setIsReplying(false);
    }
  };

  // --------- Per-contact campaign ----------
  const handleSingleCampaign = (contact: CampaignContact) => {
    setSingleCampaignContact(contact);
    setSingleSubject(defaultSubject);
    setSingleBody(defaultMessage);
    setSingleFormsLink(contact.form_link || "");
    setSinglePaymentLink(contact.payment_link || "");
    setSingleAttachment(null);
    setShowSingleCampaignModal(true);
  };

  const handleSingleCampaignSubmit = async () => {
    if (!singleCampaignContact) return;
    setSingleLoading(true);
    try {
      const formData = new FormData();
      formData.append("subject", singleSubject);
      formData.append("body", singleBody);
      formData.append("forms_link", singleFormsLink);
      formData.append("payment_link", singlePaymentLink);
      if (singleAttachment) formData.append("attachment", singleAttachment);
      await fetch(
        `${API_URL}/email_queue/${singleCampaignContact.id}/start-campaign`,
        {
          method: "POST",
          headers: { Authorization: `Bearer ${token}` },
          body: formData,
        }
      );
      toast.success("Campaign started for this contact!");
      setShowSingleCampaignModal(false);
    } catch {
      toast.error("Failed to start campaign for this contact");
    } finally {
      setSingleLoading(false);
    }
  };

  // --------- Bulk campaign ----------
  const handleStartCampaign = (contactIds: number[]) => {
    setCampaignContactIds(contactIds);
    setCampaignSubject(defaultSubject);
    setCampaignMessage(defaultMessage);
    setAttachmentFile(null);
    setShowCampaignModal(true);
  };

  const handleCampaignSubmit = async () => {
    if (campaignContactIds.length === 0) {
      toast.error("Please select contacts to start campaign");
      return;
    }
    setIsCampaignLoading(true);
    try {
      await fetch(`${API_URL}/email_queue/bulk-start-campaign`, {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(campaignContactIds),
      });
      toast.success("Campaign started for selected contacts!");
      setShowCampaignModal(false);
    } catch {
      toast.error("Failed to start campaign for selected contacts");
    } finally {
      setIsCampaignLoading(false);
    }
  };

  // --------- Add Event ----------
  const handleAddEvent = async () => {
    setAddEventError("");
    setDisplayError(null);
    if (!newEvent.event_name.trim()) {
      setAddEventError("Event Name is required");
      setDisplayError("⚠️ Event Name is required");
      return;
    }
    setIsAddingEvent(true);
    try {
      const API_URL = import.meta.env.VITE_API_URL || 'https://conferencecare.org/api';
      const response = await fetch(`${API_URL}/events`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify(newEvent),
      });
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || "Failed to create event");
      }
      setShowAddEvent(false);
      setNewEvent({
        event_name: "",
        org_name: "",
        month: "",
        sender_email: "",
        city: "",
        venue: "",
        date2: "",
      });
      loadEvents();
    } catch (err: any) {
      const errorMsg = err?.message || 'Failed to create event';
      setAddEventError(errorMsg);
      setDisplayError(`⚠️ Error: ${errorMsg}`);
      console.error('Add event error:', err);
    } finally {
      setIsAddingEvent(false);
    }
  };

  // --------- Delete Event ----------
  const handleDeleteEvent = async (eventId: number) => {
    setDisplayError(null);
    if (!window.confirm("Are you sure you want to delete this event and all its contacts? This action cannot be undone."))
      return;
    try {
      const res = await fetch(`${API_URL}/events/${eventId}`, {
        method: "DELETE",
        headers: { Authorization: `Bearer ${token}` },
      });
      if (!res.ok) {
        const errorData = await res.json().catch(() => ({}));
        throw new Error(errorData.detail || "Failed to delete event");
      }
      toast.success("Event and all contacts deleted!");
      loadEvents();
    } catch (err: any) {
      const errorMsg = err?.message || 'Failed to delete event';
      setDisplayError(`⚠️ Error: ${errorMsg}`);
      toast.error(errorMsg);
      console.error('Delete event error:', err);
    }
  };
// ===================== Edit Event =====================
  const handleEditEvent = (event: Event) => {
    seteditingEvent(event);
    
    // Parse month string into month1 and month2
    let m1 = "", m2 = "";
    if (event.month) {
      const months = event.month.split(',').map(m => m.trim());
      m1 = months[0] || "";
      m2 = months[1] || "";
    }
    
    setEditingEventData({
      event_name: event.event_name || "",
      org_name: event.org_name || "",
      month1: m1,
      month2: m2,
      sender_email: event.sender_email || "",
      city: event.city || "",
      venue: event.venue || "",
      date2: event.date2 || "",
    });
    setShowEditEventModal(true);
  };

  const handleSaveEditedEvent = async () => {
    if (!editingEvent) return;
    setIsEditingEvent(true);
    setDisplayError(null);
    try {
      const response = await fetch(`${API_URL}/events/${editingEvent.id}`, {
        method: "PATCH", 
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({
          ...editingEventData,
          month: editingEventData.month1 && editingEventData.month2 ? `${editingEventData.month1}, ${editingEventData.month2}` : editingEventData.month1 || editingEventData.month2,
        }),
      });
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        throw new Error(errorData.detail || "Failed to update event");
      }
      setShowEditEventModal(false);
      seteditingEvent(null);
      loadEvents();
    } catch (err: any) {
      const errorMsg = err?.message || 'Failed to update event';
      setDisplayError(`⚠️ Error: ${errorMsg}`);
      toast.error(errorMsg);
      console.error('Edit event error:', err);
    } finally {
      setIsEditingEvent(false);
    }
  };
  // --------- FILTER (events + their contacts) ----------
  // We compute both filteredEvents and an eventMatchMap indicating when the
  // search matched the event name (so ContactsTable can show all contacts).
  const searchNormalized = globalSearch.trim().toLowerCase();
  const eventMatchMap: { [id: number]: boolean } = {};
  // Tracks when the event name itself matched the search string
  const eventNameMatchMap: { [id: number]: boolean } = {};

  // Sort events by ID descending (newest/highest ID first)
  const sortedEvents = [...events].sort((a: Event, b: Event) => b.id - a.id);

  const filteredEvents = sortedEvents.filter((event: Event) => {
    // no search -> show all
    if (!searchNormalized) {
      eventMatchMap[event.id] = false;
      return true;
    }

    const contacts = eventContacts[event.id] || [];

    // If the user used '+' tokens, treat each token as a required term (AND).
    // A contact matches if for every token there exists at least one contact
    // field that contains that token. This is less brittle than positional
    // mapping and avoids false positives when tokens don't map exactly to
    // stage/status strings.
    if (searchNormalized.includes("+")) {
      const parts = searchNormalized.split("+")
        .map((p) => p.trim())
        .filter(Boolean);

      // If any part contains a colon, treat parts as key:value pairs (explicit)
      const hasKV = parts.some(p => p.includes(':'));
      if (hasKV) {
        // build map of key -> value
        const kv: { [k: string]: string } = {};
        parts.forEach(p => {
          const [k, ...rest] = p.split(':');
          if (!k) return;
          kv[k.trim()] = rest.join(':').trim();
        });

        const allMatch = contacts.some(c => {
          return Object.entries(kv).every(([k, v]) => {
            const val = ((c as any)[k] || '').toString().toLowerCase();
            return val.includes(v.toLowerCase());
          });
        });
        eventMatchMap[event.id] = allMatch;
        return allMatch;
      }

      // Positional mapping: strict match per column without fallback. This
      // reduces false positives ÃƒÆ’Ã‚Â¢ÃƒÂ¢Ã¢â‚¬Å¡Ã‚Â¬ÃƒÂ¢Ã¢â€šÂ¬ token0 -> stage, token1 -> status, token2 -> trigger.
      if (parts.length > 1) {
        const cols = ['stage', 'status', 'trigger'];
        const anyContactMatches = contacts.some(c => {
          return parts.every((rawPart, idx) => {
            const part = rawPart.toLowerCase();
            const col = cols[idx];
            if (!col) return false;
            const field = ((c as any)[col] || '').toString().toLowerCase();
            if (!field) return false;
            if (field === part) return true;
            // match whole words inside the field
            const tokens = field.split(/[^a-z0-9]+/).filter(Boolean);
            return tokens.includes(part);
          });
        });
        eventMatchMap[event.id] = anyContactMatches;
        return anyContactMatches;
      }

      // Single token with '+': prefer whole-word stage/status match then fallback to any-field contains
      const token = (parts[0] || '').toLowerCase();
      const anyContactMatches = contacts.some(c => {
        const stage = ((c as any).stage || '').toString().toLowerCase();
        const status = ((c as any).status || '').toString().toLowerCase();
        const wholeWordMatch = (field: string, t: string) => {
          if (!field) return false;
          if (field === t) return true;
          const tokens = field.split(/[^a-z0-9]+/).filter(Boolean);
          return tokens.includes(t);
        };
        if (wholeWordMatch(stage, token) || wholeWordMatch(status, token)) return true;
        const searchableFields = [
          'name', 'email', 'notes', 'organizer', 'hotel_name', 'supplier',
          'nationality', 'workplace', 'payment_method', 'trigger', 'form_link', 'payment_link', 'phone_number', 'booking_id'
        ];
        return searchableFields.some(f => ((c as any)[f] || '').toString().toLowerCase().includes(token));
      });
      eventMatchMap[event.id] = anyContactMatches;
      return anyContactMatches;
    }

    // If the search matches the event name, include the event and mark it so
    // the ContactsTable will receive an empty filter (show all contacts).
    if ((event.event_name || "").toLowerCase().includes(searchNormalized)) {
      eventMatchMap[event.id] = true;
      eventNameMatchMap[event.id] = true;
      return true;
    }

    // Fallback: full-text-ish search across event fields and contact text
    const contactList = contacts
      .map((c) =>
        [
          c.name,
          c.email,
          (c.stage as string) || "",
          (c.status as string) || "",
          (c.notes as string) || "",
          (c.organizer as string) || "",
          (c.hotel_name as string) || "",
          (c.supplier as string) || "",
          (c.nationality as string) || "",
          (c.workplace as string) || "",
          (c.payment_method as string) || "",
          (c.trigger as string) || "",
          (c.form_link as string) || "",
          (c.payment_link as string) || "",
          ((c as any).phone_number as string) || "",
          ((c as any).booking_id as string) || "",
        ].join(" ")
      )
      .join(" ")
      .toLowerCase();

    const eventFields = [
      event.id?.toString() ?? "",
      event.event_name,
      event.org_name,
      event.month,
      event.sender_email,
      event.city,
      event.venue,
      event.date2,
      (contactsCount[event.id] ?? "").toString(),
      contactList,
    ]
      .join(" ")
      .toLowerCase();

    eventMatchMap[event.id] = false;
    return eventFields.includes(searchNormalized);
  });

  // Create eventNumberMap based on FILTERED results only
  // This way, if only 2 events show after filtering, they're numbered 1 and 2
  // Numbers are reversed: first event gets highest number, last gets 1
  const eventNumberMap: { [id: number]: number } = {};
  filteredEvents.forEach((event: Event, index: number) => {
    eventNumberMap[event.id] = filteredEvents.length - index;
  });
  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="text-red-600">{error}</div>
      </div>
    );
  }

  // ===================== RENDER =====================
  return (
    <div className="min-h-screen bg-gray-100">
      {/* DEBUG PANEL removed */}
      {/* Error Banner */}
      {displayError && (
        <div className="fixed top-20 left-1/2 -translate-x-1/2 z-40 bg-red-50 border border-red-300 shadow-lg rounded-lg px-6 py-4 flex items-center gap-3 max-w-lg w-full">
          <div className="text-red-700 font-semibold flex-1">{displayError}</div>
          <button
            onClick={() => setDisplayError(null)}
            className="text-red-600 hover:text-red-800 font-bold"
          >
            ✕
          </button>
        </div>
      )}
      
      {/* Notification popup/banner */}
      <audio ref={audioRef} src="/notification.mp3" preload="auto" />
      {notification && (
        <div className="fixed top-4 left-1/2 -translate-x-1/2 z-50 bg-white border border-blue-400 shadow-lg rounded-lg px-6 py-4 flex flex-col items-center max-w-lg w-full">
          <div className="mb-2 text-blue-700 font-semibold">{notification.message}</div>
          <div className="flex gap-2">
            <button onClick={handleThankYou} className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700">
              Thank You
            </button>
            <button onClick={() => setShowReplyInput((v) => !v)} className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700">
              Any more options
            </button>
          </div>
          {showReplyInput && (
            <div className="mt-3 w-full flex flex-col items-center">
              <textarea
                value={replyText}
                onChange={(e) => setReplyText(e.target.value)}
                rows={2}
                className="w-full border border-gray-300 rounded p-2 mb-2"
                placeholder="Type your reply..."
              />
              <button
                onClick={handleReply}
                disabled={isReplying || !replyText.trim()}
                className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
              >
                {isReplying ? "Sending..." : "Send Reply"}
              </button>
            </div>
          )}
        </div>
      )}

      {/* Sticky header */}
      <div className="sticky top-0 z-20 bg-white shadow-md">
        <div className="max-w-7xl mx-auto px-4 py-2">
          <div className="flex justify-between items-center mb-2">
            <h1 className="text-2xl font-bold text-blue-700">CRM</h1>
            <div className="flex items-center space-x-3">
              <Link
                to="/tasks"
                className="flex items-center space-x-2 px-3 py-1.5 bg-green-600 text-white rounded-md hover:bg-green-700 text-sm"
              >
                <ListTodo size={18} />
                <span>My Tasks</span>
              </Link>
              <Link
                to="/queue"
                className="flex items-center space-x-2 px-3 py-1.5 bg-orange-600 text-white rounded-md hover:bg-orange-700 text-sm"
              >
                <ListTodo size={18} />
                <span>Queue</span>
              </Link>
              <>
                <Link to="/organizations" className="flex items-center space-x-2 px-3 py-1.5 bg-indigo-600 text-white rounded-md hover:bg-indigo-700 text-sm">
                  <Users size={18} />
                  <span>Analysis</span>
                </Link>
                {user?.is_admin && (
                  <Link to="/admin" className="px-3 py-1.5 bg-purple-600 text-white rounded-md hover:bg-purple-700 text-sm">
                    Admin Dashboard
                  </Link>
                )}
              </>
              <button onClick={logout} className="px-3 py-1.5 bg-gray-600 text-white rounded-md hover:bg-gray-700 text-sm">
                Logout
              </button>
            </div>
          </div>

          <div className="flex justify-between items-center space-x-3">
            {/* IMPORTANT: SearchBar uses the two-arg signature */}
            <SearchBar value={globalSearch} onChange={handleSearch} />
            <div className="flex items-center space-x-2">
              <button
                onClick={handleExportToExcel}
                className="flex items-center space-x-2 px-3 py-1.5 bg-blue-600 text-white rounded-md hover:bg-blue-700 text-sm"
              >
                <Download size={18} />
                <span>Export</span>
              </button>
              <button
                onClick={() => { setShowAddEvent(true); toast.info('Opening Create Event modal...'); }}
                className="flex items-center space-x-2 px-3 py-1.5 bg-green-600 text-white rounded-md hover:bg-green-700 text-sm"
              >
                <Plus size={18} />
                <span>Add Event</span>
              </button>
            </div>
          </div>
        </div>
      </div>

      {/* Events Table */}
      <div className="w-full py-4 sm:py-8">
        <div className="overflow-x-auto">
          <table className="min-w-[1100px] w-full border-collapse bg-white shadow rounded-xl overflow-hidden">
            <thead className="bg-gray-50">
              <tr className="text-left text-sm font-semibold text-gray-900">
                <th className="px-3 py-3 w-[56px] border-b border-r border-gray-200">NO.</th>
                <th className="px-3 py-3 w-[56px] border-b border-r border-gray-200"></th>
                <th className="px-3 py-3 border-b border-r border-gray-200">ID</th>
                <th className="px-3 py-3 border-b border-r border-gray-200">Event Name</th>
                <th className="px-3 py-3 border-b border-r border-gray-200">Org Name</th>
                <th className="px-3 py-3 border-b border-r border-gray-200">Month</th>
                <th className="px-3 py-3 border-b border-r border-gray-200">Sender Email</th>
                <th className="px-3 py-3 border-b border-r border-gray-200">City</th>
                <th className="px-3 py-3 border-b border-r border-gray-200">Venue</th>
                <th className="px-3 py-3 border-b border-r border-gray-200">Date2</th>
                <th className="px-3 py-3 border-b border-r border-gray-200">Note</th>
                <th className="px-3 py-3 border-b border-gray-200">Contacts</th>
              </tr>
            </thead>
            <tbody className="text-sm text-gray-800">
              {filteredEvents.map((event: Event) => (
                <React.Fragment key={event.id}>
                  <tr className="odd:bg-white even:bg-gray-50 hover:bg-blue-50/60 transition-colors">
                    {/* Event Number */}
                    <td className="px-3 py-3 align-top border-b border-r border-gray-100">
                      <div className="flex items-center justify-center w-8 h-8 rounded-full bg-gray-300" style={{ backgroundColor: '#E0E0E0' }}>
                        <span className="text-sm font-bold text-gray-900">{eventNumberMap[event.id]}</span>
                      </div>
                    </td>
                    {/* Delete */}
                    <td className="px-3 py-3 align-top border-b border-r border-gray-100">
                      <div className="flex gap-2">
                      <button
                        onClick={() => handleDeleteEvent(event.id)}
                        className="text-red-600 hover:text-red-800 transition"
                        title="Delete Event"
                      >
                        <Trash2 size={14} />
                      </button>
                      <button
                        onClick={() => handleEditEvent(event)}
                        className="ml-3 text-blue-600 hover:text-blue-800 transition"
                        title="Edit Event"
                      >
                        <Edit size={14} />
                      </button> 
                      </div>
                    </td>


                    <td className="px-3 py-3 align-top border-b border-r border-gray-100">{event.id}</td>

                    <td className="px-3 py-3 align-top border-b border-r border-gray-100">
                      <EventNameCell name={event.event_name} />
                      <div className="mt-1 text-xs text-gray-500">
                        {event.event_url ? (
                          <a href={event.event_url} target="_blank" rel="noreferrer" className="text-blue-600 underline">
                            {getDisplayUrl(event.event_url)}
                          </a>
                        ) : (
                          (event.org_name || (event.org_id ? `Org #${event.org_id}` : 'Unassigned'))
                        )}
                      </div>
                    </td>

                    <td className="px-3 py-3 align-top border-b border-r border-gray-100">{event.org_name || (event.org_id ? `Org #${event.org_id}` : 'Unassigned')}</td>
                    <td className="px-3 py-3 align-top border-b border-r border-gray-100">{event.month}</td>

                    <td className="px-3 py-3 align-top border-b border-r border-gray-100">
                      <a href={`mailto:${event.sender_email}`} className="underline-offset-2 hover:underline">
                        {event.sender_email}
                      </a>
                    </td>

                    <td className="px-3 py-3 align-top border-b border-r border-gray-100">{event.city}</td>
                    <td className="px-3 py-3 align-top border-b border-gray-100">{event.venue}</td>
                    <td className="px-3 py-3 align-top border-b border-r border-gray-100">{event.date2}</td>

                    <td className="px-3 py-3 align-top border-b border-r border-gray-100">
                      <EventNoteCell event={event} onSaved={() => loadEvents()} />
                    </td>

                    <td className="px-3 py-3 align-top border-b border-gray-100">
                      <div className="flex items-center gap-2">
                        <span className="inline-flex items-center gap-1 rounded-full border border-gray-200 px-2 py-0.5 text-xs text-gray-800">
                          <Users className="h-3.5 w-3.5" />
                          {contactsCount[event.id] ?? 0}
                        </span>
                        <button
                          onClick={() =>
                            setSelectedEvent(selectedEvent?.id === event.id ? null : event)
                          }
                          className="inline-flex items-center rounded-lg bg-blue-600 px-3 py-1.5 text-xs font-semibold text-white shadow-sm hover:bg-blue-700 transition"
                          title="View Contacts"
                        >
                          View Contacts
                        </button>
                      </div>
                    </td>
                  </tr>

                  {/* Expanded contacts row */}
                  {selectedEvent?.id === event.id && (
                    <tr className="bg-blue-50">
                      <td colSpan={10} className="p-0">
                        <ContactsTable
                          eventId={event.id}
                          onRefresh={loadEvents}
                          // If the event name matched and the search is NOT a '+' multi-token
                          // query, clear the ContactsTable filter so all contacts show. For
                          // '+' stage+status searches we must keep the filter so only
                          // matching contacts are visible.
                          globalSearch={(eventNameMatchMap[event.id] && !searchNormalized.includes('+')) ? '' : globalSearch}
                          onSingleCampaign={handleSingleCampaign}
                          onUpload={handleExcelFileUpload}
                        />
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))}
            </tbody>
          </table>
          
          {/* No Results Message */}
          {filteredEvents.length === 0 && (
            <div className="w-full py-12 text-center">
              <div className="text-gray-500 text-lg font-medium">
                {globalSearch ? "No results found" : "No events available"}
              </div>
              {globalSearch && (
                <p className="text-gray-400 text-sm mt-2">
                  Try adjusting your search criteria
                </p>
              )}
            </div>
          )}
        </div>
      </div>

      {/* Add / Edit Contact Form */}
      {showForm && (
        <CustomerForm
          customer={editingCustomer || {}}
          onSubmit={editingCustomer ? handleUpdateCustomer : handleAddCustomer}
          onClose={() => {
            setShowForm(false);
            setEditingCustomer(null);
          }}
        />
      )}

      {/* Match Results */}
      {showMatches && <MatchResults matches={matches} onClose={handleCloseMatches} />}

      {/* Campaign Modal (bulk) */}
      <Dialog open={showCampaignModal} onOpenChange={setShowCampaignModal}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Start Campaign for Selected</DialogTitle>
          </DialogHeader>
          <div>
            <input
              type="text"
              value={campaignSubject}
              onChange={(e) => setCampaignSubject(e.target.value)}
              placeholder="Subject"
              className="w-full mb-2 border p-2 rounded"
            />
            <textarea
              value={campaignMessage}
              onChange={(e) => setCampaignMessage(e.target.value)}
              placeholder="Message"
              className="w-full mb-2 border p-2 rounded"
              rows={6}
            />
            <input
              type="file"
              accept=".pdf"
              onChange={(e) => setAttachmentFile(e.target.files?.[0] || null)}
              className="mb-2"
            />
            <Button onClick={handleCampaignSubmit} disabled={isCampaignLoading} className="w-full">
              {isCampaignLoading ? "Sending..." : "Send Campaign"}
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      {/* Per-Customer Campaign Modal */}
      <Dialog open={showSingleCampaignModal} onOpenChange={setShowSingleCampaignModal}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Start Campaign for {singleCampaignContact?.name}</DialogTitle>
          </DialogHeader>
          <div>
            <input
              type="text"
              value={singleSubject}
              onChange={(e) => setSingleSubject(e.target.value)}
              placeholder="Subject"
              className="w-full mb-2 border p-2 rounded"
            />
            <textarea
              value={singleBody}
              onChange={(e) => setSingleBody(e.target.value)}
              placeholder="Message"
              className="w-full mb-2 border p-2 rounded"
              rows={6}
            />
            <input
              type="text"
              value={singleFormsLink}
              onChange={(e) => setSingleFormsLink(e.target.value)}
              placeholder="Forms Link"
              className="w-full mb-2 border p-2 rounded"
            />
            <input
              type="text"
              value={singlePaymentLink}
              onChange={(e) => setSinglePaymentLink(e.target.value)}
              placeholder="Payment Link"
              className="w-full mb-2 border p-2 rounded"
            />
            <input
              type="file"
              accept=".pdf"
              onChange={(e) => setSingleAttachment(e.target.files?.[0] || null)}
              className="mb-2"
            />
            <Button onClick={handleSingleCampaignSubmit} disabled={singleLoading} className="w-full">
              {singleLoading ? "Sending..." : "Send Campaign"}
            </Button>
          </div>
        </DialogContent>
      </Dialog>

      {/* Add Event Modal (centralized component) */}
      <CreateEventModal
        open={showAddEvent}
        onClose={() => setShowAddEvent(false)}
        onCreated={() => {
          setShowAddEvent(false);
          loadEvents();
        }}
      />
      {/* Edit Event Modal */}
      <Dialog open={showEditEventModal} onOpenChange={setShowEditEventModal}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Edit Event</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            <dev>
              <label className="block text-sm font-medium text-gray-700">Event Name</label>
              <input
                type="text"
                value={editingEventData.event_name}
                onChange={(e) => setEditingEventData({ ...editingEventData, event_name: e.target.value })}
                className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm p-2"
              />
            </dev>
            <dev>
              <label className="block text-sm font-medium text-gray-700">Org Name</label>
              <input
                type="text"
                value={editingEventData.org_name}
                onChange={(e) => setEditingEventData({ ...editingEventData, org_name: e.target.value })}
                className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm p-2"
              />
            </dev>
            <dev>
              <label className="block text-sm font-medium text-gray-700">Month</label>
              <div className="grid grid-cols-2 gap-3">
                <select
                  value={editingEventData.month1}
                  onChange={(e) => setEditingEventData({ ...editingEventData, month1: e.target.value })}
                  className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm p-2"
                >
                  <option value="">Month 1</option>
                  <option value="January">January</option>
                  <option value="February">February</option>
                  <option value="March">March</option>
                  <option value="April">April</option>
                  <option value="May">May</option>
                  <option value="June">June</option>
                  <option value="July">July</option>
                  <option value="August">August</option>
                  <option value="September">September</option>
                  <option value="October">October</option>
                  <option value="November">November</option>
                  <option value="December">December</option>
                </select>
                <select
                  value={editingEventData.month2}
                  onChange={(e) => setEditingEventData({ ...editingEventData, month2: e.target.value })}
                  className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm p-2"
                >
                  <option value="">Month 2 (Optional)</option>
                  <option value="January">January</option>
                  <option value="February">February</option>
                  <option value="March">March</option>
                  <option value="April">April</option>
                  <option value="May">May</option>
                  <option value="June">June</option>
                  <option value="July">July</option>
                  <option value="August">August</option>
                  <option value="September">September</option>
                  <option value="October">October</option>
                  <option value="November">November</option>
                  <option value="December">December</option>
                </select>
              </div>
            </dev>
            <dev>
              <label className="block text-sm font-medium text-gray-700">Sender Email</label>
              <select
                value={editingEventData.sender_email}
                onChange={(e) => setEditingEventData({ ...editingEventData, sender_email: e.target.value })}
                className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm p-2"
              >
                <option value="">Select an email</option>
                <option value="accommodations@converiatravel.com">accommodations@converiatravel.com</option>
                <option value="auto">🔄 Auto (System will distribute by capacity)</option>
                <option value="coordination@converiatravel.com">coordination@converiatravel.com</option>
                <option value="housing@converiatravel.com">housing@converiatravel.com</option>
                <option value="logistics@converiatravels.com">logistics@converiatravels.com</option>
                <option value="reservations@converiatravels.com">reservations@converiatravels.com</option>
                <option value="lodgings@converiatravels.com">lodgings@converiatravels.com</option>
              </select>
            </dev>
            <dev>
              <label className="block text-sm font-medium text-gray-700">City</label>
              <input
                type="text"
                value={editingEventData.city}
                onChange={(e) => setEditingEventData({ ...editingEventData, city: e.target.value })}
                className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm p-2"
              />
            </dev>
            <dev>
              <label className="block text-sm font-medium text-gray-700">Venue</label>
              <input
                type="text"
                value={editingEventData.venue}
                onChange={(e) => setEditingEventData({ ...editingEventData, venue: e.target.value })}
                className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm p-2"
              />
            </dev>
            <dev>
              <label className="block text-sm font-medium text-gray-700">Date2</label>
              <input
                type="text"
                value={editingEventData.date2}
                onChange={(e) => setEditingEventData({ ...editingEventData, date2: e.target.value })}
                className="mt-1 block w-full border border-gray-300 rounded-md shadow-sm p-2"
              />
            </dev>
            {addEventError && <div className="text-red-600">{addEventError}</div>}
            <Button onClick={handleSaveEditedEvent} disabled={isEditingEvent} className="w-full">
              {isEditingEvent ? "Saving..." : "Save Changes"}
            </Button>
          </div>
        </DialogContent>
      </Dialog>  
      {/* Single Email Validation Modal (floating quick access) */}
      <Dialog open={showSingleEmailValidationModal} onOpenChange={setShowSingleEmailValidationModal}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Single Email Validation</DialogTitle>
          </DialogHeader>
          <SingleEmailValidation />
        </DialogContent>
      </Dialog>

      {/* Floating button: quick access to single email validation */}
      <button
        onClick={() => setShowSingleEmailValidationModal(true)}
        className="fixed bottom-6 right-6 z-50 bg-indigo-600 text-white rounded-full p-3 shadow-lg hover:bg-indigo-700"
        title="Single Email Validation"
      >
        Validate
      </button>
      {/* Global upload preview modal (shows validation/match results when any component publishes them) */}
      <GlobalUploadResultsModal />
      
      {/* Loading Overlays */}
      <LoadingOverlay isOpen={isSearchLoading} message={loadingMessage} />
      <LoadingOverlay isOpen={isContactActionLoading} message={loadingMessage} />
      </div>
  );
}
