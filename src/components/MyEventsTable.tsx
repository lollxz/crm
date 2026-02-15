// NOTE: This component is a modern, responsive events table with only the requested columns and UI.
import React, { useEffect, useState } from 'react';
import type { JSX } from 'react';
import { toast } from 'react-toastify';
import { useNavigate } from 'react-router-dom';
// @ts-ignore
import { Button } from '@/components/ui/button';
// @ts-ignore
import { Input } from '@/components/ui/input';
// @ts-ignore
import { Badge } from '@/components/ui/badge';
// @ts-ignore
import { Dialog, DialogContent, DialogHeader, DialogTitle } from '@/components/ui/dialog';
import { authFetch } from '@/utils/authFetch';
import { ExcelUpload } from './ExcelUpload';
import OrgSelector from './OrgSelector';
import { DEFAULT_STATUS_OPTIONS, STAGE_OPTIONS, COUNTRIES } from '../types';
import { uploadContactsExcel } from '../db';

interface Contact {
  id: number;
  name?: string;
  email?: string;
  stage?: string;
  status?: string;
  nationality?: string;
}

interface EventRow {
  id: number;
  event_name: string;
  org_name: string;
  month: string;
  sender_email: string;
  city: string;
  venue: string;
  date2: string;
  contacts: number;
  contacts_list: Contact[];
}

const columns = [
  { key: 'expand', label: '' },
  { key: 'id', label: 'ID' },
  { key: 'event_name', label: 'Event Name' },
  { key: 'org_name', label: 'Org Name' },
  { key: 'month', label: 'Month' },
  { key: 'sender_email', label: 'Sender Email' },
  { key: 'city', label: 'City' },
  { key: 'venue', label: 'Venue' },
  { key: 'date2', label: 'Date2' },
  { key: 'contacts', label: 'Contacts' },
];

const filterableColumns = columns.filter(col => col.key !== 'expand' && col.key !== 'contacts');

// Add color mapping functions for status and stage
const getStatusColor = (status: string) => {
  switch (status) {
    case 'No Response': return 'bg-gray-200 text-gray-800';
    case 'Not Interested': return 'bg-red-200 text-red-800';
    case 'Covered': return 'bg-green-200 text-green-800';
    case 'Follow-up': return 'bg-yellow-200 text-yellow-800';
    case 'First Reminder': return 'bg-blue-200 text-blue-800';
    case 'Second Reminder': return 'bg-indigo-200 text-indigo-800';
    case 'Third Reminder': return 'bg-purple-200 text-purple-800';
    case 'Completed': return 'bg-green-400 text-white';
    case 'Cancelled': return 'bg-gray-400 text-white';
    case 'Pending': return 'bg-orange-200 text-orange-800';
    case 'OOO': return 'bg-pink-200 text-pink-800';
    default: return 'bg-gray-100 text-gray-700';
  }
};
const getStageColor = (stage: string) => {
  switch (stage) {
    case 'First Message': return 'bg-blue-100 text-blue-800';
    case 'Forms': return 'bg-yellow-100 text-yellow-800';
    case 'Payments': return 'bg-green-100 text-green-800';
    case 'Completed': return 'bg-green-400 text-white';
    case 'Problem': return 'bg-red-200 text-red-800';
    default: return 'bg-gray-100 text-gray-700';
  }
};

// Add getNationalityColor function
const getNationalityColor = (nat: string) => 'bg-gray-100 text-gray-700'; // You can customize this if you want

export default function MyEventsTable(): JSX.Element {
  const [events, setEvents] = useState<EventRow[]>([]);
  const [search, setSearch] = useState('');
  const [filterCol, setFilterCol] = useState<string>('event_name');
  const [modalOpen, setModalOpen] = useState(false);
  const [creating, setCreating] = useState(false);
  const [form, setForm] = useState({
    event_name: '', org_name: '', month: '', sender_email: '', city: '', venue: '', date2: ''
  });
  const [uploading, setUploading] = useState(false);
  const [exporting, setExporting] = useState(false);
  const [expanded, setExpanded] = useState<{ [id: number]: boolean }>({});
  const [showFullName, setShowFullName] = useState<{ [id: number]: boolean }>({});
  const [globalSearch, setGlobalSearch] = useState('');
  const [searchResults, setSearchResults] = useState<any[]>([]);
  const navigate = useNavigate();

  // Add state for inline editing
  const [editingContactId, setEditingContactId] = useState<number | null>(null);
  const [editingField, setEditingField] = useState<'stage' | 'status' | 'nationality' | null>(null);
  const [customValue, setCustomValue] = useState('');
  const [searchNationality, setSearchNationality] = useState('');

  // 1. Add state for selected contacts and campaign modal
  const [selectedContacts, setSelectedContacts] = useState<{ [eventId: number]: Set<number> }>({});
  const [campaignModal, setCampaignModal] = useState<{ open: boolean, eventId: number | null }>({ open: false, eventId: null });
  const [campaignSubject, setCampaignSubject] = useState('');
  const [campaignMessage, setCampaignMessage] = useState('');
  const [campaignSender, setCampaignSender] = useState('');
  const [campaignLoading, setCampaignLoading] = useState(false);

  // 1. Add state for formsLink and paymentLink
  const [formsLink, setFormsLink] = useState('');
  const [paymentLink, setPaymentLink] = useState('');

  const fetchEvents = async () => {
    const token = localStorage.getItem('token');
    const res = await authFetch('/events', {
      headers: {
        'Authorization': token ? 'Bearer ' + token : ''
      }
    });
    if (!res.ok) return toast.error('Failed to load events');
    const data = await res.json();
    setEvents(data.items || []);
  };

  // Copy helper for event name
  const copyToClipboard = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text || '');
      toast.success('Full event name copied to clipboard');
    } catch (err) {
      console.error('Copy failed', err);
      toast.error('Failed to copy');
    }
  };

  // Add updateContact function inside the component so it can access fetchEvents
  const updateContact = async (id: number, updates: any) => {
    try {
      await authFetch(`/campaign_contacts/${id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(updates),
      });
      fetchEvents();
      toast.success('Contact updated');
    } catch (err) {
      toast.error('Failed to update contact');
    }
  };

  useEffect(() => { fetchEvents(); }, []);

  // Global contact search handler
  const handleGlobalSearch = async (query: string) => {
    setGlobalSearch(query);
    if (!query) {
      setSearchResults([]);
      return;
    }
    const token = localStorage.getItem('token');
    const res = await authFetch(`/campaign_contacts/search?query=${encodeURIComponent(query)}`, {
      headers: { 'Authorization': token ? `Bearer ${token}` : '' }
    });
    const data = await res.json();
    setSearchResults(data.results || []);
  };

  // Helper to check if an event or contact matches the global search
  const isEventMatch = (eventId: number) => searchResults.some(r => r.event_id === eventId);
  const isContactMatch = (contact: any) => searchResults.some(r => r.id === contact.id);

  const filteredEvents = events.filter((ev: EventRow) => {
    if (!search) return true;
    const col = filterCol || 'event_name';
    return (ev[col as keyof EventRow] || '').toString().toLowerCase().includes(search.toLowerCase());
  });

  // Small helper text for per-event contact inline search that explains the '+' syntax
  const plusSyntaxHint = 'You can search contacts by combining fields with "+" — e.g. "stage+status+trigger" (try: initial+pending+forms)';

  const handleCreate = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    setCreating(true);
    try {
      const token = localStorage.getItem('token');
  const res = await authFetch('https://conferencecare.org/api/events', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': token ? 'Bearer ' + token : ''
        },
        body: JSON.stringify(form),
      });
      if (!res.ok) throw new Error('Failed to create event');
      const data = await res.json();
      toast.success(`Event created! Use Event ID ${data.event_id} in your Excel upload.`);
      try {
        await navigator.clipboard.writeText(data.event_id.toString());
        toast.info('Event ID copied to clipboard');
      } catch {}
      setModalOpen(false);
      setForm({ event_name: '', org_name: '', month: '', sender_email: '', city: '', venue: '', date2: '' });
      fetchEvents();
    } catch (err: any) {
      toast.error(err.message || 'Error creating event');
    } finally {
      setCreating(false);
    }
  };

  const handleExport = async () => {
    setExporting(true);
    try {
      const token = localStorage.getItem('token');
      const res = await authFetch('/events/export', {
        headers: {
          'Authorization': token ? 'Bearer ' + token : ''
        }
      });
      if (!res.ok) throw new Error('Failed to export events');
      const blob = await res.blob();
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'events.csv';
      document.body.appendChild(a);
      a.click();
      a.remove();
      window.URL.revokeObjectURL(url);
    } catch (err: any) {
      toast.error(err.message || 'Export failed');
    } finally {
      setExporting(false);
    }
  };

  const toggleExpand = (id: number) => {
    setExpanded(exp => ({ ...exp, [id]: !exp[id] }));
  };

  // 2. Add helpers for selection
  const toggleContact = (eventId: number, contactId: number) => {
    setSelectedContacts(prev => {
      const set = new Set(prev[eventId] || []);
      if (set.has(contactId)) set.delete(contactId); else set.add(contactId);
      return { ...prev, [eventId]: set };
    });
  };
  const toggleAllContacts = (eventId: number, contacts: Contact[]) => {
    setSelectedContacts(prev => {
      const allSelected = (prev[eventId] && prev[eventId].size === contacts.length);
      return { ...prev, [eventId]: allSelected ? new Set() : new Set(contacts.map(c => c.id)) };
    });
  };

  // 3. Add campaign launch handler
  // 2. Load default templates when opening the modal
  const loadDefaultTemplates = async () => {
    try {
      const subjectRes = await fetch('/templates/emails/campaign_default_subject.txt');
      const bodyRes = await fetch('/templates/emails/campaign_default_body.txt');
      setCampaignSubject(await subjectRes.text());
      setCampaignMessage(await bodyRes.text());
    } catch {
      setCampaignSubject('');
      setCampaignMessage('');
    }
  };
  const handleStartCampaign = async (eventId: number, contacts: Contact[]) => {
    setCampaignModal({ open: true, eventId });
    setCampaignSender(events.find(ev => ev.id === eventId)?.sender_email || '');
    setFormsLink('');
    setPaymentLink('');
    await loadDefaultTemplates();
  };
  const handleSendCampaign = async () => {
    if (!campaignModal.eventId) return;
    const contactIds = Array.from(selectedContacts[campaignModal.eventId] || []);
    if (contactIds.length === 0) return toast.error('No contacts selected!');
    setCampaignLoading(true);
    try {
      const res = await authFetch('/start-campaign', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          customer_ids: contactIds,
          subject_template: campaignSubject,
          message_template: campaignMessage,
          sender_emails: [campaignSender],
          forms_link: formsLink,
          payment_link: paymentLink,
        })
      });
      if (!res.ok) throw new Error('Failed to start campaign');
      toast.success('Campaign started!');
      setCampaignModal({ open: false, eventId: null });
      setSelectedContacts(prev => ({ ...prev, [campaignModal.eventId!]: new Set() }));
    } catch (err: any) {
      toast.error(err.message || 'Failed to start campaign');
    } finally {
      setCampaignLoading(false);
    }
  };

  return (
    <div className="w-full max-w-7xl mx-auto bg-gray-50 p-6 rounded-2xl shadow-lg">
      {/* Global Contact Search Bar */}
      <div className="flex items-center mb-4 gap-2">
        <Input
          type="text"
          placeholder="Global search contacts by name or email..."
          value={globalSearch}
          onChange={e => handleGlobalSearch(e.target.value)}
          className="w-80"
        />
        {globalSearch && (
          <Button variant="ghost" size="sm" onClick={() => setGlobalSearch('')}>Clear</Button>
        )}
        <div className="ml-4 text-xs text-gray-500">Tip: combine fields with '+', e.g. <span className="font-medium">stage+status+trigger</span></div>
      </div>
      {/* Top Bar */}
      <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4 mb-6">
        <div className="flex-1 flex flex-col sm:flex-row gap-2 items-center">
          <Input
            className="w-full sm:w-64 text-sm"
            placeholder="Search events..."
            value={search}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setSearch(e.target.value)}
          />
          <div className="flex flex-wrap gap-1 mt-2 sm:mt-0">
            {filterableColumns.map(col => (
              <Button
                key={col.key}
                type="button"
                size="sm"
                variant={filterCol === col.key ? 'default' : 'outline'}
                className={filterCol === col.key ? 'bg-blue-600 text-white' : 'bg-gray-200 text-gray-700'}
                onClick={() => setFilterCol(col.key)}
              >
                {col.label}
              </Button>
            ))}
          </div>
        </div>
        <div className="flex gap-2 justify-end">
          <ExcelUpload
            isProcessing={uploading}
            onUpload={async (file) => {
              setUploading(true);
              try {
                const preview = await uploadContactsExcel(file, false);
                if (preview && preview.status === 'preview' && preview.summary && preview.summary.matched_rows > 0) {
                  const sample = (preview.results || []).filter((r: any) => r.matches && r.matches.length > 0).slice(0,5)
                    .map((m: any) => `${m.email} -> ${m.matches.map((mm: any) => `event:${mm.event_id} id:${mm.id}`).join(', ')}`).join('\n');
                  const proceed = window.confirm(`Detected ${preview.summary.matched_rows} matching row(s). Sample:\n\n${sample}\n\nProceed to save and persist these rows?`);
                  if (!proceed) {
                    setUploading(false);
                    return;
                  }
                }

                await uploadContactsExcel(file, true);
                toast.success('Contacts uploaded!');
                fetchEvents();
              } catch (err) {
                toast.error('Failed to upload contacts');
              } finally {
                setUploading(false);
              }
            }}
          />
          <Button variant="outline" onClick={handleExport} disabled={exporting} className="text-sm">⬇ Export</Button>
          <Button onClick={() => setModalOpen(true)} className="bg-blue-600 text-white text-sm font-semibold">+ Create New Event</Button>
        </div>
      </div>
      {/* Table */}
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200 shadow rounded-lg overflow-hidden bg-white">
          <thead className="bg-gray-50">
            <tr>
              {columns.map(col => (
                <th key={col.key} className="px-4 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                  {col.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-100">
            {filteredEvents.map(event => (
              <React.Fragment key={event.id}>
                <tr className="hover:bg-gray-50 transition">
                  <td className="px-4 py-2">
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => toggleExpand(event.id)}
                      className="text-gray-600 hover:text-gray-900"
                    >
                      {expanded[event.id] ? '▼' : '▶'}
                    </Button>
                  </td>
                  <td className="px-4 py-2 font-semibold">{event.id}</td>
                  <td className="px-4 py-2">
                    {(() => {
                      const full = event.event_name || '';
                      const limit = 40;
                      if (full.length <= limit) return <span>{full}</span>;
                      const showing = !!showFullName[event.id];
                      return (
                        <div>
                          <div className="flex items-center gap-2">
                            <span>{showing ? full : `${full.slice(0, limit)}...`}</span>
                            <Button size="sm" variant="ghost" onClick={() => setShowFullName(s => ({ ...s, [event.id]: !s[event.id] }))}>
                              {showing ? 'Show less' : 'Read more'}
                            </Button>
                            <Button size="sm" variant="outline" onClick={() => copyToClipboard(full)}>
                              Copy
                            </Button>
                          </div>
                        </div>
                      );
                    })()}
                  </td>
                  <td className="px-4 py-2">{event.org_name}</td>
                  <td className="px-4 py-2">{event.month}</td>
                  <td className="px-4 py-2">{event.sender_email}</td>
                  <td className="px-4 py-2">{event.city}</td>
                  <td className="px-4 py-2">{event.venue}</td>
                  <td className="px-4 py-2">{event.date2}</td>
                  <td className="px-4 py-2 text-center font-bold">{event.contacts}</td>
                </tr>
                {expanded[event.id] && (
                  <tr className="bg-blue-50">
                    <td colSpan={columns.length} className="p-0">
                      <div className="p-4">
                        <div className="flex items-center mb-2 gap-2">
                          <Input
                            type="text"
                            placeholder="Search contacts in this event..."
                            value={search}
                            onChange={e => setSearch(e.target.value)}
                            className="w-64"
                          />
                          {search && (
                            <Button variant="ghost" size="sm" onClick={() => setSearch('')}>Clear</Button>
                          )}
                        </div>
                        <div className="text-sm text-gray-500 mb-2">
                          Tip: you can combine stage and status using a plus sign. For example
                          <span className="font-medium"> &nbsp;initial+pending</span> will return contacts whose
                          stage contains "initial" AND status contains "pending".
                        </div>
                        {/* Excel upload for this event */}
                        <ExcelUpload
                          isProcessing={uploading}
                          onUpload={async (file) => {
                            setUploading(true);
                            try {
                              await uploadContactsExcel(file);
                              toast.success('Contacts uploaded!');
                              fetchEvents();
                            } catch (err) {
                              toast.error('Failed to upload contacts');
                            } finally {
                              setUploading(false);
                            }
                          }}
                        />
                        {/* Campaign modal trigger */}
                        <Button
                          className="bg-blue-600 text-white px-4 py-2 rounded mt-2"
                          disabled={!(selectedContacts[event.id]?.size > 0)}
                          onClick={() => setCampaignModal({ open: true, eventId: event.id })}
                        >
                          Start Campaign for Selected
                        </Button>
                        {/* Contacts table as before */}
                        <div className="overflow-x-auto mt-4">
                          <table className="min-w-full divide-y divide-gray-200 shadow rounded-lg overflow-hidden bg-white">
                            <thead className="bg-gray-50">
                              <tr>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                                  Name
                                </th>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                                  Email
                                </th>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                                  Stage
                                </th>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                                  Status
                                </th>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                                  Nationality
                                </th>
                                <th className="px-4 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                                  Actions
                                </th>
                              </tr>
                            </thead>
                            <tbody className="bg-white divide-y divide-gray-100">
                              {event.contacts_list.map(contact => (
                                <tr key={contact.id} className="hover:bg-gray-50 transition">
                                  <td className="px-4 py-2">
                                    {isContactMatch(contact) ? (
                                      <span className="font-semibold text-blue-600">{contact.name}</span>
                                    ) : (
                                      contact.name
                                    )}
                                  </td>
                                  <td className="px-4 py-2">
                                    {isContactMatch(contact) ? (
                                      <span className="font-semibold text-blue-600">{contact.email}</span>
                                    ) : (
                                      contact.email
                                    )}
                                  </td>
                                  <td className="px-4 py-2">
                                    {isContactMatch(contact) ? (
                                      <Badge className="bg-blue-100 text-blue-800">{contact.stage}</Badge>
                                    ) : (
                                      <Badge className={getStageColor(contact.stage || '')}>{contact.stage}</Badge>
                                    )}
                                  </td>
                                  <td className="px-4 py-2">
                                    {isContactMatch(contact) ? (
                                      <Badge className="bg-blue-100 text-blue-800">{contact.status}</Badge>
                                    ) : (
                                      <Badge className={getStatusColor(contact.status || '')}>{contact.status}</Badge>
                                    )}
                                  </td>
                                  <td className="px-4 py-2">
                                    {isContactMatch(contact) ? (
                                      <Badge className="bg-blue-100 text-blue-800">{contact.nationality}</Badge>
                                    ) : (
                                      <Badge className={getNationalityColor(contact.nationality || '')}>{contact.nationality}</Badge>
                                    )}
                                  </td>
                                  <td className="px-4 py-2 text-sm">
                                    <Button
                                      variant="ghost"
                                      size="sm"
                                      onClick={() => toggleContact(event.id, contact.id)}
                                      className={`${selectedContacts[event.id]?.has(contact.id) ? 'bg-blue-100 text-blue-800' : 'text-gray-600 hover:text-gray-900'}`}
                                    >
                                      {selectedContacts[event.id]?.has(contact.id) ? '✓' : ''}
                                    </Button>
                                  </td>
                                </tr>
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
      {/* Create Event Modal */}
      <Dialog open={modalOpen} onOpenChange={setModalOpen}>
        <DialogContent className="max-w-md bg-white shadow-xl p-6 rounded-2xl w-[600px] max-w-full">
          <DialogHeader>
            <DialogTitle className="text-lg font-bold">Create New Event</DialogTitle>
          </DialogHeader>
          <form onSubmit={handleCreate} className="flex flex-col gap-3 mt-2">
            <Input placeholder="Event Name" value={form.event_name} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setForm(f => ({ ...f, event_name: e.target.value }))} required />
            {/* Organization selector: replaced free-text with dropdown */}
            <OrgSelector value={form.org_name} onChange={(val: any) => setForm(f => ({ ...f, org_name: val }))} onCreated={() => fetchEvents()} />
            <Input placeholder="Month" value={form.month} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setForm(f => ({ ...f, month: e.target.value }))} required />
            <Input placeholder="Sender Email" type="email" value={form.sender_email} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setForm(f => ({ ...f, sender_email: e.target.value }))} required />
            <Input placeholder="City" value={form.city} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setForm(f => ({ ...f, city: e.target.value }))} required />
            <Input placeholder="Venue" value={form.venue} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setForm(f => ({ ...f, venue: e.target.value }))} required />
            <Input placeholder="Date2 (e.g. 11_15 Jun)" value={form.date2} onChange={(e: React.ChangeEvent<HTMLInputElement>) => setForm(f => ({ ...f, date2: e.target.value }))} required />
            <div className="flex gap-2 justify-end mt-2">
              <Button type="button" variant="outline" onClick={() => setModalOpen(false)} disabled={creating}>Cancel</Button>
              <Button type="submit" className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded" disabled={creating}>Create</Button>
            </div>
          </form>
        </DialogContent>
      </Dialog>
      {/* Add the campaign modal at the end of the component: */}
      {campaignModal.open && (
        <Dialog open={campaignModal.open} onOpenChange={open => setCampaignModal({ open, eventId: campaignModal.eventId })}>
          <DialogContent className="max-w-md bg-white shadow-xl p-6 rounded-2xl w-[600px] max-w-full">
            <DialogHeader>
              <DialogTitle className="text-lg font-bold">Start Email Campaign</DialogTitle>
            </DialogHeader>
            <div className="flex flex-col gap-3 mt-2">
              <Input placeholder="Sender Email" value={campaignSender} onChange={e => setCampaignSender(e.target.value)} required />
              <Input placeholder="Forms Link (optional)" value={formsLink} onChange={e => setFormsLink(e.target.value)} />
              <Input placeholder="Payment Link (optional)" value={paymentLink} onChange={e => setPaymentLink(e.target.value)} />
              <Input placeholder="Subject" value={campaignSubject} onChange={e => setCampaignSubject(e.target.value)} required />
              <textarea className="border rounded px-2 py-1" rows={5} placeholder="Message" value={campaignMessage} onChange={e => setCampaignMessage(e.target.value)} required />
              <div className="flex gap-2 justify-end mt-2">
                <Button type="button" variant="outline" onClick={() => setCampaignModal({ open: false, eventId: null })} disabled={campaignLoading}>Cancel</Button>
                <Button type="button" className="bg-blue-600 hover:bg-blue-700 text-white px-4 py-2 rounded" onClick={handleSendCampaign} disabled={campaignLoading}>
                  {campaignLoading ? 'Sending...' : 'Send Campaign'}
                </Button>
              </div>
            </div>
          </DialogContent>
        </Dialog>
      )}
    </div>
  );
} 
