import React, { useEffect, useState } from 'react';
import { useParams, useNavigate } from 'react-router-dom';
import { Event, CampaignContact } from '../types';
import { toast } from 'react-toastify';
import { EditEventModal } from './EditEventModal';
import { authFetch } from '@/utils/authFetch';

export function EventDetailsPage() {
  const { event_id } = useParams<{ event_id: string }>();
  const [event, setEvent] = useState<Event | null>(null);
  const [contacts, setContacts] = useState<CampaignContact[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [statusFilter, setStatusFilter] = useState('');
  const [stageFilter, setStageFilter] = useState('');
  const [editModalOpen, setEditModalOpen] = useState(false);
  const [campaignLoading, setCampaignLoading] = useState(false);
  const navigate = useNavigate();

  useEffect(() => {
    const fetchDetails = async () => {
      setLoading(true);
      setError('');
      try {
        const res = await authFetch(`/events/${event_id}`);
        if (!res.ok) throw new Error('Event not found');
        const data = await res.json();
        setEvent(data.event);
        setContacts(data.contacts);
      } catch (e: any) {
        setError(e.message || 'Failed to load event');
      } finally {
        setLoading(false);
      }
    };
    fetchDetails();
  }, [event_id]);

  const filteredContacts = contacts.filter(c =>
    (!statusFilter || (c.status || '').toLowerCase() === statusFilter.toLowerCase()) &&
    (!stageFilter || (c.stage || '').toLowerCase() === stageFilter.toLowerCase())
  );

  const handleExport = () => {
    window.open(`/events/${event_id}/export`, '_blank');
  };

  const handleEventSaved = (updated: Event) => {
    setEvent(updated);
    setEditModalOpen(false);
    toast.success('Event updated!');
  };

  const handleStartCampaign = async () => {
    if (!event_id) return;
    setCampaignLoading(true);
    try {
      // If backend endpoint exists, use it. Otherwise, show placeholder.
      const res = await authFetch(`/events/${event_id}/start-campaign`, { method: 'POST' });
      if (!res.ok) throw new Error('Failed to start campaign');
      toast.success('Campaign started!');
    } catch (err: any) {
      toast.error(err.message || 'Error starting campaign (placeholder)');
    } finally {
      setCampaignLoading(false);
    }
  };

  if (loading) return <div className="p-8">Loading...</div>;
  if (error) return <div className="p-8 text-red-600">{error}</div>;
  if (!event) return <div className="p-8">Event not found.</div>;

  const uniqueStatuses = Array.from(new Set(contacts.map(c => c.status).filter(Boolean)));
  const uniqueStages = Array.from(new Set(contacts.map(c => c.stage).filter(Boolean)));

  return (
    <div className="p-8 max-w-5xl mx-auto">
      <button className="mb-4 text-blue-600 underline" onClick={() => navigate('/events')}>&larr; Back to My Events</button>
      <div className="flex items-center gap-2 mb-2">
        <h2 className="text-xl font-bold">{event?.name}</h2>
        <button onClick={() => setEditModalOpen(true)} className="bg-yellow-500 text-white px-2 py-1 rounded">Edit Event</button>
      </div>
      <EditEventModal open={editModalOpen} onClose={() => setEditModalOpen(false)} event={event} onSaved={handleEventSaved} />
      <div className="mb-4 text-gray-700">
        <div><b>Dates:</b> {event.start_date} - {event.end_date}</div>
        <div><b>Location:</b> {event.location}</div>
        <div><b>Sender Email:</b> {event.sender_email}</div>
        <div><b>Status:</b> {event.status}</div>
        <div><b>Total Contacts:</b> {event.total_contacts}</div>
      </div>
      <div className="flex gap-2 mb-4">
        <button className="bg-blue-600 text-white px-4 py-2 rounded" onClick={handleExport}>Export Data</button>
        <button className="bg-green-600 text-white px-4 py-2 rounded disabled:opacity-50" onClick={handleStartCampaign} disabled={campaignLoading}>Start Campaign</button>
      </div>
      <div className="flex gap-2 mb-4">
        <select value={statusFilter} onChange={e => setStatusFilter(e.target.value)} className="border p-2 rounded">
          <option value="">All Statuses</option>
          {uniqueStatuses.map(s => <option key={s} value={s || ''}>{s}</option>)}
        </select>
        <select value={stageFilter} onChange={e => setStageFilter(e.target.value)} className="border p-2 rounded">
          <option value="">All Stages</option>
          {uniqueStages.map(s => <option key={s} value={s || ''}>{s}</option>)}
        </select>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full bg-white border border-gray-200">
          <thead>
            <tr>
              <th className="px-4 py-2">Name</th>
              <th className="px-4 py-2">Email</th>
              <th className="px-4 py-2">Status</th>
              <th className="px-4 py-2">Stage</th>
              <th className="px-4 py-2">Link</th>
            </tr>
          </thead>
          <tbody>
            {filteredContacts.map((c) => (
              <tr key={c.id}>
                <td className="px-4 py-2">{c.name}</td>
                <td className="px-4 py-2">{c.email}</td>
                <td className="px-4 py-2">{c.status}</td>
                <td className="px-4 py-2">{c.stage}</td>
                <td className="px-4 py-2">
                  {c.link ? <a href={c.link} target="_blank" rel="noopener noreferrer" className="text-blue-600 underline">Link</a> : ''}
                </td>
              </tr>
            ))}
            {filteredContacts.length === 0 && (
              <tr><td colSpan={5} className="text-center py-4">No contacts found.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
} 