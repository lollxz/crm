import React, { useState, useEffect } from 'react';
import { Event } from '../types';
import { toast } from 'react-toastify';
import { authFetch } from '@/utils/authFetch';

interface EditEventModalProps {
  open: boolean;
  onClose: () => void;
  event: Event | null;
  onSaved: (event: Event) => void;
}

export function EditEventModal({ open, onClose, event, onSaved }: EditEventModalProps) {
  const [name, setName] = useState('');
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');
  const [location, setLocation] = useState('');
  const [senderEmail, setSenderEmail] = useState('');
  const [status, setStatus] = useState('');
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (event) {
      setName(event.name || '');
      setStartDate(event.start_date || '');
      setEndDate(event.end_date || '');
      setLocation(event.location || '');
      setSenderEmail(event.sender_email || '');
      setStatus(event.status || '');
    }
  }, [event, open]);

  if (!open || !event) return null;

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    try {
      const res = await authFetch(`/events/${event.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name,
          start_date: startDate,
          end_date: endDate,
          location,
          sender_email: senderEmail,
          status,
        }),
      });
      if (!res.ok) throw new Error('Failed to update event');
      const data = await res.json();
      toast.success('Event updated!');
      onSaved(data);
      onClose();
    } catch (err: any) {
      toast.error(err.message || 'Error updating event');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="modal-backdrop">
      <div className="modal">
        <h2>Edit Event</h2>
        <form onSubmit={handleSubmit}>
          <label>Name:<input value={name} onChange={e => setName(e.target.value)} required /></label>
          <label>Start Date:<input type="date" value={startDate} onChange={e => setStartDate(e.target.value)} /></label>
          <label>End Date:<input type="date" value={endDate} onChange={e => setEndDate(e.target.value)} /></label>
          <label>Location:<input value={location} onChange={e => setLocation(e.target.value)} /></label>
          <label>Sender Email:<input type="email" value={senderEmail} onChange={e => setSenderEmail(e.target.value)} /></label>
          <label>Status:<input value={status} onChange={e => setStatus(e.target.value)} /></label>
          <div className="modal-actions">
            <button type="button" onClick={onClose} disabled={loading}>Cancel</button>
            <button type="submit" disabled={loading}>Save</button>
          </div>
        </form>
      </div>
    </div>
  );
} 