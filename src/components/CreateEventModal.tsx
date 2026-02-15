import React, { useEffect, useState } from 'react';
import { toast } from 'react-toastify';
import { authFetch } from '@/utils/authFetch';
import OrgSelector from './OrgSelector';

type Org = { id: number; name: string };

const MONTHS = [
  "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December"
];

const SENDER_EMAILS = [
  'accommodations@converiatravel.com',
  'coordination@converiatravel.com',
  'housing@converiatravel.com',
  'logistics@converiatravels.com',
  'reservations@converiatravels.com',
  'lodgings@converiatravels.com',
];

interface CreateEventModalProps {
  open: boolean;
  onClose: () => void;
  onCreated: (event_id: number) => void;
}

export function CreateEventModal({ open, onClose, onCreated }: CreateEventModalProps) {
  const [eventName, setEventName] = useState('');
  const [month, setMonth] = useState('');
  const [customMonth, setCustomMonth] = useState('');
  const [isCustomMonth, setIsCustomMonth] = useState(false);
  const [senderEmail, setSenderEmail] = useState('');
  const [expectedContactCount, setExpectedContactCount] = useState('');
  const [city, setCity] = useState('');
  const [venue, setVenue] = useState('');
  const [eventUrl, setEventUrl] = useState('');
  const [date2, setDate2] = useState('');
  const [loading, setLoading] = useState(false);
  const [orgs, setOrgs] = useState<Org[]>([]);
  const [orgId, setOrgId] = useState<number | null>(null);
  const [similarMatches, setSimilarMatches] = useState<Array<{ event_id: number; event_name: string; score: number }>>([]);
  const [similarLoading, setSimilarLoading] = useState(false);
  const [senderCapacities, setSenderCapacities] = useState<Record<string, { load: number; available: number }>>({});

  useEffect(() => {
    if (!open) return;
    const fetchOrgs = async () => {
      try {
        const res = await authFetch('/organizations/');
        if (res.ok) setOrgs(await res.json() as Org[]);
      } catch {}
    };
    fetchOrgs();
    fetchSenderCapacities();
  }, [open]);

  // Fetch current sender capacities
  const fetchSenderCapacities = async () => {
    try {
      const res = await authFetch('/admin/sender-capacities');
      if (res.ok) {
        const data = await res.json();
        setSenderCapacities(data);
      }
    } catch {
      // If endpoint doesn't exist, just skip capacity display
    }
  };

  // Update sender capacities when contact count changes
  useEffect(() => {
    if (!expectedContactCount || isNaN(Number(expectedContactCount))) return;

    const contactCount = Number(expectedContactCount);
    const capacities: Record<string, { load: number; available: number }> = {};

    // Calculate remaining capacity for each sender
    SENDER_EMAILS.forEach((email) => {
      const current = senderCapacities[email]?.load || 0;
      const available = 150 - current - contactCount;
      capacities[email] = {
        load: current,
        available: Math.max(0, available)
      };
    });

    setSenderCapacities(capacities);
  }, [expectedContactCount]);

  // Debounced similarity check for event name
  useEffect(() => {
    if (!eventName || eventName.trim().length < 3) {
      setSimilarMatches([]);
      return;
    }
    const t = setTimeout(async () => {
      setSimilarLoading(true);
      try {
        const q = `/search/events/similarity?name=${encodeURIComponent(eventName)}&threshold=80`;
        const res = await authFetch(q);
        if (res.ok) {
          const data = await res.json();
          setSimilarMatches(data.matches || []);
        } else {
          setSimilarMatches([]);
        }
      } catch (err) {
        setSimilarMatches([]);
      } finally {
        setSimilarLoading(false);
      }
    }, 450);
    return () => clearTimeout(t);
  }, [eventName]);

  const handleSubmit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!eventName.trim()) return toast.error('Event Name is required');
    if (!expectedContactCount || Number(expectedContactCount) <= 0) {
      return toast.error('Expected Contact Count is required and must be greater than 0');
    }

    setLoading(true);
    try {
      const orgName = orgs.find((o) => o.id === orgId)?.name || '';
      let normalizedUrl: string | null = null;
      if (eventUrl && eventUrl.trim()) {
        const raw = eventUrl.trim();
        normalizedUrl = /^[a-zA-Z][a-zA-Z0-9+.-]*:/.test(raw) ? raw : `https://${raw}`;
      }

      const payload = {
        event_name: eventName,
        org_id: orgId ?? null,
        org_name: orgName,
        month: isCustomMonth ? customMonth : month,
        sender_email: senderEmail,
        city,
        venue,
        date2,
        event_url: normalizedUrl,
        expected_contact_count: Number(expectedContactCount),
      };

      const res = await authFetch('/events', {
        method: 'POST',
        body: JSON.stringify(payload),
      });
      if (!res.ok) throw new Error('Failed to create event');
      const data = await res.json();
      const newId = data.event_id ?? data.id ?? null;
      toast.success(`Event created${newId ? `! ID: ${newId}` : ''}`);
      if (newId) {
        try {
          await navigator.clipboard.writeText(String(newId));
          toast.info('Event ID copied to clipboard');
        } catch {}
      }
      onCreated(newId);
      onClose();
    } catch (err: any) {
      toast.error(err.message || 'Error creating event');
    } finally {
      setLoading(false);
    }
  };

  const fetchOrgs = async () => {
    try {
      const res = await authFetch('/organizations/');
      if (res.ok) setOrgs(await res.json() as Org[]);
    } catch {}
  };

  const getSenderDisplayText = (email: string) => {
    const capacity = senderCapacities[email];
    if (!capacity) return email;
    const available = capacity.available;
    const load = capacity.load;
    const statusColor = available < 0 ? 'text-red-600' : available === 0 ? 'text-orange-600' : 'text-green-600';
    return `${email} (Loaded: ${load}, After event: ${Math.max(0, load + Number(expectedContactCount || 0))}/150)`;
  };

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Overlay */}
      <div
        className="absolute inset-0 bg-black/40 backdrop-blur-sm"
        onClick={onClose}
      ></div>

      {/* Modal Container */}
      <div className="relative z-10 bg-white rounded-2xl shadow-2xl max-w-lg w-full p-6 max-h-[90vh] overflow-y-auto">
        <h2 className="text-2xl font-semibold mb-5 text-gray-800">Add New Event</h2>

        <form onSubmit={handleSubmit} className="space-y-3 relative">
          <input
            className="w-full border border-gray-300 rounded-lg p-3 focus:outline-none focus:ring-2 focus:ring-black transition"
            placeholder="Event Name"
            value={eventName}
            onChange={(e: React.ChangeEvent<HTMLInputElement>) => setEventName(e.target.value)}
          />

          {/* Similarity suggestions */}
          {eventName.trim().length > 0 && (
            <div className="mt-2">
              {similarLoading && <div className="text-sm text-gray-500">Checking similar events…</div>}
              {!similarLoading && similarMatches.length > 0 && (
                <div className="p-2 border rounded bg-yellow-50 text-sm text-gray-800">
                  <div className="font-semibold mb-1">Possible duplicate events</div>
                  {similarMatches.slice(0, 3).map((m: { event_id: number; event_name: string; score: number }) => (
                    <div key={m.event_id} className="flex items-center justify-between py-1">
                      <div className="truncate">{m.event_name}</div>
                      <div className="ml-3 text-xs text-gray-600">{m.score}%</div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          )}

          <div className="relative z-30">
            <OrgSelector
              value={orgId ?? ''}
              onChange={(val: any) => setOrgId(val ? Number(val) : null)}
              onCreated={() => fetchOrgs()}
            />
          </div>

          <select
            className="w-full border border-gray-300 rounded-lg p-3 focus:outline-none focus:ring-2 focus:ring-black transition"
            value={isCustomMonth ? 'custom' : month}
            onChange={(e) => {
              if (e.target.value === 'custom') {
                setIsCustomMonth(true);
                setMonth('');
                setCustomMonth('');
              } else {
                setIsCustomMonth(false);
                setMonth(e.target.value);
                setCustomMonth('');
              }
            }}
          >
            <option value="">Select a Month</option>
            {MONTHS.map((m) => (
              <option key={m} value={m}>
                {m}
              </option>
            ))}
            <option value="custom">Custom</option>
          </select>

          {isCustomMonth && (
            <input
              className="w-full border border-gray-300 rounded-lg p-3 focus:outline-none focus:ring-2 focus:ring-black transition"
              placeholder="Type custom month"
              value={customMonth}
              onChange={(e) => setCustomMonth(e.target.value)}
            />
          )}

          {/* Expected Contact Count Input */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Expected Number of Contacts
            </label>
            <input
              type="number"
              min="1"
              max="500"
              className="w-full border border-gray-300 rounded-lg p-3 focus:outline-none focus:ring-2 focus:ring-black transition"
              placeholder="e.g., 150"
              value={expectedContactCount}
              onChange={(e) => setExpectedContactCount(e.target.value)}
            />
            <p className="text-xs text-gray-500 mt-1">
              Enter the number of contacts you'll upload. This helps the system distribute across senders fairly.
            </p>
          </div>

          {/* Sender Email Selection with Capacity Info */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">
              Sender Email
            </label>
            <select
              className="w-full border border-gray-300 rounded-lg p-3 focus:outline-none focus:ring-2 focus:ring-black transition"
              value={senderEmail}
              onChange={(e) => setSenderEmail(e.target.value)}
            >
              <option value="">Select Sender Email</option>
              <option value="auto">🔄 Auto (System will distribute by capacity)</option>
              {SENDER_EMAILS.map((email) => (
                <option key={email} value={email}>
                  {getSenderDisplayText(email)}
                </option>
              ))}
            </select>
            {senderEmail && senderEmail !== 'auto' && expectedContactCount && (
              <p className="text-xs text-gray-500 mt-1">
                Current load: {senderCapacities[senderEmail]?.load || 0} contacts today
              </p>
            )}
          </div>

          <input
            className="w-full border border-gray-300 rounded-lg p-3 focus:outline-none focus:ring-2 focus:ring-black transition"
            placeholder="City"
            value={city}
            onChange={(e) => setCity(e.target.value)}
          />

          <input
            className="w-full border border-gray-300 rounded-lg p-3 focus:outline-none focus:ring-2 focus:ring-black transition"
            placeholder="Venue"
            value={venue}
            onChange={(e) => setVenue(e.target.value)}
          />

          <input
            className="w-full border border-gray-300 rounded-lg p-3 focus:outline-none focus:ring-2 focus:ring-black transition"
            placeholder="Event URL (https://...)"
            value={eventUrl}
            onChange={(e) => setEventUrl(e.target.value)}
          />

          <input
            className="w-full border border-gray-300 rounded-lg p-3 focus:outline-none focus:ring-2 focus:ring-black transition"
            placeholder="Date2"
            value={date2}
            onChange={(e) => setDate2(e.target.value)}
          />

          <button
            type="submit"
            disabled={loading}
            className={`w-full py-3 rounded-lg text-white font-medium transition ${
              loading
                ? 'bg-gray-400 cursor-not-allowed'
                : 'bg-black hover:bg-gray-800'
            }`}
          >
            {loading ? 'Creating...' : 'Add Event'}
          </button>
        </form>
      </div>
    </div>
  );
}
