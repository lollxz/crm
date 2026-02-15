import React, { useEffect, useMemo, useState } from 'react';
import { authFetch } from '../utils/authFetch';
import { useAuth } from '../contexts/AuthContext';
import { format } from 'date-fns';
import { Inbox, FileText, CreditCard, Layers, AlertTriangle } from 'lucide-react';
import * as DropdownMenu from '@radix-ui/react-dropdown-menu';

interface QueueRow {
  contact_id: number | string;
  event_id: number | string;
  name: string;
  email: string;
  current_stage?: string;
  current_status?: string;
  next_action?: string;
  next_action_at?: string | null;
  last_message_preview?: string | null;
  last_message_sent_at?: string | null;
  last_updated_by?: string | null;
  send_failed?: boolean;
  queue_created_at?: string | null;
  contact_created_at?: string | null;
  sender_email?: string | null;
}

export function QueuePage() {
  const { user } = useAuth();
  const [rows, setRows] = useState<QueueRow[]>([]);
  const [query, setQuery] = useState('');
  const [dedupe, setDedupe] = useState(false); // false => show every queue entry; true => one row per contact
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Overview state (new)
  type StageBreakdown = { total: number; [key: string]: number };
  interface QueueOverview {
    initial: StageBreakdown;
    forms: StageBreakdown;
    payments: StageBreakdown;
    custom_flow: StageBreakdown;
    // errors summary added by backend
    errors?: {
      total_items: number;
      contacts: number;
      by_error_message: Record<string, number>;
    };
  }

  const [overview, setOverview] = useState<QueueOverview | null>(null);
  const [overviewLoading, setOverviewLoading] = useState(false);
  const [overviewError, setOverviewError] = useState<string | null>(null);
  const [dropdownOpen, setDropdownOpen] = useState<Record<string, boolean>>({});

  // Added missing state hooks required by the component
  const [total, setTotal] = useState<number>(0);
  const [counts, setCounts] = useState<Record<string, number> | null>(null);

  const [flowOpen, setFlowOpen] = useState<boolean>(false);
  const [flowContact, setFlowContact] = useState<{ contact_id: number | string; name: string; email: string } | null>(null);
  const [flowSteps, setFlowSteps] = useState<any[]>([]);
  const [flowLoading, setFlowLoading] = useState<boolean>(false);
  const [flowError, setFlowError] = useState<string | null>(null);

  type TypeCount = { message_type: string; count: number };
  const [typesOpen, setTypesOpen] = useState<boolean>(false);
  const [typeCounts, setTypeCounts] = useState<TypeCount[]>([]);
  const [typeCountsLoading, setTypeCountsLoading] = useState<boolean>(false);
  const [typeCountsError, setTypeCountsError] = useState<string | null>(null);

  // Store timeout ID as ref for debounce
  const searchTimeoutRef = React.useRef<ReturnType<typeof setTimeout> | null>(null);

  // Function to calculate next action based on current status
  const getNextAction = (status: string | undefined, stage: string | undefined, messageType?: string | undefined): string => {
    // Prefer explicit messageType (campaign_stage) when status is generic like 'sent'
    const genericStatuses = new Set(['sent', 'pending', 'skipped', 'failed', 'queued', 'processed']);

    const statusToken = (status || '').toLowerCase();
    const stageToken = (stage || '').toLowerCase();
    const msgTypeToken = (messageType || '').toLowerCase();

    // If status is generic but we have a messageType token, use that for decision
    const effectiveToken = (genericStatuses.has(statusToken) && msgTypeToken) ? msgTypeToken : statusToken;

    if (!effectiveToken && !stageToken) return '-';

    // PRIMARY: Check stageToken directly (campaign_main, forms_main, payments_initial, etc.)
    // This is the most reliable source for message type
    
    // Initial/Campaign Main progression
    if (stageToken.includes('campaign_main') || stageToken.includes('first_message')) return 'First Reminder (3 days)';
    if (stageToken.includes('first_reminder')) return 'Second Reminder (4 days)';
    if (stageToken.includes('second_reminder')) return 'Finalized (after 3 days)';

    // Forms progression
    if (stageToken.includes('forms_initial') || stageToken.includes('forms_main')) return 'Forms Reminder 1 (2 days)';
    if (stageToken.includes('forms_reminder1')) return 'Forms Reminder 2 (2 days)';
    if (stageToken.includes('forms_reminder2')) return 'Forms Reminder 3 (3 days)';
    if (stageToken.includes('forms_reminder3')) return 'Finalized (after 3 days)';

    // Payments progression
    if (stageToken.includes('payments_initial') || stageToken.includes('payments_main')) return 'Payments Reminder 1 (2 days)';
    if (stageToken.includes('payments_reminder1')) return 'Payments Reminder 2 (2 days)';
    if (stageToken.includes('payments_reminder2')) return 'Payments Reminder 3 (3 days)';
    if (stageToken.includes('payments_reminder3')) return 'Payments Reminder 4 (7 days)';
    if (stageToken.includes('payments_reminder4')) return 'Payments Reminder 5 (7 days)';
    if (stageToken.includes('payments_reminder5')) return 'Payments Reminder 6 (7 days)';
    if (stageToken.includes('payments_reminder6')) return 'Finalized (after 3 days)';

    // Custom flow
    if (stageToken.includes('step') || stageToken.includes('custom')) {
      const stepMatch = stageToken.match(/step[_-]?(\d+)/);
      if (stepMatch) {
        const currentStep = parseInt(stepMatch[1]);
        if (!Number.isNaN(currentStep)) return `Custom Step ${currentStep + 1}`;
      }
      return 'Custom Flow - Next Step';
    }

    // SECONDARY: Check effectiveToken (messageType fallback)
    if (effectiveToken.includes('first_message') || effectiveToken.includes('campaign_main')) return 'First Reminder (3 days)';
    if (effectiveToken.includes('first_reminder')) return 'Second Reminder (4 days)';
    if (effectiveToken.includes('second_reminder')) return 'Finalized (after 3 days)';
    if (effectiveToken.includes('forms_initial') || effectiveToken.includes('forms_main')) return 'Forms Reminder 1 (2 days)';
    if (effectiveToken.includes('forms_reminder1')) return 'Forms Reminder 2 (2 days)';
    if (effectiveToken.includes('forms_reminder2')) return 'Forms Reminder 3 (3 days)';
    if (effectiveToken.includes('forms_reminder3')) return 'Finalized (after 3 days)';
    if (effectiveToken.includes('payments_initial') || effectiveToken.includes('payments_main')) return 'Payments Reminder 1 (2 days)';
    if (effectiveToken.includes('payments_reminder1')) return 'Payments Reminder 2 (2 days)';
    if (effectiveToken.includes('payments_reminder2')) return 'Payments Reminder 3 (3 days)';
    if (effectiveToken.includes('payments_reminder3')) return 'Payments Reminder 4 (7 days)';
    if (effectiveToken.includes('payments_reminder4')) return 'Payments Reminder 5 (7 days)';
    if (effectiveToken.includes('payments_reminder5')) return 'Payments Reminder 6 (7 days)';
    if (effectiveToken.includes('payments_reminder6')) return 'Finalized (after 3 days)';

    // Fallback
    return '-';
  };

  // Explicit bucket keys the backend now returns
  const INITIAL_KEYS = ['initial', 'first_message_sent', 'first_reminder', 'second_reminder', 'total'];
  const FORMS_KEYS = ['forms_main', 'forms_reminder1_sent', 'forms_reminder2_sent', 'forms_reminder3_sent', 'total'];
  const PAYMENTS_KEYS = ['payments_initial', 'payments_reminder1_sent', 'payments_reminder2_sent', 'payments_reminder3_sent', 'payments_reminder4_sent', 'payments_reminder5_sent', 'payments_reminder6_sent', 'total'];

  const renderBreakdown = (category: keyof QueueOverview, keys: string[]) => {
    if (!overview) return null;
    return (
      <>
        {keys.map(k => (
          <div key={k} className="flex justify-between">
            <div className="capitalize">{k.replace(/_/g, ' ').replace(/\bsent\b/, 'sent')}</div>
            <div className="text-gray-700">{(overview as any)[category]?.[k] ?? 0}</div>
          </div>
        ))}
      </>
    );
  };

  // Fetch overview from new backend endpoint
  const fetchOverview = async () => {
    setOverviewLoading(true);
    setOverviewError(null);
    try {
      const res = await authFetch(`${import.meta.env.VITE_API_URL}/api/queue/overview`);
      if (!res.ok) {
        const txt = await res.text().catch(() => '');
        throw new Error(`${res.status} ${res.statusText}: ${txt}`);
      }
      const data = await res.json();
      setOverview(data as QueueOverview);
    } catch (e: any) {
      console.error('Overview fetch error', e);
      setOverviewError(e.message || 'Failed to load overview');
    } finally {
      setOverviewLoading(false);
    }
  };

  const fetchQueue = async (queryStr = '', dedupeParam: boolean | null = null) => {
    setLoading(true);
    setError(null);
    try {
      const effectiveDedupe = dedupeParam === null ? dedupe : dedupeParam;
      
      // Use the /api/queue/contacts endpoint with optional 'q' parameter for search
      const params = new URLSearchParams();
      if (queryStr && queryStr.trim()) {
        params.set('q', queryStr.trim());
      }
      params.set('dedupe', effectiveDedupe ? 'true' : 'false');
      
      const res = await authFetch(`${import.meta.env.VITE_API_URL}/api/queue/contacts?${params.toString()}`);
      if (!res.ok) {
        const text = await res.text().catch(() => '');
        throw new Error(`${res.status} ${res.statusText}: ${text}`);
      }
      const data = await res.json().catch(async () => {
        const txt = await res.text().catch(() => '');
        throw new Error('Invalid JSON response: ' + txt);
      });
      
      let contactsAccum = data.contacts || [];
      const totalFromServer = typeof data.total === 'number' ? data.total : 0;
      setTotal(totalFromServer);
      if (data.counts) setCounts(data.counts);
      
      setRows(contactsAccum);
    } catch (e: any) {
      console.error(e);
      setError(e.message || 'Unknown error');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    // On mount, fetch the live overview and all queue items (not deduplicated)
    fetchOverview();
    // Fetch all queue items from email_queue table - search is backend-driven
    fetchQueue('', false);

    // Poll overview periodically to keep counts live
    const iv = setInterval(() => {
      fetchOverview();
    }, 15000);
    return () => clearInterval(iv);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  const fetchContactFlow = async (contact_id: number | string) => {
    setFlowLoading(true);
    setFlowError(null);
    setFlowSteps([]);
    try {
      const res = await authFetch(`${import.meta.env.VITE_API_URL}/contacts/${contact_id}/messages`);
      if (!res.ok) {
        const txt = await res.text().catch(() => '');
        throw new Error(`${res.status} ${res.statusText}: ${txt}`);
      }
      const data = await res.json();
      setFlowSteps(Array.isArray(data) ? data : data.steps || []);
    } catch (e: any) {
      console.error(e);
      setFlowError(e.message || 'Failed to load messages');
    } finally {
      setFlowLoading(false);
    }
  };

  // Fetch message type counts for the Types modal
  const fetchTypeCounts = async () => {
    setTypeCountsLoading(true);
    setTypeCountsError(null);
    try {
      const url = `${import.meta.env.VITE_API_URL}/api/queue/types?dedupe=${dedupe ? 'true' : 'false'}`;
      const res = await authFetch(url);
      if (!res.ok) {
        const txt = await res.text().catch(() => '');
        throw new Error(`${res.status} ${res.statusText}: ${txt}`);
      }
      const data = await res.json().catch(() => ({}));

      let types: TypeCount[] = [];
      if (Array.isArray(data.types)) {
        types = data.types.map((t: any) => ({ message_type: t.message_type || t.type || t.name || 'unknown', count: Number(t.count || t.cnt || 0) }));
      } else if (data.counts && typeof data.counts === 'object') {
        types = Object.entries(data.counts).map(([k, v]) => ({ message_type: k, count: Number(v) }));
      } else if (Array.isArray(data)) {
        types = data.map((t: any) => ({ message_type: t.message_type || t.type || t.name || 'unknown', count: Number(t.count || t.cnt || 0) }));
      }

      setTypeCounts(types);
    } catch (e: any) {
      console.error('Type counts fetch error', e);
      setTypeCountsError(e.message || 'Failed to load type counts');
    } finally {
      setTypeCountsLoading(false);
    }
  };

  // Load type counts when the modal opens
  useEffect(() => {
    if (typesOpen) {
      fetchTypeCounts();
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [typesOpen, dedupe]);

  // Filter out finalized contacts - search is now backend-driven
  const filteredRows = rows.filter((r: QueueRow) => {
    // Exclude finalized status
    if (r.current_status === 'finalized') {
      return false;
    }
    return true;
  });

  // Display all results (no client-side filtering, backend handles search)
  const displayRows = filteredRows;

  if (loading) return <div className="p-6">Loading...</div>;
  if (error) return <div className="p-6 text-red-600">Error: {error}</div>;

  return (
    <div className="p-6">
      <div>
        <h1 className="text-2xl font-bold mb-1">Email Queue</h1>
        <p className="text-sm text-gray-600 mb-4"></p>
      </div>


      <div className="mb-4">
        <div className="flex items-center space-x-4 mb-3">
          {/* New overview icon buttons using Radix DropdownMenu (shadcn-style) */}
          <DropdownMenu.Root>
            <DropdownMenu.Trigger asChild>
              <button className="flex items-center space-x-2 px-3 py-2 border rounded">
                <Inbox size={16} />
                <span>Initial</span>
                <span className="text-sm text-gray-600">{overview ? overview.initial?.total ?? 0 : 'â€”'}</span>
              </button>
            </DropdownMenu.Trigger>
            <DropdownMenu.Content align="start" sideOffset={6} className="bg-white border rounded shadow p-2 z-50 w-56">
              <div className="text-sm font-semibold mb-2">Initial breakdown</div>
              {overviewLoading && <div>Loading...</div>}
              {overviewError && <div className="text-red-600">{overviewError}</div>}
              {!overviewLoading && !overviewError && overview && (
                <div className="space-y-1 text-sm">
                  {renderBreakdown('initial', INITIAL_KEYS)}
                </div>
              )}
            </DropdownMenu.Content>
          </DropdownMenu.Root>

          <DropdownMenu.Root>
            <DropdownMenu.Trigger asChild>
              <button className="flex items-center space-x-2 px-3 py-2 border rounded">
                <FileText size={16} />
                <span>Forms</span>
                <span className="text-sm text-gray-600">{overview ? overview.forms?.total ?? 0 : 'â€”'}</span>
              </button>
            </DropdownMenu.Trigger>
            <DropdownMenu.Content align="start" sideOffset={6} className="bg-white border rounded shadow p-2 z-50 w-56">
              <div className="text-sm font-semibold mb-2">Forms breakdown</div>
              {overviewLoading && <div>Loading...</div>}
              {overviewError && <div className="text-red-600">{overviewError}</div>}
              {!overviewLoading && !overviewError && overview && (
                <div className="space-y-1 text-sm">
                  {renderBreakdown('forms', FORMS_KEYS)}
                </div>
              )}
            </DropdownMenu.Content>
          </DropdownMenu.Root>

          <DropdownMenu.Root>
            <DropdownMenu.Trigger asChild>
              <button className="flex items-center space-x-2 px-3 py-2 border rounded">
                <CreditCard size={16} />
                <span>Payments</span>
                <span className="text-sm text-gray-600">{overview ? overview.payments?.total ?? 0 : 'â€”'}</span>
              </button>
            </DropdownMenu.Trigger>
            <DropdownMenu.Content align="start" sideOffset={6} className="bg-white border rounded shadow p-2 z-50 w-56">
              <div className="text-sm font-semibold mb-2">Payments breakdown</div>
              {overviewLoading && <div>Loading...</div>}
              {overviewError && <div className="text-red-600">{overviewError}</div>}
              {!overviewLoading && !overviewError && overview && (
                <div className="space-y-1 text-sm">
                  {renderBreakdown('payments', PAYMENTS_KEYS)}
                </div>
              )}
            </DropdownMenu.Content>
          </DropdownMenu.Root>

          <DropdownMenu.Root>
            <DropdownMenu.Trigger asChild>
              <button className="flex items-center space-x-2 px-3 py-2 border rounded">
                <Layers size={16} />
                <span>Custom Flow</span>
                <span className="text-sm text-gray-600">{overview ? overview.custom_flow?.total ?? 0 : 'â€”'}</span>
              </button>
            </DropdownMenu.Trigger>
            <DropdownMenu.Content align="start" sideOffset={6} className="bg-white border rounded shadow p-2 z-50 w-56">
              <div className="text-sm font-semibold mb-2">Custom Flow breakdown</div>
              {overviewLoading && <div>Loading...</div>}
              {overviewError && <div className="text-red-600">{overviewError}</div>}
              {!overviewLoading && !overviewError && overview && (
                <div className="space-y-1 text-sm">
                  {Object.entries(overview.custom_flow).map(([k, v]) => (
                    <div key={k} className="flex justify-between">
                      <div className="capitalize">{k.replace('_', ' ')}</div>
                      <div className="text-gray-700">{v}</div>
                    </div>
                  ))}
                </div>
              )}
            </DropdownMenu.Content>
          </DropdownMenu.Root>

          {/* Errors overview */}
          <DropdownMenu.Root>
            <DropdownMenu.Trigger asChild>
              <button className="flex items-center space-x-2 px-3 py-2 border rounded">
                <AlertTriangle size={16} />
                <span>Errors</span>
                <span className="text-sm text-gray-600">{overview ? (overview.errors?.contacts ?? overview.errors?.total_items ?? 0) : 'â€”'}</span>
              </button>
            </DropdownMenu.Trigger>
            <DropdownMenu.Content align="start" sideOffset={6} className="bg-white border rounded shadow p-2 z-50 w-64">
              <div className="text-sm font-semibold mb-2">Error sending breakdown</div>
              {overviewLoading && <div>Loading...</div>}
              {overviewError && <div className="text-red-600">{overviewError}</div>}
              {!overviewLoading && !overviewError && overview && (
                <div className="space-y-2 text-sm">
                  <div className="flex justify-between font-medium">
                    <div>Total failed items</div>
                    <div className="text-gray-700">{overview.errors?.total_items ?? 0}</div>
                  </div>
                  <div className="flex justify-between">
                    <div>Distinct contacts</div>
                    <div className="text-gray-700">{overview.errors?.contacts ?? 0}</div>
                  </div>
                  <div className="mt-2 text-xs text-gray-500">Top error messages</div>
                  <div className="space-y-1 max-h-40 overflow-auto">
                    {overview.errors?.by_error_message && Object.entries(overview.errors.by_error_message).length > 0 ? (
                      Object.entries(overview.errors.by_error_message).map(([msg, cnt]) => (
                        <div key={msg} className="flex justify-between">
                          <div className="truncate pr-2" title={msg}>{msg}</div>
                          <div className="text-gray-700 ml-2">{cnt}</div>
                        </div>
                      ))
                    ) : (
                      <div className="text-sm text-gray-500">No errors reported</div>
                    )}
                  </div>
                </div>
              )}
            </DropdownMenu.Content>
          </DropdownMenu.Root>
        </div>

        <div className="flex items-center justify-between">
          <div className="flex items-center space-x-2">
            <input 
              value={query} 
              onChange={(e: React.ChangeEvent<HTMLInputElement>) => { 
                const newQuery = e.target.value;
                setQuery(newQuery);
                
                // Clear previous timeout
                if (searchTimeoutRef.current) {
                  clearTimeout(searchTimeoutRef.current);
                }
                
                // Debounce backend search by 500ms
                searchTimeoutRef.current = setTimeout(() => {
                  // Trigger backend search with new query
                  fetchQueue(newQuery, null);
                }, 500);
              }} 
              placeholder="Search name, email, stage, status... (searches all data)" 
              className="w-[50vw] px-4 py-2 border rounded" 
            />
            <button 
              onClick={() => { 
                // Clear search and fetch all
                setQuery('');
                if (searchTimeoutRef.current) {
                  clearTimeout(searchTimeoutRef.current);
                }
                fetchQueue('', null);
              }} 
              className="px-3 py-2 bg-gray-100 rounded hover:bg-gray-200"
            >
              Clear
            </button>
          </div>
          <div className="text-sm text-gray-600"> {displayRows.length} </div>
        </div>
      </div>

      <div className="overflow-x-auto bg-white rounded shadow">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Event ID</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Contact</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Stage</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Status</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Next Action</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Sender Email</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Queue Created</th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {displayRows.map((r: QueueRow) => (
              <tr key={r.contact_id} className={r.send_failed ? 'bg-red-50' : ''} onClick={() => { setFlowContact({ contact_id: r.contact_id, name: r.name, email: r.email }); setFlowOpen(true); fetchContactFlow(r.contact_id); }} style={{ cursor: 'pointer' }}>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">{r.event_id || '-'}</td>
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="text-sm font-medium text-gray-900">{r.name}</div>
                  <div className="text-sm text-gray-500">{r.email}</div>
                </td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">{r.current_stage}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">{r.current_status}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-600 font-medium">{getNextAction(r.current_status, r.current_stage, (r.next_action || (r as any).queue_type || r.current_stage))}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">{r.sender_email || '-'}</td>
                <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">{r.queue_created_at ? format(new Date(r.queue_created_at), 'MMM dd, yyyy HH:mm') : '-'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Flow Modal */}
      {flowOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black bg-opacity-40">
          <div className="bg-white w-11/12 max-w-3xl rounded shadow-lg overflow-auto max-h-[80vh]">
            <div className="p-4 border-b flex items-center justify-between">
              <div>
                <div className="text-lg font-semibold">Message History</div>
                <div className="text-sm text-gray-600">{flowContact?.name} â€” {flowContact?.email}</div>
              </div>
              <div>
                <button onClick={() => { setFlowOpen(false); setFlowSteps([]); setFlowContact(null); }} className="px-3 py-1 border rounded">Close</button>
              </div>
            </div>
            <div className="p-4">
              {flowLoading && <div>Loading messages...</div>}
              {flowError && <div className="text-red-600">Error: {flowError}</div>}
              {!flowLoading && !flowError && (
                <div className="space-y-3">
                  {flowSteps.length === 0 && <div className="text-sm text-gray-500">No messages found for this contact.</div>}
                  {flowSteps.map((m: any, idx: number) => (
                    <div key={m.id || idx} className="p-3 border rounded bg-white">
                      <div className="flex items-center justify-between">
                        <div className="font-medium">{m.direction === 'sent' ? 'ðŸ“¤ Sent' : 'ðŸ“¥ Received'}</div>
                        <div className="text-sm text-gray-600">{m.stage || '-'}</div>
                      </div>
                      <div className="text-sm text-gray-600 mt-1">
                        From: {m.sender_email} â†’ To: {m.recipient_email}
                      </div>
                      <div className="text-sm text-gray-600 mt-1">
                        {m.sent_at ? `Sent: ${format(new Date(m.sent_at), 'PPpp')}` : (m.received_at ? `Received: ${format(new Date(m.received_at), 'PPpp')}` : '-')}
                      </div>
                      <div className="mt-2 text-sm"><strong>Subject:</strong> {m.subject || '(no subject)'}</div>
                      <div className="mt-2 text-sm text-gray-800 border-l-2 border-gray-300 pl-3">
                        {m.body ? m.body.substring(0, 500) + (m.body.length > 500 ? '...' : '') : '(no body)'}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Types Modal */}
      {typesOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black bg-opacity-40">
          <div className="bg-white w-11/12 max-w-2xl rounded shadow-lg overflow-auto max-h-[80vh]">
            <div className="p-4 border-b flex items-center justify-between">
              <div>
                <div className="text-lg font-semibold">Message Type Counts</div>
                <div className="text-sm text-gray-600">Grouped by queue type (dedupe: {dedupe ? 'on' : 'off'})</div>
              </div>
              <div>
                <button onClick={() => { setTypesOpen(false); setTypeCounts([]); }} className="px-3 py-1 border rounded">Close</button>
              </div>
            </div>
            <div className="p-4">
              {typeCountsLoading && <div>Loading...</div>}
              {typeCountsError && <div className="text-red-600">Error: {typeCountsError}</div>}
              {!typeCountsLoading && !typeCountsError && (
                <div className="space-y-2">
                  {typeCounts.length === 0 && <div className="text-sm text-gray-500">No types found.</div>}
                  {typeCounts.map((t: TypeCount) => (
                    <div key={t.message_type} className="p-2 border rounded flex items-center justify-between">
                      <div className="font-medium">{t.message_type}</div>
                      <div className="text-sm text-gray-700">{t.count}</div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Pagination removed - showing single page with up to 1000 contacts */}
    </div>
  );
}

export default QueuePage;

