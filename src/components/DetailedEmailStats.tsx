import React, { useState, useEffect } from 'react';
import { Mail, Send, Reply, Clock, AlertCircle, CheckCircle, User } from 'lucide-react';
import { format } from 'date-fns';

interface EmailSummary {
  sent: number;
  failed: number;
  pending: number;
  received_replies: number;
}

interface EmailRecord {
  from: string;
  to: string;
  subject: string;
  timestamp: string;
  status: string;
  contact_stage?: string;
  contact_status?: string;
  next_action?: [string, string] | null; // [action, iso_timestamp]
  type: 'sent' | 'received';
}

interface SenderBreakdown {
  sender_email: string;
  sent_count: number;
  failed_count: number;
}

interface EmailStatsData {
  last_24_hours: {
    summary: EmailSummary;
    sent_emails: EmailRecord[];
    received_emails: EmailRecord[];
    sender_breakdown: SenderBreakdown[];
  bounced: any[];
  };
  all_previous_days: {
    summary: EmailSummary;
    sent_emails: EmailRecord[];
    received_emails: EmailRecord[];
    sender_breakdown: SenderBreakdown[];
  bounced: any[];
  };
  generated_at: string;
}

interface DetailedEmailStatsProps {
  token: string;
}

// Provide a permissive JSX.IntrinsicElements declaration when React/TSX types are not available
declare global {
  namespace JSX {
    interface IntrinsicElements {
      [elemName: string]: any;
    }
  }
}



export function DetailedEmailStats({ token }: DetailedEmailStatsProps) {
  const [emailStats, setEmailStats] = useState<EmailStatsData | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<'last24h' | 'previous'>('last24h');
  const [activeSection, setActiveSection] = useState<'summary' | 'sent' | 'received' | 'senders' | 'failed' | 'bounced'>('summary');
  const [page, setPage] = useState(1);
  const pageSize = 100;
  const [searchQuery, setSearchQuery] = useState('');
  const [failedSends, setFailedSends] = useState<any[]>([]);
  const [failedPage, setFailedPage] = useState(1);
  const [failedTotal, setFailedTotal] = useState(0);

  const fetchEmailStats = async () => {
    try {
      setLoading(true);
      const response = await fetch(`/api/monitoring/detailed_email_stats`, {
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`,
        },
      });

      if (response.status === 401) {
        throw new Error('Authentication failed - please refresh and login again');
      }

      if (!response.ok) {
        throw new Error(`Failed to fetch email statistics: ${response.statusText}`);
      }

      const data = await response.json();
      setEmailStats(data);
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unknown error occurred');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchEmailStats();
    // Refresh every 30 seconds
    const interval = setInterval(fetchEmailStats, 30000);
    return () => clearInterval(interval);
  }, [token]);

  useEffect(() => {
    // fetch failed sends for the failed tab (paginated)
    const fetchFailed = async () => {
      try {
        const q = searchQuery ? `&q=${encodeURIComponent(searchQuery)}` : '';
        const res = await fetch(`/api/monitoring/failed_sends?page=${failedPage}&page_size=50${q}`, {
          headers: {
            'Content-Type': 'application/json',
            'Authorization': `Bearer ${token}`,
          },
        });
        if (!res.ok) {
          console.error('Failed to fetch failed sends', res.statusText);
          setFailedSends([]);
          setFailedTotal(0);
          return;
        }
        const json = await res.json();
        setFailedSends(json.results || []);
        setFailedTotal(json.total || 0);
      } catch (err) {
        console.error('Error fetching failed sends', err);
        setFailedSends([]);
        setFailedTotal(0);
      }
    };

    fetchFailed();
  }, [failedPage, searchQuery, token]);

  const formatTimestamp = (timestamp: string) => {
    if (!timestamp || timestamp === 'null' || timestamp === 'undefined') {
      return 'Never';
    }
    try {
      const date = new Date(timestamp);
      if (isNaN(date.getTime())) {
        console.warn('Invalid timestamp detected in DetailedEmailStats:', timestamp);
        return 'Invalid Date';
      }
      return format(date, 'MMM dd, yyyy HH:mm:ss');
    } catch (error) {
      console.error('Error formatting timestamp in DetailedEmailStats:', timestamp, error);
      return timestamp;
    }
  };

  const getStatusIcon = (status: string) => {
    switch (status.toLowerCase()) {
      case 'sent':
        return <CheckCircle className="w-4 h-4 text-green-500" />;
      case 'failed':
        return <AlertCircle className="w-4 h-4 text-red-500" />;
      case 'pending':
        return <Clock className="w-4 h-4 text-yellow-500" />;
      case 'replied':
        return <Reply className="w-4 h-4 text-blue-500" />;
      default:
        return <Mail className="w-4 h-4 text-gray-500" />;
    }
  };

  const renderSummaryCards = (summary: EmailSummary) => (
    <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-6">
      <div className="bg-green-50 p-4 rounded-lg border border-green-200">
        <div className="flex items-center">
          <Send className="w-8 h-8 text-green-600 mr-3" />
          <div>
            <p className="text-sm font-medium text-green-600">Sent</p>
            <p className="text-2xl font-bold text-green-700">{summary.sent}</p>
          </div>
        </div>
      </div>
      <div className="bg-blue-50 p-4 rounded-lg border border-blue-200">
        <div className="flex items-center">
          <Reply className="w-8 h-8 text-blue-600 mr-3" />
          <div>
            <p className="text-sm font-medium text-blue-600">Received Replies</p>
            <p className="text-2xl font-bold text-blue-700">{summary.received_replies}</p>
          </div>
        </div>
      </div>
      <div className="bg-red-50 p-4 rounded-lg border border-red-200">
        <div className="flex items-center">
          <AlertCircle className="w-8 h-8 text-red-600 mr-3" />
          <div>
            <p className="text-sm font-medium text-red-600">Failed</p>
            <p className="text-2xl font-bold text-red-700">{summary.failed}</p>
          </div>
        </div>
      </div>
      <div className="bg-yellow-50 p-4 rounded-lg border border-yellow-200">
        <div className="flex items-center">
          <Clock className="w-8 h-8 text-yellow-600 mr-3" />
          <div>
            <p className="text-sm font-medium text-yellow-600">Pending</p>
            <p className="text-2xl font-bold text-yellow-700">{summary.pending}</p>
          </div>
        </div>
      </div>
    </div>
  );

  const renderEmailTable = (emails: EmailRecord[], title: string) => (
    <div className="bg-white rounded-lg shadow overflow-hidden mb-6">
      <div className="px-6 py-4 border-b border-gray-200">
        <h3 className="text-lg font-medium text-gray-900">{title}</h3>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Status
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                From
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                To
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Subject
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Meta
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Timestamp
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {emails.length === 0 ? (
        <tr>
          <td colSpan={6} className="px-6 py-4 text-center text-gray-500">
                  No emails found
                </td>
              </tr>
            ) : (
              emails.map((email, index) => (
                <tr key={index} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap">
                    <div className="flex items-center">
                      {getStatusIcon(email.status)}
                      <span className="ml-2 text-sm text-gray-900 capitalize">{email.status}</span>
                    </div>
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {email.from}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                    {email.to}
                  </td>
                  <td className="px-6 py-4 text-sm text-gray-900 max-w-xs truncate" title={email.subject}>
                    {email.subject}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {email.contact_stage ? <span className="text-xs text-gray-600">{email.contact_stage} / {email.contact_status}</span> : <span className="text-xs text-gray-400">-</span>}
                    {email.next_action ? (
                      <div className="text-xs text-gray-700">Next: {email.next_action[0]} at {format(new Date(email.next_action[1]), 'MMM dd, yyyy HH:mm')}</div>
                    ) : null}
                  </td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                    {formatTimestamp(email.timestamp)}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );

  const renderSenderBreakdown = (senders: SenderBreakdown[]) => (
    <div className="bg-white rounded-lg shadow overflow-hidden mb-6">
      <div className="px-6 py-4 border-b border-gray-200">
        <h3 className="text-lg font-medium text-gray-900">Sender Breakdown</h3>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Sender Email
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Sent
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Failed
              </th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                Success Rate
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {senders.length === 0 ? (
              <tr>
                <td colSpan={4} className="px-6 py-4 text-center text-gray-500">
                  No sender data found
                </td>
              </tr>
            ) : (
              senders.map((sender, index) => {
                const total = sender.sent_count + sender.failed_count;
                const successRate = total > 0 ? ((sender.sent_count / total) * 100).toFixed(1) : '0';
                return (
                  <tr key={index} className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="flex items-center">
                        <User className="w-4 h-4 text-gray-400 mr-2" />
                        <span className="text-sm text-gray-900">{sender.sender_email}</span>
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-green-600 font-medium">
                      {sender.sent_count}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-red-600 font-medium">
                      {sender.failed_count}
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                      <div className="flex items-center">
                        <div className="w-16 bg-gray-200 rounded-full h-2 mr-2">
                          <div
                            className="bg-green-500 h-2 rounded-full"
                            style={{ width: `${successRate}%` }}
                          ></div>
                        </div>
                        <span>{successRate}%</span>
                      </div>
                    </td>
                  </tr>
                );
              })
            )}
          </tbody>
        </table>
      </div>
    </div>
  );

  const renderBounced = (bounced: any[]) => (
    <div className="bg-white rounded-lg shadow overflow-hidden mb-6">
      <div className="px-6 py-4 border-b border-gray-200">
        <h3 className="text-lg font-medium text-gray-900">Bounced Emails</h3>
      </div>
      <div className="overflow-x-auto">
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Email</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Bounce Type</th>
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Timestamp</th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {bounced.length === 0 ? (
              <tr><td colSpan={3} className="px-6 py-4 text-center text-gray-500">No bounces</td></tr>
            ) : (
              bounced.slice((page-1)*pageSize, page*pageSize).map((b, i) => (
                <tr key={i} className="hover:bg-gray-50">
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{b.email}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{b.bounce_type}</td>
                  <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{formatTimestamp(b.timestamp)}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
      {bounced.length > pageSize && (
        <div className="p-4 flex justify-between items-center">
          <button className="px-3 py-1 bg-gray-200 rounded" onClick={() => setPage(Math.max(1, page-1))}>Prev</button>
          <div className="text-sm text-gray-600">Page {page} of {Math.ceil(bounced.length / pageSize)}</div>
          <button className="px-3 py-1 bg-gray-200 rounded" onClick={() => setPage(Math.min(Math.ceil(bounced.length / pageSize), page+1))}>Next</button>
        </div>
      )}
    </div>
  );

  if (loading) {
    return (
      <div className="flex items-center justify-center p-8">
        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600"></div>
        <span className="ml-2 text-gray-600">Loading email statistics...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-red-50 border border-red-200 rounded-lg p-4">
        <div className="flex items-center">
          <AlertCircle className="w-5 h-5 text-red-500 mr-2" />
          <span className="text-red-700">Error loading email statistics: {error}</span>
        </div>
        <button
          onClick={fetchEmailStats}
          className="mt-2 px-4 py-2 bg-red-600 text-white rounded hover:bg-red-700"
        >
          Retry
        </button>
      </div>
    );
  }

  if (!emailStats) {
    return (
      <div className="text-center p-8 text-gray-500">
        No email statistics available
      </div>
    );
  }

  const currentData = activeTab === 'last24h' ? emailStats.last_24_hours : emailStats.all_previous_days;

  // Search matching helper
  const matchesQuery = (item: any, q: string) => {
    if (!q) return true;
    const lq = q.toLowerCase();
    // EmailRecord fields
    const from = (item.from || '').toString().toLowerCase();
    const to = (item.to || '').toString().toLowerCase();
    const subject = (item.subject || '').toString().toLowerCase();
    const stage = (item.contact_stage || '').toString().toLowerCase();
    const status = (item.contact_status || '').toString().toLowerCase();
    if (from.includes(lq) || to.includes(lq) || subject.includes(lq) || stage.includes(lq) || status.includes(lq)) {
      return true;
    }
    // If bounced item
    const email = (item.email || '').toString().toLowerCase();
    const bounceType = (item.bounce_type || '').toString().toLowerCase();
    if (email.includes(lq) || bounceType.includes(lq)) return true;
    return false;
  };

  const filteredSent = currentData.sent_emails.filter((e: EmailRecord) => matchesQuery(e, searchQuery));
  const filteredReceived = currentData.received_emails.filter((e: EmailRecord) => matchesQuery(e, searchQuery));
  const filteredBounced = (currentData.bounced || []).filter((b: any) => matchesQuery(b, searchQuery));

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-bold text-gray-900">📧 Detailed Email Statistics</h2>
        <div className="text-sm text-gray-500">
          Last updated: {formatTimestamp(emailStats.generated_at)}
        </div>
      </div>

      {/* Search bar */}
      <div className="flex items-center">
        <input
          type="text"
          placeholder="Search by email, subject, stage, or status..."
          className="w-full md:w-1/2 px-3 py-2 border rounded mr-2"
          value={searchQuery}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) => { setSearchQuery(e.target.value); setPage(1); }}
        />
        <button
          className="px-3 py-2 bg-gray-200 rounded"
          onClick={() => { setSearchQuery(''); setPage(1); }}
        >
          Clear
        </button>
      </div>

      {/* Time Period Tabs */}
      <div className="border-b border-gray-200">
        <nav className="-mb-px flex space-x-8">
          <button
            onClick={() => setActiveTab('last24h')}
            className={`py-2 px-1 border-b-2 font-medium text-sm ${
              activeTab === 'last24h'
                ? 'border-blue-500 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Last 24 Hours
          </button>
          <button
            onClick={() => setActiveTab('previous')}
            className={`py-2 px-1 border-b-2 font-medium text-sm ${
              activeTab === 'previous'
                ? 'border-blue-500 text-blue-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            All Previous Days
          </button>
        </nav>
      </div>

      {/* Section Tabs */}
      <div className="border-b border-gray-200">
        <nav className="-mb-px flex space-x-8">
          <button
            onClick={() => setActiveSection('summary')}
            className={`py-2 px-1 border-b-2 font-medium text-sm ${
              activeSection === 'summary'
                ? 'border-green-500 text-green-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Summary
          </button>
            <button
            onClick={() => setActiveSection('sent')}
            className={`py-2 px-1 border-b-2 font-medium text-sm ${
              activeSection === 'sent'
                ? 'border-green-500 text-green-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Sent Emails ({filteredSent.length})
          </button>
          <button
            onClick={() => setActiveSection('received')}
            className={`py-2 px-1 border-b-2 font-medium text-sm ${
              activeSection === 'received'
                ? 'border-green-500 text-green-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Received Replies ({filteredReceived.length})
          </button>
          <button
            onClick={() => setActiveSection('senders')}
            className={`py-2 px-1 border-b-2 font-medium text-sm ${
              activeSection === 'senders'
                ? 'border-green-500 text-green-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Sender Breakdown ({currentData.sender_breakdown.length})
          </button>
          <button
            onClick={() => setActiveSection('failed')}
            className={`py-2 px-1 border-b-2 font-medium text-sm ${
              activeSection === 'failed'
                ? 'border-red-500 text-red-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Failed ({failedTotal || currentData.summary.failed})
          </button>
          <button
            onClick={() => setActiveSection('bounced')}
            className={`py-2 px-1 border-b-2 font-medium text-sm ${
              activeSection === 'bounced'
                ? 'border-green-500 text-green-600'
                : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
            }`}
          >
            Bounced ({filteredBounced.length})
          </button>
        </nav>
      </div>

      {/* Content */}
      <div>
  {activeSection === 'summary' && renderSummaryCards(currentData.summary)}
  {activeSection === 'sent' && renderEmailTable(filteredSent, 'Sent Emails')}
  {activeSection === 'received' && renderEmailTable(filteredReceived, 'Received Replies')}
  {activeSection === 'senders' && renderSenderBreakdown(currentData.sender_breakdown)}
  {activeSection === 'failed' && (
    <div>
      <div className="bg-white rounded-lg shadow overflow-hidden mb-6">
        <div className="px-6 py-4 border-b border-gray-200">
          <h3 className="text-lg font-medium text-gray-900">Failed Sends</h3>
        </div>
        <div className="overflow-x-auto">
          <table className="min-w-full divide-y divide-gray-200">
            <thead className="bg-gray-50">
              <tr>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Contact</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Email</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Subject</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Error</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">When</th>
                <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">Retries</th>
              </tr>
            </thead>
            <tbody className="bg-white divide-y divide-gray-200">
              {failedSends.length === 0 ? (
                <tr><td colSpan={6} className="px-6 py-4 text-center text-gray-500">No failed sends</td></tr>
              ) : (
                failedSends.map((f: any) => (
                  <tr key={f.queue_id} className="hover:bg-gray-50">
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{f.contact_name || '(unknown)'}</td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{f.contact_email || f.to}</td>
                    <td className="px-6 py-4 text-sm text-gray-900 max-w-xs truncate" title={f.subject}>{f.subject}</td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-red-600">{f.error_message}</td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">{f.failed_at ? format(new Date(f.failed_at), 'MMM dd, yyyy HH:mm:ss') : ''}</td>
                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">{f.retry_count}</td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
        <div className="p-4 flex justify-between items-center">
          <button className="px-3 py-1 bg-gray-200 rounded" onClick={() => setFailedPage(Math.max(1, failedPage-1))} disabled={failedPage === 1}>Prev</button>
          <div className="text-sm text-gray-600">Page {failedPage} of {Math.max(1, Math.ceil(failedTotal / 50))}</div>
          <button className="px-3 py-1 bg-gray-200 rounded" onClick={() => setFailedPage((p: number) => p + 1)} disabled={failedPage * 50 >= failedTotal}>Next</button>
        </div>
      </div>
    </div>
  )}
  {activeSection === 'bounced' && renderBounced(filteredBounced)}
      </div>
    </div>
  );
}
