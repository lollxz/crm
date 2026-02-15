import React, { useState, useEffect } from 'react';
import { DetailedEmailStats } from './DetailedEmailStats';

// Enhanced monitoring dashboard with comprehensive features
interface WorkerStatus {
  name: string;
  status: string;
  last_heartbeat: string;
  error_count: number;
  last_error: string | null;
  cpu_percent?: number;
  memory_percent?: number;
  memory_mb?: number;
  uptime?: number;
}

interface EmailMetrics {
  sent_today: number;
  failed_today: number;
  queued_now: number;
  avg_delivery_time: number;
  success_rate?: number;
  failure_rate?: number;
}

interface ScheduledTask {
  name: string;
  next_run: string;
  last_run: string;
  status: string;
  duration_seconds: number;
  success_rate: number;
}

interface ErrorAnalytics {
  total_errors_24h: number;
  rate_limit_warnings: number;
  auth_failures: number;
  error_trend: string;
}

interface UserActivityLog {
  id: number;
  username: string;
  action_type: string;
  action_description: string;
  target_type: string | null;
  target_id: number | null;
  target_name: string | null;
  old_values: any;
  new_values: any;
  ip_address: string | null;
  user_agent: string | null;
  timestamp: string;
}

function safeToFixed(value: any, digits: number, fallback: string = '0') {
  return (typeof value === 'number' && isFinite(value)) ? value.toFixed(digits) : fallback;
}

export function EnhancedDashboard() {
  const [isAuthenticated, setIsAuthenticated] = useState(false);
  const [token, setToken] = useState<string | null>(null);
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [loginError, setLoginError] = useState('');
  const [loading, setLoading] = useState(false);
  const [activeTab, setActiveTab] = useState('workers');
  const [lastUpdate, setLastUpdate] = useState<Date>(new Date());
  const [autoRefresh, setAutoRefresh] = useState(true);

  // Enhanced data states
  const [workers, setWorkers] = useState<WorkerStatus[]>([]);
  const [emailMetrics, setEmailMetrics] = useState<EmailMetrics | null>(null);
  const [scheduledTasks, setScheduledTasks] = useState<ScheduledTask[]>([]);
  const [errorAnalytics, setErrorAnalytics] = useState<ErrorAnalytics | null>(null);
  const [failedEmails, setFailedEmails] = useState<any[]>([]);
  const [userActivityLogs, setUserActivityLogs] = useState<UserActivityLog[]>([]);
  const [activityLoading, setActivityLoading] = useState(false);
  const [activityFilters, setActivityFilters] = useState({
    action_type: '',
    username: '',
    date_from: '',
    date_to: ''
  });
  
  // Dynamic columns state
  const [standardColumns, setStandardColumns] = useState<any[]>([]);
  const [dynamicColumns, setDynamicColumns] = useState<any[]>([]);
  const [newColumnName, setNewColumnName] = useState('');
  const [newColumnType, setNewColumnType] = useState('TEXT');

  // Email queue view state
  const [emailQueueResults, setEmailQueueResults] = useState<any[]>([]);
  const [emailQueuePage, setEmailQueuePage] = useState(1);
  const [emailQueuePageSize, setEmailQueuePageSize] = useState(50);
  const [emailQueueTotal, setEmailQueueTotal] = useState(0);
  const [emailQueueLoading, setEmailQueueLoading] = useState(false);
  const [emailQueueSearch, setEmailQueueSearch] = useState('');

  // Check for existing token on mount
  useEffect(() => {
    const savedToken = localStorage.getItem('monitoring_token');
    if (savedToken) {
      setToken(savedToken);
      setIsAuthenticated(true);
    }
  }, []);

  // Auto-refresh data every 30 seconds (disabled for activity tab)
  useEffect(() => {
    if (isAuthenticated && autoRefresh && activeTab !== 'activity') {
      const interval = setInterval(() => {
        try {
          fetchDashboardData();
        } catch (error) {
          console.error('Error in auto-refresh:', error);
        }
      }, 30000);
      return () => clearInterval(interval);
    }
  }, [isAuthenticated, autoRefresh, activeTab]);

  // Initial data fetch when authenticated
  useEffect(() => {
    if (isAuthenticated) {
      fetchDashboardData();
    }
  }, [isAuthenticated]);

  // Fetch user activity logs when activity tab is selected
  useEffect(() => {
    if (isAuthenticated && activeTab === 'activity') {
      console.log('Activity tab selected, fetching logs...');
      try {
        fetchUserActivityLogs();
      } catch (error) {
        console.error('Error in useEffect for activity logs:', error);
      }
    }
  }, [isAuthenticated, activeTab]);
  
  // Fetch dynamic columns when dynamic-columns tab is selected
  useEffect(() => {
    if (isAuthenticated && activeTab === 'dynamic-columns') {
      console.log('Dynamic columns tab selected, fetching columns...');
      try {
        fetchDynamicColumns();
      } catch (error) {
        console.error('Error in useEffect for dynamic columns:', error);
      }
    }
  }, [isAuthenticated, activeTab]);

  // API helper with proper error handling
  const apiCall = async (endpoint: string, options: RequestInit = {}) => {
    const response = await fetch(`/api/monitoring${endpoint}`, {
      ...options,
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`,
        ...options.headers,
      },
    });

    if (response.status === 401) {
      // Token expired
      setIsAuthenticated(false);
      setToken(null);
      localStorage.removeItem('monitoring_token');
      throw new Error('Session expired');
    }

    if (!response.ok) {
      throw new Error(`API call failed: ${response.statusText}`);
    }

    return response.json();
  };

  // User Activity API call with authentication
  const fetchUserActivityLogs = async (filters = activityFilters) => {
    if (!token) return;

    setActivityLoading(true);
    try {
      const params = new URLSearchParams();
      if (filters.action_type) params.append('action_type', filters.action_type);
      if (filters.username) params.append('username', filters.username);
      if (filters.date_from) params.append('date_from', filters.date_from);
      if (filters.date_to) params.append('date_to', filters.date_to);
      params.append('limit', '50');

      const response = await fetch(`/api/user-activity-logs?${params}`, {
        headers: {
          'Authorization': `Bearer ${token}`,
        },
      });

      if (response.status === 401) {
        setIsAuthenticated(false);
        setToken(null);
        localStorage.removeItem('monitoring_token');
        throw new Error('Session expired');
      }

      if (!response.ok) {
        throw new Error(`Failed to fetch user activity logs: ${response.statusText}`);
      }

      const data = await response.json();
      console.log('User activity logs data:', data); // Debug log
      
      // Validate and clean the logs data
      const validLogs = (data.logs || []).map((log: any) => ({
        ...log,
        timestamp: log.timestamp || new Date().toISOString(), // Fallback timestamp
        action_type: log.action_type || 'UNKNOWN',
        action_description: log.action_description || 'No description',
        username: log.username || 'Unknown user'
      }));
      
      setUserActivityLogs(validLogs);
    } catch (error: any) {
      console.error('Failed to fetch user activity logs:', error);
      setUserActivityLogs([]); // Set empty array on error
    } finally {
      setActivityLoading(false);
    }
  };

  // Dynamic columns management functions
  const fetchDynamicColumns = async () => {
    if (!token) return;
    
    try {
      const response = await fetch(`${import.meta.env.VITE_API_URL}/admin/dynamic-columns`, {
        headers: {
          'Authorization': `Bearer ${token}`,
        },
      });

      if (!response.ok) {
        throw new Error(`Failed to fetch dynamic columns: ${response.statusText}`);
      }

      const data = await response.json();
      setStandardColumns(data.standard_columns || []);
      setDynamicColumns(data.dynamic_columns || []);
    } catch (error: any) {
      console.error('Failed to fetch dynamic columns:', error);
    }
  };

  const addDynamicColumn = async () => {
    if (!token || !newColumnName.trim()) return;
    
    try {
      const response = await fetch(`${import.meta.env.VITE_API_URL}/admin/dynamic-columns`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`,
        },
        body: JSON.stringify({
          column_name: newColumnName.trim(),
          data_type: newColumnType,
          is_nullable: true
        }),
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to add column');
      }

      const result = await response.json();
      console.log('Column added successfully:', result);
      
      // Reset form and refresh columns
      setNewColumnName('');
      setNewColumnType('TEXT');
      await fetchDynamicColumns();
      
      // Show success message (you can add a toast notification here)
      alert(`Column '${result.column_name}' added successfully!`);
      
    } catch (error: any) {
      console.error('Failed to add dynamic column:', error);
      alert(`Error: ${error.message}`);
    }
  };

  const deleteDynamicColumn = async (columnName: string) => {
    if (!token || !confirm(`Are you sure you want to delete column '${columnName}'? This will also remove all data in that column.`)) return;
    
    try {
      const response = await fetch(`${import.meta.env.VITE_API_URL}/admin/dynamic-columns/${columnName}`, {
        method: 'DELETE',
        headers: {
          'Authorization': `Bearer ${token}`,
        },
      });

      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.detail || 'Failed to delete column');
      }

      console.log('Column deleted successfully');
      
      // Refresh columns
      await fetchDynamicColumns();
      
      // Show success message
      alert(`Column '${columnName}' deleted successfully!`);
      
    } catch (error: any) {
      console.error('Failed to delete dynamic column:', error);
      alert(`Error: ${error.message}`);
    }
  };

  // Login function with proper error handling
  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setLoginError('');

    try {
      const response = await fetch('/api/monitoring/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ username, password }),
      });

      if (!response.ok) {
        const error = await response.json();
        throw new Error(error.detail || 'Login failed');
      }

      const data = await response.json();
      setToken(data.access_token);
      localStorage.setItem('monitoring_token', data.access_token);
      setIsAuthenticated(true);
      setPassword(''); // Clear password
    } catch (error: any) {
      setLoginError(error.message);
    } finally {
      setLoading(false);
    }
  };

  // Fetch dashboard data with proper error handling
  const fetchDashboardData = async () => {
    if (!token) return;

    try {
      const [workerData, emailData, scheduleData, errorData] = await Promise.all([
        apiCall('/worker-monitoring'),
        apiCall('/email-dashboard'),
        apiCall('/schedule-management'),
        apiCall('/error-tracking')
      ]);

      // Debug logging for potentially problematic data
      console.log('Dashboard data received:', {
        workers: workerData.workers,
        scheduledTasks: scheduleData.scheduled_tasks,
        emailMetrics: emailData.real_time_metrics
      });

      setWorkers(workerData.workers || []);
      setEmailMetrics(emailData.real_time_metrics ? {
  ...emailData.real_time_metrics,
  success_rate: emailData.success_rate,
  failure_rate: emailData.failure_rate
} : null);
      setScheduledTasks(scheduleData.scheduled_tasks || []);
      setErrorAnalytics(errorData.analytics || null);
      setFailedEmails(errorData.failed_emails || []);
      setLastUpdate(new Date());
    } catch (error: any) {
      console.error('Failed to fetch dashboard data:', error);
      if (error.message === 'Session expired') {
        // Token expired, user will be redirected to login
        return;
      }
    }
  };

  // Logout function
  const handleLogout = () => {
    setIsAuthenticated(false);
    setToken(null);
    localStorage.removeItem('monitoring_token');
    setUsername('');
    setPassword('');
    setLoginError('');
  };

  // Email queue helpers
  const handleEmailQueueSearchChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    setEmailQueueSearch(e.target.value);
  };

  const fetchEmailQueue = async (page = emailQueuePage, q = emailQueueSearch) => {
    if (!token) return;
    setEmailQueueLoading(true);
    try {
      const params = new URLSearchParams();
      params.append('page', String(page));
      params.append('page_size', String(emailQueuePageSize));
      if (q) params.append('q', q);

      const response = await fetch(`/api/monitoring/email_queue_with_stage?${params.toString()}`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });

      if (!response.ok) {
        if (response.status === 401) {
          setIsAuthenticated(false);
          setToken(null);
          localStorage.removeItem('monitoring_token');
        }
        throw new Error('Failed to fetch email queue');
      }

      const data = await response.json();
      setEmailQueueResults(data.results || []);
      setEmailQueueTotal(data.total || 0);
      setEmailQueuePage(data.page || 1);
    } catch (error: any) {
      console.error('Error fetching email queue:', error);
      setEmailQueueResults([]);
    } finally {
      setEmailQueueLoading(false);
    }
  };

  const triggerActionForContact = async (contactId: number, action: string) => {
    if (!token) return;
    try {
      const response = await fetch('/api/monitoring/trigger_next_action', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
        body: JSON.stringify({ contact_id: contactId, action })
      });
      if (!response.ok) {
        const err = await response.json();
        throw new Error(err.detail || 'Trigger failed');
      }
      const res = await response.json();
      alert(res.message || 'Triggered');
      // Refresh the queue so status updates show
      fetchEmailQueue(emailQueuePage);
    } catch (error: any) {
      console.error('Trigger error:', error);
      alert(`Error: ${error.message}`);
    }
  };

  // Auto-refresh
  useEffect(() => {
    if (isAuthenticated && token) {
      fetchDashboardData();
      const interval = setInterval(fetchDashboardData, 30000);
      return () => clearInterval(interval);
    }
  }, [isAuthenticated, token]);

  // Format helpers
  const formatTime = (dateString: string | null | undefined) => {
    console.log('formatTime called with:', typeof dateString, dateString);
    
    if (!dateString || dateString === 'null' || dateString === 'undefined') {
      console.log('formatTime: returning N/A for null/undefined');
      return 'N/A';
    }
    
    try {
      // Handle various date formats
      let dateToFormat = dateString;
      
      // If it's a number, treat it as timestamp
      if (typeof dateString === 'number') {
        dateToFormat = new Date(dateString).toISOString();
        console.log('formatTime: converted number to ISO:', dateToFormat);
      }
      
      console.log('formatTime: creating date from:', dateToFormat);
      const date = new Date(dateToFormat);
      console.log('formatTime: created date object:', date);
      
      // Check if date is valid
      if (isNaN(date.getTime()) || date.getTime() === 0) {
        console.warn('formatTime: Invalid date detected:', dateString, 'getTime():', date.getTime());
        return 'Invalid Date';
      }
      
      // Check if date is reasonable (not too far in past/future)
      const now = new Date();
      const yearsDiff = Math.abs(now.getFullYear() - date.getFullYear());
      if (yearsDiff > 50) {
        console.warn('formatTime: Date seems unreasonable:', dateString, date, 'yearsDiff:', yearsDiff);
        return 'Invalid Date';
      }
      
      const result = date.toLocaleString();
      console.log('formatTime: successfully formatted to:', result);
      return result;
    } catch (error) {
      console.error('formatTime: Error formatting date:', dateString, error);
      return 'Invalid Date';
    }
  };

  const formatUptime = (seconds: number) => {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    return `${hours}h ${minutes}m`;
  };

  if (!isAuthenticated) {
    return (
      <div className="min-h-screen bg-gray-100 flex items-center justify-center">
        <div className="bg-white p-8 rounded-lg shadow-md w-96">
          <h2 className="text-2xl font-bold mb-6 text-center text-gray-800">
            🔐 Enhanced CRM Monitoring
          </h2>
          <form onSubmit={handleLogin}>
            <div className="mb-4">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Username
              </label>
              <input
                type="text"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>
            <div className="mb-6">
              <label className="block text-gray-700 text-sm font-bold mb-2">
                Password
              </label>
              <input
                type="password"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md focus:outline-none focus:ring-2 focus:ring-blue-500"
                required
              />
            </div>
            <button
              type="submit"
              disabled={loading}
              className="w-full bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded focus:outline-none focus:shadow-outline disabled:opacity-50"
            >
              {loading ? 'Logging in...' : 'Login'}
            </button>
          </form>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-100">
      {/* Header */}
      <div className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between items-center py-4">
            <h1 className="text-2xl font-bold text-gray-900">
              📊 Enhanced CRM Monitoring Dashboard
            </h1>
            {/* Header actions removed per request */}
          </div>
        </div>
      </div>

      {/* Navigation Tabs */}
      <div className="bg-white border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <nav className="flex space-x-8">
            {[
              { id: 'workers', name: '👥 Workers' },
              { id: 'emailstats', name: '📊 Email Statistics' },
              { id: 'schedule', name: '⏰ Schedule' },
              { id: 'activity', name: '👤 User Activity' },
              { id: 'dynamic-columns', name: '🗂️ Dynamic Columns' }
            ].map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                className={`py-4 px-1 border-b-2 font-medium text-sm ${
                  activeTab === tab.id
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
                }`}
              >
                {tab.name}
              </button>
            ))}
          </nav>
        </div>
      </div>

      {/* Main Content */}
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">

        {/* Overview tab removed per request */}

        {/* Workers Tab */}
        {activeTab === 'workers' && (
          <div className="space-y-6">
            <div className="bg-white shadow rounded-lg">
              <div className="px-4 py-5 sm:p-6">
                <h3 className="text-lg leading-6 font-medium text-gray-900 mb-4">
                  👥 Worker Status & Metrics
                </h3>
                <div className="space-y-4">
                  {workers.map((worker, index) => (
                    <div key={index} className="border border-gray-200 rounded-lg p-4">
                      <div className="flex items-center justify-between mb-3">
                        <h4 className="text-lg font-medium text-gray-900">{worker.name}</h4>
                        <span className={`px-2 py-1 text-xs font-semibold rounded-full ${
                          worker.status === 'healthy'
                            ? 'bg-green-100 text-green-800'
                            : 'bg-red-100 text-red-800'
                        }`}>
                          {worker.status}
                        </span>
                      </div>
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                        <div>
                          <span className="text-gray-500">CPU:</span>
                          <span className="ml-1 font-medium">{worker.cpu_percent?.toFixed(1) || 0}%</span>
                        </div>
                        <div>
                          <span className="text-gray-500">Memory:</span>
                          <span className="ml-1 font-medium">{worker.memory_mb?.toFixed(0) || 0}MB</span>
                        </div>
                        <div>
                          <span className="text-gray-500">Uptime:</span>
                          <span className="ml-1 font-medium">{formatUptime(worker.uptime || 0)}</span>
                        </div>
                        <div>
                          <span className="text-gray-500">Errors:</span>
                          <span className="ml-1 font-medium">{worker.error_count}</span>
                        </div>
                      </div>
                      <div className="mt-2 text-sm text-gray-600">
                        Last Heartbeat: {worker.last_heartbeat ? formatTime(worker.last_heartbeat) : 'Never'}
                      </div>
                      {worker.last_error && (
                        <div className="mt-2 text-sm text-red-600">
                          Last Error: {worker.last_error}
                        </div>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Email tab removed per request */}

        {/* Email Queue tab removed per request */}

        {/* Schedule Tab */}
        {activeTab === 'schedule' && (
          <div className="space-y-6">
            <div className="bg-white shadow rounded-lg">
              <div className="px-4 py-5 sm:p-6">
                <h3 className="text-lg leading-6 font-medium text-gray-900 mb-4">
                  ⏰ Scheduled Tasks
                </h3>
                <div className="space-y-4">
                  {scheduledTasks.map((task, index) => (
                    <div key={index} className="border border-gray-200 rounded-lg p-4">
                      <div className="flex items-center justify-between mb-2">
                        <h4 className="text-lg font-medium text-gray-900">{task.name}</h4>
                        <span className={`px-2 py-1 text-xs font-semibold rounded-full ${
                          task.status === 'running'
                            ? 'bg-blue-100 text-blue-800'
                            : 'bg-gray-100 text-gray-800'
                        }`}>
                          {task.status}
                        </span>
                      </div>
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                        <div>
                          <span className="text-gray-500">Next Run:</span>
                          <div className="font-medium">{task.next_run ? formatTime(task.next_run) : 'Not scheduled'}</div>
                        </div>
                        <div>
                          <span className="text-gray-500">Last Run:</span>
                          <div className="font-medium">{task.last_run ? formatTime(task.last_run) : 'Never'}</div>
                        </div>
                        <div>
                          <span className="text-gray-500">Duration:</span>
                          <div className="font-medium">{task.duration_seconds}s</div>
                        </div>
                        <div>
                          <span className="text-gray-500">Success Rate:</span>
                          <div className="font-medium">{task.success_rate}%</div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Email Statistics Tab */}
        {activeTab === 'emailstats' && token && (
          <div className="space-y-6">
            <DetailedEmailStats token={token} />
          </div>
        )}

        {/* Dynamic Columns Management Tab */}
        {activeTab === 'dynamic-columns' && (
          <div className="space-y-6">
            <div className="bg-white shadow rounded-lg">
              <div className="px-4 py-5 sm:p-6">
                <h3 className="text-lg leading-6 font-medium text-gray-900 mb-4">
                  🗂️ Dynamic Columns Management
                </h3>
                
                {/* Add New Column Form */}
                <div className="mb-6 p-4 bg-gray-50 rounded-lg">
                  <h4 className="font-medium text-gray-700 mb-3">Add New Column</h4>
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Column Name</label>
                      <input
                        type="text"
                        value={newColumnName}
                        onChange={(e) => setNewColumnName(e.target.value)}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
                        placeholder="e.g., company_name"
                      />
                    </div>
                    <div>
                      <label className="block text-sm font-medium text-gray-700 mb-1">Data Type</label>
                      <select
                        value={newColumnType}
                        onChange={(e) => setNewColumnType(e.target.value)}
                        className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
                      >
                        <option value="TEXT">Text</option>
                        <option value="VARCHAR">Varchar</option>
                        <option value="INTEGER">Integer</option>
                        <option value="BOOLEAN">Boolean</option>
                        <option value="DATE">Date</option>
                        <option value="TIMESTAMP">Timestamp</option>
                        <option value="NUMERIC">Numeric</option>
                      </select>
                    </div>
                    <div className="flex items-end">
                      <button
                        onClick={addDynamicColumn}
                        disabled={!newColumnName.trim()}
                        className="w-full bg-blue-500 hover:bg-blue-700 disabled:bg-gray-300 text-white font-bold py-2 px-4 rounded"
                      >
                        Add Column
                      </button>
                    </div>
                  </div>
                  <p className="text-xs text-gray-500 mt-2">
                    Column names must start with a letter and contain only lowercase letters, numbers, and underscores.
                  </p>
                </div>

                {/* Current Columns Display */}
                <div className="space-y-4">
                  <h4 className="font-medium text-gray-700">Current Columns</h4>
                  
                  {/* Standard Columns */}
                  <div>
                    <h5 className="text-sm font-medium text-gray-600 mb-2">Standard Columns</h5>
                    <div className="grid grid-cols-1 md:grid-cols-4 gap-2">
                      {standardColumns.map((col, index) => (
                        <div key={index} className="p-2 bg-gray-100 rounded text-sm">
                          <div className="font-medium">{col.column_name}</div>
                          <div className="text-gray-500 text-xs">{col.data_type}</div>
                        </div>
                      ))}
                    </div>
                  </div>

                  {/* Dynamic Columns */}
                  <div>
                    <h5 className="text-sm font-medium text-gray-600 mb-2">Dynamic Columns</h5>
                    {dynamicColumns.length === 0 ? (
                      <p className="text-gray-500 text-sm">No dynamic columns added yet.</p>
                    ) : (
                      <div className="grid grid-cols-1 md:grid-cols-4 gap-2">
                        {dynamicColumns.map((col, index) => (
                          <div key={index} className="p-3 bg-blue-50 border border-blue-200 rounded">
                            <div className="flex justify-between items-start mb-2">
                              <div>
                                <div className="font-medium text-blue-800">{col.column_name}</div>
                                <div className="text-blue-600 text-xs">{col.data_type}</div>
                                <div className="text-blue-500 text-xs">Added by {col.created_by}</div>
                              </div>
                              <button
                                onClick={() => deleteDynamicColumn(col.column_name)}
                                className="text-red-500 hover:text-red-700 text-sm"
                              >
                                🗑️
                              </button>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {/* User Activity Tab */}
        {activeTab === 'activity' && (
          <div className="space-y-6">
            <div className="bg-white shadow rounded-lg">
              <div className="px-4 py-5 sm:p-6">
                <h3 className="text-lg leading-6 font-medium text-gray-900 mb-4">
                  👤 User Activity Logs
                </h3>
                
                {/* Debug Information */}
                <div className="mb-4 p-4 bg-gray-100 rounded">
                  <h4 className="font-medium text-gray-700">Debug Info:</h4>
                  <p>Activity Loading: {activityLoading ? 'Yes' : 'No'}</p>
                  <p>Logs Count: {userActivityLogs.length}</p>
                  <p>Auth Status: {isAuthenticated ? 'Authenticated' : 'Not Authenticated'}</p>
                  <p>Token Present: {token ? 'Yes' : 'No'}</p>
                  <p>Sample Timestamp: {userActivityLogs.length > 0 ? userActivityLogs[0].timestamp : 'No logs'}</p>
                  <p>Sample Action: {userActivityLogs.length > 0 ? userActivityLogs[0].action_type : 'No logs'}</p>
                </div>

                {/* Simple Test Display */}
                <div className="mb-4 p-4 bg-blue-50 rounded">
                  <h4 className="font-medium text-blue-700 mb-2">Simple Activity Display Test:</h4>
                  {userActivityLogs.length > 0 && (
                    <div className="text-sm">
                      <p>First log: {userActivityLogs[0].username} - {userActivityLogs[0].action_type}</p>
                      <p>Raw timestamp: {userActivityLogs[0].timestamp}</p>
                      <p>Formatted timestamp: {(() => {
                        try {
                          return formatTime(userActivityLogs[0].timestamp);
                        } catch (e) {
                          return `ERROR: ${e.message}`;
                        }
                      })()}</p>
                    </div>
                  )}
                </div>
                
                {/* Filters */}
                <div className="mb-6 grid grid-cols-1 md:grid-cols-4 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Action Type</label>
                    <select
                      value={activityFilters.action_type}
                      onChange={(e) => setActivityFilters({...activityFilters, action_type: e.target.value})}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
                    >
                      <option value="">All Actions</option>
                      <option value="LOGIN">Login</option>
                      <option value="CREATE_CONTACT">Create Contact</option>
                      <option value="UPDATE_CONTACT">Update Contact</option>
                      <option value="DELETE_CONTACT">Delete Contact</option>
                      <option value="STAGE_CHANGE">Stage Change</option>
                      <option value="BULK_CAMPAIGN">Bulk Campaign</option>
                      <option value="START_INDIVIDUAL_CAMPAIGN">Start Individual Campaign</option>
                      <option value="PAUSE_CAMPAIGN">Pause Campaign</option>
                      <option value="RESUME_CAMPAIGN">Resume Campaign</option>
                      <option value="CREATE_EVENT">Create Event</option>
                      <option value="DELETE_EVENT">Delete Event</option>
                      <option value="EXCEL_UPLOAD">Excel Upload</option>
                      <option value="CREATE_USER">Create User</option>
                      <option value="DELETE_USER">Delete User</option>
                      <option value="CHANGE_PASSWORD">Change Password</option>
                      <option value="ADD_DYNAMIC_COLUMN">Add Dynamic Column</option>
                      <option value="DELETE_DYNAMIC_COLUMN">Delete Dynamic Column</option>
                    </select>
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Username</label>
                    <input
                      type="text"
                      value={activityFilters.username}
                      onChange={(e) => setActivityFilters({...activityFilters, username: e.target.value})}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
                      placeholder="Filter by username"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Date From</label>
                    <input
                      type="date"
                      value={activityFilters.date_from}
                      onChange={(e) => setActivityFilters({...activityFilters, date_from: e.target.value})}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-gray-700 mb-1">Date To</label>
                    <input
                      type="date"
                      value={activityFilters.date_to}
                      onChange={(e) => setActivityFilters({...activityFilters, date_to: e.target.value})}
                      className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-indigo-500 focus:border-indigo-500"
                    />
                  </div>
                </div>

                <div className="flex gap-2 mb-6">
                  <button
                    onClick={() => fetchUserActivityLogs()}
                    className="bg-blue-500 hover:bg-blue-700 text-white font-bold py-2 px-4 rounded"
                  >
                    Apply Filters
                  </button>
                  <button
                    onClick={() => {
                      setActivityFilters({action_type: '', username: '', date_from: '', date_to: ''});
                      fetchUserActivityLogs({action_type: '', username: '', date_from: '', date_to: ''});
                    }}
                    className="bg-gray-500 hover:bg-gray-700 text-white font-bold py-2 px-4 rounded"
                  >
                    Clear Filters
                  </button>
                </div>

                {/* Activity Logs Table */}
                {(() => {
                  try {
                    return (
                      <div className="overflow-x-auto">
                        <table className="min-w-full divide-y divide-gray-200">
                          <thead className="bg-gray-50">
                            <tr>
                              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Time
                              </th>
                              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                User
                              </th>
                              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Action
                              </th>
                              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Description
                              </th>
                              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                Target
                              </th>
                              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                                IP Address
                              </th>
                            </tr>
                          </thead>
                          <tbody className="bg-white divide-y divide-gray-200">
                            {userActivityLogs.map((log, index) => {
                              try {
                                return (
                                  <tr key={log.id || index} className="hover:bg-gray-50">
                                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                                      {(() => {
                                        try {
                                          return formatTime(log.timestamp);
                                        } catch (e) {
                                          console.error('Error formatting timestamp for log:', log, e);
                                          return 'Error';
                                        }
                                      })()}
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-900">
                                      <span className="font-medium">{log.username || 'Unknown'}</span>
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap text-sm">
                                      <span className={`px-2 inline-flex text-xs leading-5 font-semibold rounded-full ${
                                        log.action_type === 'LOGIN' ? 'bg-green-100 text-green-800' :
                                        log.action_type === 'CREATE_CONTACT' ? 'bg-blue-100 text-blue-800' :
                                        log.action_type === 'UPDATE_CONTACT' ? 'bg-yellow-100 text-yellow-800' :
                                        log.action_type === 'DELETE_CONTACT' ? 'bg-red-100 text-red-800' :
                                        log.action_type === 'STAGE_CHANGE' ? 'bg-purple-100 text-purple-800' :
                                        log.action_type === 'BULK_CAMPAIGN' ? 'bg-orange-100 text-orange-800' :
                                        log.action_type === 'START_INDIVIDUAL_CAMPAIGN' ? 'bg-indigo-100 text-indigo-800' :
                                        log.action_type === 'PAUSE_CAMPAIGN' ? 'bg-gray-100 text-gray-800' :
                                        log.action_type === 'RESUME_CAMPAIGN' ? 'bg-green-100 text-green-800' :
                                        log.action_type === 'CREATE_EVENT' ? 'bg-cyan-100 text-cyan-800' :
                                        log.action_type === 'EXCEL_UPLOAD' ? 'bg-pink-100 text-pink-800' :
                                        log.action_type === 'DELETE_EVENT' ? 'bg-red-100 text-red-800' :
                                        log.action_type === 'CREATE_USER' ? 'bg-emerald-100 text-emerald-800' :
                                        log.action_type === 'DELETE_USER' ? 'bg-red-100 text-red-800' :
                                        log.action_type === 'CHANGE_PASSWORD' ? 'bg-amber-100 text-amber-800' :
                                        log.action_type === 'ADD_DYNAMIC_COLUMN' ? 'bg-teal-100 text-teal-800' :
                                        log.action_type === 'DELETE_DYNAMIC_COLUMN' ? 'bg-red-100 text-red-800' :
                                        'bg-gray-100 text-gray-800'
                                      }`}>
                                        {log.action_type || 'UNKNOWN'}
                                      </span>
                                    </td>
                                    <td className="px-6 py-4 text-sm text-gray-900 max-w-xs truncate">
                                      {log.action_description || 'No description'}
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                      {log.target_type && (
                                        <span>
                                          {log.target_type}: {log.target_name || log.target_id}
                                        </span>
                                      )}
                                    </td>
                                    <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-500">
                                      {log.ip_address || 'N/A'}
                                    </td>
                                  </tr>
                                );
                              } catch (e) {
                                console.error('Error rendering log row:', log, e);
                                return (
                                  <tr key={index} className="hover:bg-gray-50">
                                    <td colSpan={6} className="px-6 py-4 text-sm text-red-600">
                                      Error rendering log entry
                                    </td>
                                  </tr>
                                );
                              }
                            })}
                          </tbody>
                        </table>
                      </div>
                    );
                  } catch (e) {
                    console.error('Error rendering activity logs table:', e);
                    return (
                      <div className="text-center py-8 text-red-600">
                        Error loading activity logs table. Check console for details.
                      </div>
                    );
                  }
                })()}

                {activityLoading && (
                  <div className="text-center py-8 text-gray-500">
                    <div className="inline-block animate-spin rounded-full h-8 w-8 border-b-2 border-gray-900"></div>
                    <div className="mt-2">Loading activity logs...</div>
                  </div>
                )}

                {!activityLoading && userActivityLogs.length === 0 && (
                  <div className="text-center py-8 text-gray-500">
                    No activity logs found matching the current filters.
                  </div>
                )}
              </div>
            </div>
          </div>
        )}

        {/* Errors tab removed per request */}

      </div>
    </div>
  );
}
