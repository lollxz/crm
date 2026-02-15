import React, { useState, useMemo, useEffect } from 'react';
import { Edit2, Trash2, ChevronRight, ChevronDown, Copy } from 'lucide-react';
import { Customer, SelectedCustomers } from '../types';
import { format, parse } from 'date-fns';
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { authFetch } from '@/utils/authFetch';

interface DataTableProps {
  data: Customer[];
  onEdit: (customer: Customer) => void;
  onDelete: (id: number) => void;
  onBatchDelete?: (customers: Customer[]) => void;
  onStartCampaign?: (customerIds: number[]) => void;
  onSingleCampaign?: (customer: Customer) => void;
}

export function DataTable({ data, onEdit, onDelete, onBatchDelete, onStartCampaign, onSingleCampaign }: DataTableProps) {
  const [expandedRows, setExpandedRows] = useState<Record<string, boolean>>({});
  const [expandedSources, setExpandedSources] = useState<Record<string, boolean>>({});
  const [selectedCustomers, setSelectedCustomers] = useState<SelectedCustomers>({});
  const [selectAll, setSelectAll] = useState(false);
  const [campaignSelection, setCampaignSelection] = useState<{ [key: number]: boolean }>({});
  const [editingProgressId, setEditingProgressId] = useState<number | null>(null);
  const [progressUpdateStatus, setProgressUpdateStatus] = useState<{ [id: number]: 'idle' | 'saving' | 'saved' | 'error' }>({});

  const handleSelectAll = () => {
    if (selectAll) {
      setSelectedCustomers({});
    } else {
      const newSelected: SelectedCustomers = {};
      data.forEach(customer => {
        newSelected[customer.id] = customer;
      });
      setSelectedCustomers(newSelected);
    }
    setSelectAll(!selectAll);
  };

  const handleSelectCustomer = (customer: Customer) => {
    setSelectedCustomers(prev => {
      const newSelected = { ...prev };
      if (newSelected[customer.id]) {
        delete newSelected[customer.id];
      } else {
        newSelected[customer.id] = customer;
      }
      return newSelected;
    });
  };

  const handleBatchDelete = () => {
    const selectedCount = Object.keys(selectedCustomers).length;
    if (selectedCount === 0) {
      toast.error('Please select records to delete');
      return;
    }

    const selectedArray = Object.values(selectedCustomers);
    const customerNames = selectedArray.map(c => c.name).join(', ');
    const message = `Are you sure you want to delete ${selectedCount} selected record(s)?\n\nSelected customers: ${customerNames}`;

    if (window.confirm(message)) {
      onBatchDelete?.(selectedArray);
      setSelectedCustomers({});
      setSelectAll(false);
    }
  };

  const handleCampaignCheckbox = (id: number) => {
    setCampaignSelection(prev => ({ ...prev, [id]: !prev[id] }));
  };

  const handleSelectAllCampaign = () => {
    const allSelected = data.every(c => campaignSelection[c.id]);
    const newSelection: { [key: number]: boolean } = {};
    data.forEach(c => {
      newSelection[c.id] = !allSelected;
    });
    setCampaignSelection(newSelection);
  };

  const selectedCampaignIds = Object.entries(campaignSelection)
    .filter(([_, checked]) => checked)
    .map(([id]) => Number(id));

  const sortedData = useMemo(() => {
    const monthOrder = [
      'January', 'February', 'March', 'April', 'May', 'June',
      'July', 'August', 'September', 'October', 'November', 'December'
    ];

    return [...data].sort((a, b) => {
      const monthA = (a.month || '').toLowerCase();
      const monthB = (b.month || '').toLowerCase();

      if (monthA === 'pending' && monthB !== 'pending') return 1;
      if (monthB === 'pending' && monthA !== 'pending') return -1;

      const indexA = monthOrder.findIndex(m => (m || '').toLowerCase() === monthA);
      const indexB = monthOrder.findIndex(m => (m || '').toLowerCase() === monthB);

      if (indexA === -1 && indexB === -1) return 0;
      if (indexA === -1) return 1;
      if (indexB === -1) return -1;

      return indexA - indexB;
    });
  }, [data]);

  const formatDate = (dateStr: string) => {
    if (!dateStr || dateStr === 'N/A') return 'N/A';
    try {
      const date = dateStr.includes('/') 
        ? parse(dateStr, 'dd/MM/yyyy', new Date())
        : new Date(dateStr);
      
      return format(date, 'dd/MM/yyyy');
    } catch (error) {
      return dateStr;
    }
  };

  const getStatusColor = (status: string) => {
    const statusLower = (status || '').toLowerCase();
    
    if (statusLower === 'n/a') {
      return 'text-gray-600';
    }
    
    switch (statusLower) {
      case 'no response':
        return 'bg-[#a36c50] text-white';
      case 'not interested':
        return 'bg-[#ffff00] text-black';
      case 'covered':
        return 'bg-[#000000] text-white';
      case 'follow-up':
        return 'bg-[#FF8800] text-white';
      case 'first reminder':
        return 'bg-[#FF8800] text-white';
      case 'second reminder':
        return 'bg-[#FF8800] text-white';
      case 'third reminder':
        return 'bg-[#FF8800] text-white';
      case 'fourth reminder':
        return 'bg-[#FF8800] text-white';
      case 'fifth reminder':
        return 'bg-[#FF8800] text-white';
      case 'sixth reminder':
        return 'bg-[#FF8800] text-white';
      case 'completed':
        return 'bg-[#28A745] text-white';
      case 'cancelled':
        return 'bg-[#FF0000] text-white';
      case 'pending':
        return 'bg-[#526FD0] text-white';
      case 'ooo':
        return 'bg-[#B0E0E6] text-black';
      default:
        return 'bg-gray-100 text-gray-800';
    }
  };

  const getStageColor = (stage: string) => {
    const stageLower = (stage || '').toLowerCase();
    
    if (stageLower === 'n/a') {
      return 'text-gray-600';
    }
    
    switch (stageLower) {
      case 'first message':
        return 'bg-[#b2cef3] text-black';
      case 'forms':
        return 'bg-[#1967B2] text-white';
      case 'payments':
        return 'bg-[#dcffd4] text-black';
      case 'invoice & confirmation':
        return 'bg-[#FF8800] text-white';
      case 'payment due':
        return 'bg-[#FF8800] text-white';
      case 'completed':
        return 'bg-[#28A745] text-white';
      case 'problem':
        return 'bg-[#FF0000] text-white';
      case 'wrong person':
        return 'bg-[#d16afc] text-white';
      case 'mail delivery':
        return 'bg-[#ababab] text-white';
      case 'hcn':
        return 'bg-[#FFC8DD] text-black';
      case "supplier's payment":
        return 'bg-[#E65C9C] text-white';
      default:
        return 'bg-blue-100 text-blue-800';
    }
  };

  const getValidationColor = (validation: string) => {
    const validationLower = (validation || '').toLowerCase().trim();
    switch (validationLower) {
      case 'valid':
        return 'text-green-600';
      case 'not valid':
      case 'notvalid':
      case 'invalid':
        return 'text-red-600';
      default:
        return 'text-gray-600';
    }
  };

  const getProgressColor = (progress: string) => {
    switch ((progress || '').toLowerCase()) {
      case 'pause':
        return 'bg-yellow-300 text-yellow-900';
      case 'continue':
        return 'bg-green-500 text-white';
      case 'none':
      default:
        return 'bg-gray-200 text-gray-700';
    }
  };

  const handleProgressChange = async (customer: Customer, newProgress: 'none' | 'pause' | 'continue') => {
    setProgressUpdateStatus(prev => ({ ...prev, [customer.id]: 'saving' }));
    try {
      // Call backend API to update progress
      await fetch(`/customers/${customer.id}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ progress: newProgress })
      });
      setProgressUpdateStatus(prev => ({ ...prev, [customer.id]: 'saved' }));
      setTimeout(() => setProgressUpdateStatus(prev => ({ ...prev, [customer.id]: 'idle' })), 1000);
      setEditingProgressId(null);
    } catch (e) {
      setProgressUpdateStatus(prev => ({ ...prev, [customer.id]: 'error' }));
      setTimeout(() => setProgressUpdateStatus(prev => ({ ...prev, [customer.id]: 'idle' })), 2000);
    }
  };

  const toggleRow = (id: number) => {
    setExpandedRows(prev => ({
      ...prev,
      [id]: !prev[id]
    }));
  };

  const toggleSource = (id: number) => {
    setExpandedSources(prev => ({
      ...prev,
      [id]: !prev[id]
    }));
  };

  const copyToClipboard = async (text: string) => {
    try {
      await navigator.clipboard.writeText(text);
      toast.success('Event name copied to clipboard!');
    } catch (err) {
      console.error('Failed to copy:', err);
      const textArea = document.createElement('textarea');
      textArea.value = text;
      document.body.appendChild(textArea);
      textArea.select();
      try {
        document.execCommand('copy');
        toast.success('Event name copied to clipboard!');
      } catch (err) {
        toast.error('Failed to copy text to clipboard');
      }
      document.body.removeChild(textArea);
    }
  };

  const renderEventNameCell = (content: string, id: number) => {
    const isExpanded = expandedRows[id];
    const shouldShowExpand = content && content.length > 50;

    return (
      <div className="max-w-[200px]">
        <div className="flex items-center space-x-2">
          <div className={isExpanded ? 'whitespace-pre-line' : 'truncate'}>
            {isExpanded ? content : shouldShowExpand ? `${content.slice(0, 50)}...` : content}
          </div>
          <button
            onClick={(e) => {
              e.stopPropagation();
              copyToClipboard(content);
            }}
            className="text-gray-600 hover:text-gray-800 text-sm flex items-center"
            title="Copy event name"
          >
            <Copy size={14} />
          </button>
        </div>
        {shouldShowExpand && (
          <button
            onClick={() => toggleRow(id)}
            className="text-blue-600 hover:text-blue-800 text-sm flex items-center mt-1"
          >
            {isExpanded ? (
              <>
                Show Less <ChevronDown size={16} />
              </>
            ) : (
              <>
                Read More <ChevronRight size={16} />
              </>
            )}
          </button>
        )}
      </div>
    );
  };

  const renderEmailCell = (content: string) => {
    if (!content) return null;
    
    const emails = content.split(/[,;]/).map(email => email.trim()).filter(Boolean);
    
    return (
      <div className="whitespace-pre-line">
        {emails.map((email, index) => (
          <div key={index} className="py-0.5">{email}</div>
        ))}
      </div>
    );
  };

  const renderValidationCell = (content: string) => {
    if (!content) return null;
    
    const validations = content.split(/[,;]/).map(validation => validation.trim()).filter(Boolean);
    
    return (
      <div className="whitespace-pre-line">
        {validations.map((validation, index) => (
          <div key={index} className={`py-0.5 ${getValidationColor(validation)}`}>
            {validation}
          </div>
        ))}
      </div>
    );
  };

  const renderSourceCell = (content: string, id: number) => {
    const isExpanded = expandedSources[id];
    const shouldShowExpand = content && content.length > 30;

    return (
      <div className="max-w-[200px]">
        <div className={isExpanded ? 'whitespace-pre-line' : 'truncate'}>
          {isExpanded ? content : shouldShowExpand ? `${content.slice(0, 30)}...` : content}
        </div>
        {shouldShowExpand && (
          <button
            onClick={() => toggleSource(id)}
            className="text-blue-600 hover:text-blue-800 text-sm flex items-center mt-1"
          >
            {isExpanded ? (
              <>
                Show Less <ChevronDown size={16} />
              </>
            ) : (
              <>
                Read More <ChevronRight size={16} />
              </>
            )}
          </button>
        )}
      </div>
    );
  };

  const renderNotesCell = (content: string) => {
    if (!content) return null;
    return (
      <div className="w-[300px] min-w-[300px]">
        <div className="whitespace-pre-line">{content}</div>
      </div>
    );
  };

  const renderCell = (content: string, field: string, id: number) => {
    if (field === 'notes') {
      return renderNotesCell(content);
    }
    if (field === 'source') {
      return renderSourceCell(content, id);
    }
    if (field === 'event_name') {
      return renderEventNameCell(content, id);
    }
    if (field === 'email' || field === 'sender_email') {
      return renderEmailCell(content);
    }
    if (field === 'validation_result') {
      return renderValidationCell(content);
    }
    if (field === 'status') {
      const colorClass = getStatusColor(content);
      return (
        <span className={`px-2 py-1 rounded-full text-xs font-medium whitespace-nowrap ${colorClass}`}>
          {content}
        </span>
      );
    }
    if (field === 'stage') {
      const colorClass = getStageColor(content);
      return (
        <span className={`px-2 py-1 rounded-full text-xs font-medium whitespace-nowrap ${colorClass}`}>
          {content}
        </span>
      );
    }
    if (field === 'progress') {
      const colorClass = getProgressColor(content);
      if (editingProgressId === id) {
        return (
          <span>
            <select
              value={content}
              onChange={e => handleProgressChange(data.find(c => c.id === id)!, e.target.value as 'none' | 'pause' | 'continue')}
              onBlur={() => setEditingProgressId(null)}
              className={`px-2 py-1 rounded-full text-xs font-medium whitespace-nowrap border ${colorClass}`}
              autoFocus
            >
              <option value="none">None</option>
              <option value="pause">Pause</option>
              <option value="continue">Continue</option>
            </select>
            {progressUpdateStatus[id] === 'saving' && <span className="ml-1 animate-spin">⏳</span>}
            {progressUpdateStatus[id] === 'saved' && <span className="ml-1 text-green-500">✔</span>}
            {progressUpdateStatus[id] === 'error' && <span className="ml-1 text-red-500">✖</span>}
          </span>
        );
      }
      return (
        <span
          className={`px-2 py-1 rounded-full text-xs font-medium whitespace-nowrap cursor-pointer ${colorClass} hover:ring-2 hover:ring-blue-400`}
          onClick={() => setEditingProgressId(id)}
          title="Click to edit progress"
        >
          {content.charAt(0).toUpperCase() + content.slice(1)}
        </span>
      );
    }
    return <div className="truncate">{content}</div>;
  };

  const columns = [
    { key: 'event_name', label: 'Event Name' },
    { key: 'org_name', label: 'Organization' },
    { key: 'month', label: 'Month' },
    { key: 'name', label: 'Name' },
    { key: 'email', label: 'Email' },
    { key: 'source', label: 'Source' },
    { key: 'validation_result', label: 'Validation' },
    { key: 'organizer', label: 'Organizer' },
    { key: 'date', label: 'Date' },
    { key: 'date2', label: 'Date2' },
    { key: 'sender_email', label: 'Sender Email' },
    { key: 'stage', label: 'Stage' },
    { key: 'status', label: 'Status' },
    { key: 'progress', label: 'Progress' },
    { key: 'hotel_name', label: 'Hotel Name' },
    { key: 'supplier', label: 'Supplier' },
    { key: 'notes', label: 'Notes' },
    { key: 'speaker_type', label: 'Speaker Type' },
    { key: 'nationality', label: 'Nationality' },
    { key: 'workplace', label: 'Workplace' },
    { key: 'payment_method', label: 'Payment Method' },
    { key: 'trigger', label: 'Trigger' },
    { key: 'sending_time', label: 'Sending Time' }
  ];

  if (data.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16">
        <div className="text-5xl text-gray-300 mb-4">🔍</div>
        <h3 className="text-2xl font-semibold text-gray-700 mb-2">No results found</h3>
        <p className="text-gray-500 text-center max-w-md">
          Oops! It seems we couldn't find any leads that match your current search.
        </p>
      </div>
    );
  }

  return (
    <div>
      {/* Start Campaign Button */}
      {onStartCampaign && (
        <div className="p-4 bg-gray-50 border-b flex justify-between items-center">
          <button
            onClick={() => onStartCampaign(selectedCampaignIds)}
            className={`px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 flex items-center space-x-2 ${selectedCampaignIds.length === 0 ? 'opacity-50 cursor-not-allowed' : ''}`}
            disabled={selectedCampaignIds.length === 0}
          >
            <span>Start Campaign</span>
          </button>
        </div>
      )}
      {Object.keys(selectedCustomers).length > 0 && (
        <div className="p-4 bg-gray-50 border-b flex justify-between items-center">
          <div className="text-sm text-gray-600">
            {Object.keys(selectedCustomers).length} record(s) selected
          </div>
          <button
            onClick={handleBatchDelete}
            className="px-4 py-2 bg-red-600 text-white rounded-md hover:bg-red-700 flex items-center space-x-2"
          >
            <Trash2 size={16} />
            <span>Delete Selected</span>
          </button>
        </div>
      )}
      <div className="relative overflow-x-auto" style={{ height: 'calc(100vh - 16rem)' }}>
        <table className="min-w-full divide-y divide-gray-200">
          <thead className="bg-gray-50">
            <tr>
              <th className="px-2 py-3 text-left">+</th>
              <th className="px-6 py-3 text-left">
                <input
                  type="checkbox"
                  checked={selectAll}
                  onChange={handleSelectAll}
                  className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                />
              </th>
              {columns.map((column) => (
                <th 
                  key={column.key} 
                  className={`px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider ${
                    column.key === 'notes' ? 'w-[300px] min-w-[300px]' : 'whitespace-nowrap'
                  }`}
                >
                  {column.label}
                  {column.key === 'email' && (
                    <input
                      type="checkbox"
                      className="ml-2"
                      checked={data.length > 0 && data.every(c => campaignSelection[c.id])}
                      onChange={handleSelectAllCampaign}
                    />
                  )}
                </th>
              ))}
              <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider whitespace-nowrap">
                Actions
              </th>
            </tr>
          </thead>
          <tbody className="bg-white divide-y divide-gray-200">
            {sortedData.map((customer) => (
              <tr key={customer.id} className="hover:bg-gray-50">
                <td className="px-2 py-2">
                  {onSingleCampaign && (
                    <button
                      onClick={() => onSingleCampaign(customer)}
                      className="text-blue-600 hover:text-blue-800 font-bold text-lg"
                      title="Start Campaign for this contact"
                    >
                      +
                    </button>
                  )}
                </td>
                <td className="px-6 py-4">
                  <input
                    type="checkbox"
                    checked={!!selectedCustomers[customer.id]}
                    onChange={() => handleSelectCustomer(customer)}
                    className="rounded border-gray-300 text-blue-600 focus:ring-blue-500"
                  />
                </td>
                {columns.map((column) => (
                  <td key={column.key} className="px-6 py-4">
                    {column.key === 'email' ? (
                      <div className="flex items-center space-x-2">
                        {renderCell(customer[column.key as keyof Customer], column.key, customer.id)}
                        <input
                          type="checkbox"
                          className="ml-2"
                          checked={!!campaignSelection[customer.id]}
                          onChange={() => handleCampaignCheckbox(customer.id)}
                        />
                      </div>
                    ) : column.key === 'sending_time' ? (
                      <div className="whitespace-nowrap">
                        {customer.sending_time ? format(new Date(customer.sending_time), 'PPpp') : ''}
                      </div>
                    ) : (
                      renderCell(customer[column.key as keyof Customer], column.key, customer.id)
                    )}
                  </td>
                ))}
                <td className="px-6 py-4 whitespace-nowrap">
                  <div className="flex space-x-2">
                    <button
                      onClick={() => onEdit(customer)}
                      className="text-blue-600 hover:text-blue-900"
                    >
                      <Edit2 size={18} />
                    </button>
                    <button
                      onClick={() => onDelete(Number(customer.id))}
                      className="text-red-600 hover:text-red-900"
                    >
                      <Trash2 size={18} />
                    </button>
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}