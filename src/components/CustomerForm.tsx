import React, { useState, useRef, useEffect } from 'react';
import { Customer, DEFAULT_STATUS_OPTIONS, STAGE_OPTIONS, COUNTRIES, SUPPLIER_OPTIONS } from '../types';
import { X, ChevronDown } from 'lucide-react';
import DatePicker from 'react-datepicker';
import "react-datepicker/dist/react-datepicker.css";
import { format, parse } from 'date-fns';
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { authFetch } from '@/utils/authFetch';

interface CustomerFormProps {
  customer: Partial<Customer>;
  onSubmit: (customer: Partial<Customer>) => void;
  onClose: () => void;
}

export function CustomerForm({ customer, onSubmit, onClose }: CustomerFormProps) {
  const [formData, setFormData] = React.useState({
    ...customer,
    date: customer.date || '',
    email: customer.email || '',
    sender_email: customer.sender_email || '',
    speaker_type: customer.speaker_type || ''
  });
  const [customStatus, setCustomStatus] = React.useState('');
  const [selectedStatus, setSelectedStatus] = React.useState(customer.status || DEFAULT_STATUS_OPTIONS[0]);
  const [customStage, setCustomStage] = React.useState('');
  const [selectedStage, setSelectedStage] = React.useState(customer.stage || STAGE_OPTIONS[0]);
  const [customSupplier, setCustomSupplier] = React.useState('');
  const [selectedSupplier, setSelectedSupplier] = React.useState(customer.supplier || SUPPLIER_OPTIONS[0]);
  const [selectedDate, setSelectedDate] = useState<Date | null>(() => {
    if (customer.date && customer.date !== 'Pending') {
      const parsedDate = parse(customer.date, 'dd/MM/yyyy', new Date());
      return isNaN(parsedDate.getTime()) ? null : parsedDate;
    }
    return null;
  });
  const [progress, setProgress] = useState((customer as any).progress || 'none');
  const [date2, setDate2] = useState((customer as any).date2 || '');

  // Nationality dropdown state
  const [isNationalityDropdownOpen, setIsNationalityDropdownOpen] = useState(false);
  const [nationalitySearch, setNationalitySearch] = useState(customer.nationality || '');
  const dropdownRef = useRef<HTMLDivElement>(null);

  const filteredCountries = COUNTRIES.filter(country =>
    (country || '').toLowerCase().includes((nationalitySearch || '').toLowerCase())
  );

  useEffect(() => {
    const handleClickOutside = (event: MouseEvent) => {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setIsNationalityDropdownOpen(false);
      }
    };

    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const handleDateChange = (date: Date | null) => {
    setSelectedDate(date);
    if (date) {
      setFormData({ ...formData, date: format(date, 'dd/MM/yyyy') });
    } else {
      setFormData({ ...formData, date: 'N/A' });
    }
  };

  const formatNotes = (notes: string) => {
    return notes
      .replace(/\s+/g, ' ')
      .trim()
      .split(/(.{1,100})(?:\s|$)/)
      .filter(Boolean)
      .join('\n');
  };

  const validateEmails = (emails: string): boolean => {
    const emailList = emails.split(/[,;]/).map(email => email.trim()).filter(Boolean);
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    return emailList.every(email => emailRegex.test(email));
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    
    // Validate emails
    if (formData.email && !validateEmails(formData.email)) {
      alert('Please enter valid email addresses (separated by commas or semicolons)');
      return;
    }
    if (formData.sender_email && !validateEmails(formData.sender_email)) {
      alert('Please enter valid sender email addresses (separated by commas or semicolons)');
      return;
    }

    const finalStatus = selectedStatus === 'custom' ? customStatus : selectedStatus;
    const finalStage = selectedStage === 'custom' ? customStage : selectedStage;
    const formattedNotes = formatNotes(formData.notes || '');
    
    // Validation for form_link/payment_link
    if (customer.id && finalStage === 'forms' && !formData.form_link) {
      toast.error('Missing form link for this customer');
      return;
    }
    if (customer.id && finalStage === 'payments' && !formData.payment_link) {
      toast.error('Missing payment link for this customer');
      return;
    }
    
    onSubmit({ 
      ...formData, 
      status: finalStatus,
      stage: finalStage,
      notes: formattedNotes,
      email: formData.email || undefined,
      sender_email: formData.sender_email || undefined,
      speaker_type: formData.speaker_type || undefined,
      city: formData.city || '',
      venue: formData.venue || '',
      progress: progress || 'none',
      date2: date2 || '',
    });
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg p-8 max-w-2xl w-full max-h-[90vh] overflow-y-auto">
        <div className="flex justify-between items-center mb-6">
          <h2 className="text-2xl font-bold">
            {customer.id ? 'Edit Customer' : 'Add Customer'}
          </h2>
          <button onClick={onClose} className="text-gray-500 hover:text-gray-700">
            <X size={24} />
          </button>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700">Event Name</label>
              <input
                type="text"
                value={formData.event_name || ''}
                onChange={(e) => setFormData({ ...formData, event_name: e.target.value })}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                required
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Organization</label>
              <input
                type="text"
                value={formData.org_name || ''}
                onChange={(e) => setFormData({ ...formData, org_name: e.target.value })}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                required
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Month</label>
              <input
                type="text"
                value={formData.month || ''}
                onChange={(e) => setFormData({ ...formData, month: e.target.value })}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                required
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Name</label>
              <input
                type="text"
                value={formData.name || ''}
                onChange={(e) => setFormData({ ...formData, name: e.target.value })}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                required
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Email (separate multiple emails with commas)</label>
              <input
                type="text"
                value={formData.email || ''}
                onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                placeholder="email1@example.com, email2@example.com"
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Source</label>
              <input
                type="text"
                value={formData.source || ''}
                onChange={(e) => setFormData({ ...formData, source: e.target.value })}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Validation Result</label>
              <input
                type="text"
                value={formData.validation_result || ''}
                onChange={(e) => setFormData({ ...formData, validation_result: e.target.value })}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Organizer</label>
              <input
                type="text"
                value={formData.organizer || ''}
                onChange={(e) => setFormData({ ...formData, organizer: e.target.value })}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Date</label>
              <DatePicker
                selected={selectedDate}
                onChange={handleDateChange}
                dateFormat="dd/MM/yyyy"
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                placeholderText="Select date"
                isClearable
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Sender Email (separate multiple emails with commas)</label>
              <input
                type="text"
                value={formData.sender_email || ''}
                onChange={(e) => setFormData({ ...formData, sender_email: e.target.value })}
                placeholder="sender1@example.com, sender2@example.com"
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Stage</label>
              <div className="space-y-2">
                <select
                  value={selectedStage}
                  onChange={(e) => {
                    setSelectedStage(e.target.value);
                    if (e.target.value !== 'custom') {
                      setFormData({ ...formData, stage: e.target.value });
                    }
                  }}
                  className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                  required
                >
                  {STAGE_OPTIONS.map((option) => (
                    <option key={option} value={option}>
                      {option === 'custom' ? 'Custom Stage' : option}
                    </option>
                  ))}
                </select>
                {selectedStage === 'custom' && (
                  <input
                    type="text"
                    value={customStage}
                    onChange={(e) => {
                      setCustomStage(e.target.value);
                      setFormData({ ...formData, stage: e.target.value });
                    }}
                    placeholder="Enter custom stage"
                    className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                  />
                )}
              </div>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Status</label>
              <div className="space-y-2">
                <select
                  value={selectedStatus}
                  onChange={(e) => {
                    setSelectedStatus(e.target.value);
                    if (e.target.value !== 'custom') {
                      setFormData({ ...formData, status: e.target.value });
                    }
                  }}
                  className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                >
                  {DEFAULT_STATUS_OPTIONS.map((option) => (
                    <option key={option} value={option}>
                      {option === 'custom' ? 'Custom Status' : option}
                    </option>
                  ))}
                </select>
                {selectedStatus === 'custom' && (
                  <input
                    type="text"
                    value={customStatus}
                    onChange={(e) => {
                      setCustomStatus(e.target.value);
                      setFormData({ ...formData, status: e.target.value });
                    }}
                    placeholder="Enter custom status"
                    className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                  />
                )}
              </div>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Hotel Name</label>
              <input
                type="text"
                value={formData.hotel_name || ''}
                onChange={(e) => setFormData({ ...formData, hotel_name: e.target.value })}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Supplier</label>
              <div className="flex space-x-2 items-center">
                <select
                  value={selectedSupplier}
                  onChange={(e) => {
                    setSelectedSupplier(e.target.value);
                    if (e.target.value !== 'custom') {
                      setFormData({ ...formData, supplier: e.target.value + (customSupplier ? '+' + customSupplier : '') });
                    } else {
                      setFormData({ ...formData, supplier: customSupplier });
                    }
                  }}
                  className="mt-1 block w-40 rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                >
                  {SUPPLIER_OPTIONS.map((option) => (
                    <option key={option} value={option}>
                      {option === 'custom' ? 'Custom Supplier' : option}
                    </option>
                  ))}
                </select>
                {selectedSupplier === 'custom' ? (
                  <input
                    type="text"
                    value={customSupplier}
                    onChange={(e) => {
                      setCustomSupplier(e.target.value);
                      setFormData({ ...formData, supplier: e.target.value });
                    }}
                    placeholder="Enter custom supplier name"
                    className="mt-1 block flex-1 rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                  />
                ) : (
                  <input
                    type="text"
                    value={customSupplier}
                    onChange={(e) => {
                      setCustomSupplier(e.target.value);
                      setFormData({ ...formData, supplier: selectedSupplier + (e.target.value ? '+' + e.target.value : '') });
                    }}
                    placeholder="Add details (optional)"
                    className="mt-1 block flex-1 rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                  />
                )}
              </div>
            </div>
            <div className="relative" ref={dropdownRef}>
              <label className="block text-sm font-medium text-gray-700">Nationality</label>
              <div className="mt-1 relative">
                <div
                  className="flex items-center justify-between w-full rounded-md border border-gray-300 shadow-sm px-3 py-2 bg-white cursor-pointer focus:outline-none focus:ring-2 focus:ring-blue-500"
                  onClick={() => setIsNationalityDropdownOpen(!isNationalityDropdownOpen)}
                >
                  <input
                    type="text"
                    value={nationalitySearch}
                    onChange={(e) => {
                      setNationalitySearch(e.target.value);
                      setFormData({ ...formData, nationality: e.target.value });
                    }}
                    className="w-full border-none focus:outline-none"
                    placeholder="Search country..."
                    onClick={(e) => e.stopPropagation()}
                  />
                  <ChevronDown size={20} className="text-gray-400" />
                </div>
                {isNationalityDropdownOpen && (
                  <div className="absolute z-10 mt-1 w-full bg-white shadow-lg max-h-60 rounded-md py-1 overflow-auto border border-gray-300">
                    {filteredCountries.map((country) => (
                      <div
                        key={country}
                        className="px-3 py-2 hover:bg-blue-50 cursor-pointer"
                        onClick={() => {
                          setFormData({ ...formData, nationality: country });
                          setNationalitySearch(country);
                          setIsNationalityDropdownOpen(false);
                        }}
                      >
                        {country}
                      </div>
                    ))}
                  </div>
                )}
              </div>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Workplace</label>
              <input
                type="text"
                value={formData.workplace || ''}
                onChange={(e) => setFormData({ ...formData, workplace: e.target.value })}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Payment Method</label>
              <input
                type="text"
                value={formData.payment_method || ''}
                onChange={(e) => setFormData({ ...formData, payment_method: e.target.value })}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Speaker Type</label>
              <input
                type="text"
                value={formData.speaker_type || ''}
                onChange={(e) => setFormData({ ...formData, speaker_type: e.target.value })}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700">Progress</label>
              <select
                value={progress}
                onChange={e => setProgress(e.target.value)}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
              >
                <option value="none">None</option>
                <option value="pause">Pause</option>
                <option value="continue">Continue</option>
              </select>
            </div>
            {/* Conditionally show form_link/payment_link only in edit mode and correct stage */}
            {customer.id && selectedStage === 'forms' && (
              <div className="col-span-2">
                <label className="block text-sm font-medium text-gray-700">Form Link</label>
                <input
                  type="text"
                  value={formData.form_link || ''}
                  onChange={e => setFormData({ ...formData, form_link: e.target.value })}
                  className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                  placeholder="https://..."
                />
              </div>
            )}
            {customer.id && selectedStage === 'payments' && (
              <div className="col-span-2">
                <label className="block text-sm font-medium text-gray-700">Payment Link</label>
                <input
                  type="text"
                  value={formData.payment_link || ''}
                  onChange={e => setFormData({ ...formData, payment_link: e.target.value })}
                  className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                  placeholder="https://..."
                />
              </div>
            )}
            <div>
              <label className="block text-sm font-medium text-gray-700">Date2 (any format)</label>
              <input
                type="text"
                value={date2}
                onChange={e => setDate2(e.target.value)}
                className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                placeholder="e.g. 12 Jun, May 12-2, etc."
              />
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700">Notes</label>
            <textarea
              value={formData.notes || ''}
              onChange={(e) => setFormData({ ...formData, notes: e.target.value })}
              rows={3}
              className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
            />
          </div>

          <div className="flex justify-end space-x-3 pt-4">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 border border-gray-300 rounded-md text-gray-700 hover:bg-gray-50"
            >
              Cancel
            </button>
            <button
              type="submit"
              className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
            >
              {customer.id ? 'Update' : 'Add'} Customer
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}