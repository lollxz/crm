import React, { useState, useCallback } from 'react';
import { Search } from 'lucide-react';
import { SUPPLIER_OPTIONS } from '../types';

interface SearchBarProps {
  value: string;
  onChange: (value: string, selectedColumns: string[]) => void;
}

export function SearchBar({ value, onChange }: SearchBarProps) {
  const [inputValue, setInputValue] = useState(value);
  const [selectedColumns, setSelectedColumns] = useState<string[]>(['all']);
  const [selectedSupplierFilter, setSelectedSupplierFilter] = useState<string>('all');

  const handleSearch = useCallback(() => {
    // Split the search query by '+' to handle multiple column searches
    const searchTerms = inputValue.split('+').map(term => term.trim());
    onChange(inputValue, selectedColumns);
  }, [inputValue, selectedColumns, onChange]);

  const handleKeyPress = (e: React.KeyboardEvent) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  const handleColumnChange = useCallback((columnKey: string) => {
    let newSelectedColumns: string[];

    if (columnKey === 'all') {
      newSelectedColumns = selectedColumns.includes('all') ? [] : ['all'];
    } else {
      newSelectedColumns = selectedColumns.filter(col => col !== 'all');
      if (selectedColumns.includes(columnKey)) {
        newSelectedColumns = newSelectedColumns.filter(col => col !== columnKey);
      } else {
        newSelectedColumns.push(columnKey);
      }

      if (newSelectedColumns.length === 0) {
        newSelectedColumns = ['all'];
      }
    }

    setSelectedColumns(newSelectedColumns);
  }, [selectedColumns]);

  const columns = [
    { key: 'all', label: 'All Columns' },
    { key: 'event_name', label: 'Event Name' },
    { key: 'org_name', label: 'Organization' },
    { key: 'month', label: 'Month' },
    { key: 'name', label: 'Name' },
    { key: 'email', label: 'Email' },
    { key: 'source', label: 'Source' },
    { key: 'validation_result', label: 'Validation' },
    { key: 'organizer', label: 'Organizer' },
    { key: 'date', label: 'Date' },
    { key: 'sender_email', label: 'Sender Email' },
    { key: 'stage', label: 'Stage' },
    { key: 'status', label: 'Status' },
    { key: 'hotel_name', label: 'Hotel Name' },
    { key: 'supplier', label: 'Supplier' },
    { key: 'notes', label: 'Notes' },
    { key: 'nationality', label: 'Nationality' },
    { key: 'workplace', label: 'Workplace' },
    { key: 'payment_method', label: 'Payment Method' },
    { key: 'speaker_type', label: 'Speaker Type' }
  ];

  return (
    <div className="w-full max-w-4xl">
      <div className="relative flex items-center">
        <Search className="absolute left-3 text-gray-400" size={20} />
        <input
          type="text"
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
          onKeyPress={handleKeyPress}
          placeholder="Search... (Use + to search multiple terms, e.g., email + date)"
          className="w-full pl-10 pr-4 py-3 text-lg border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
        />
        <select
          value={selectedSupplierFilter}
          onChange={(e) => setSelectedSupplierFilter(e.target.value)}
          className="ml-2 px-4 py-3 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500"
        >
          <option value="all">All Suppliers</option>
          {SUPPLIER_OPTIONS.filter(option => option !== 'custom').map((supplier) => (
            <option key={supplier} value={supplier}>
              {supplier}
            </option>
          ))}
        </select>
        <button
          onClick={handleSearch}
          className="ml-2 px-4 py-3 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
        >
          Search
        </button>
      </div>
      {/* Removed selected columns UI for a cleaner interface */}
    </div>
  );
}