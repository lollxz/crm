import React, { useState } from 'react';
import { Upload, Loader2 } from 'lucide-react';
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { authFetch } from '@/utils/authFetch';

interface ExcelUploadProps {
  onUpload: (file: File) => void;
  isProcessing: boolean;
}

export function ExcelUpload({ onUpload, isProcessing }: ExcelUploadProps) {
  const [isUploading, setIsUploading] = useState(false);
  const [showNotification, setShowNotification] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;
    setIsUploading(true);
    try {
      await onUpload(file);
    } finally {
      setIsUploading(false);
      event.target.value = '';
    }
  };

  return (
    <div className="relative">
      <input
        type="file"
        accept=".xlsx,.xls"
        onChange={handleFileUpload}
        className="hidden"
        id="excel-upload"
        disabled={isUploading || isProcessing}
      />
      <label
        htmlFor="excel-upload"
        className={`flex items-center space-x-2 px-4 py-2 ${
          isUploading || isProcessing ? 'bg-gray-400' : 'bg-green-600 hover:bg-green-700'
        } text-white rounded-md cursor-pointer`}
      >
        {isUploading || isProcessing ? (
          <Loader2 size={20} className="animate-spin" />
        ) : (
          <Upload size={20} />
        )}
        <span>{isUploading || isProcessing ? 'Processing...' : 'Upload Excel'}</span>
      </label>
      
      {showNotification && (
        <div className="fixed top-4 right-4 p-4 bg-green-100 text-green-800 rounded-md shadow-lg z-50 animate-fade-in">
          Data uploaded and saved successfully!<br />
          <span style={{ fontSize: '0.9em' }}>
            (Unknown columns will be stored and logged. Required fields missing rows will be skipped.)
          </span>
        </div>
      )}
      
      {error && (
        <div className="fixed top-4 right-4 p-4 bg-red-100 text-red-800 rounded-md shadow-lg z-50 animate-fade-in">
          {error}
        </div>
      )}
    </div>
  );
}