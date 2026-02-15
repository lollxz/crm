import React from 'react';
import { Button } from '@/components/ui/button';
import { usePreviewResults } from '../contexts/PreviewResultsContext';

const getMatchStatusStyles = (status: string) => {
  const s = status?.toLowerCase() || '';
  if (s.includes('active') || s.includes('won') || s.includes('qualified')) {
    return 'bg-green-100 text-green-700 border-green-200';
  }
  if (s.includes('lost') || s.includes('inactive')) {
    return 'bg-red-100 text-red-700 border-red-200';
  }
  return 'bg-gray-100 text-gray-700 border-gray-200';
};

const getRowStatusBadge = (status: string) => {
  const s = status?.toLowerCase() || '';
  if (s === 'added') {
    return <span className="px-2 py-1 rounded-full text-xs font-bold bg-green-100 text-green-700 border border-green-200">Added</span>;
  }
  if (s === 'updated') {
    return <span className="px-2 py-1 rounded-full text-xs font-bold bg-blue-100 text-blue-700 border border-blue-200">Updated</span>;
  }
  if (s === 'skipped') {
    return <span className="px-2 py-1 rounded-full text-xs font-bold bg-red-100 text-red-700 border border-red-200">Refused</span>;
  }
  return <span className="px-2 py-1 rounded-full text-xs font-medium bg-gray-100 text-gray-700 border border-gray-200">{status}</span>;
};

export const GlobalUploadResultsModal: React.FC = () => {
  const { previewResults, setPreviewResults, open, setOpen } = usePreviewResults();

  if (!open) return null;

  const close = () => {
    setOpen(false);
    setPreviewResults(null);
  };

  const renderMatches = (matchesInput: any, excelName: string) => {
    if (!matchesInput) return null;

    let parsed = [];
    try {
      parsed = typeof matchesInput === 'string' ? JSON.parse(matchesInput) : matchesInput;
    } catch (e) {
      return <span className="text-gray-400 text-xs">{String(matchesInput)}</span>;
    }

    if (!Array.isArray(parsed) || parsed.length === 0) return null;

    // Normalize Excel name
    const normExcelName = (excelName || '').trim().toLowerCase();

    return (
      <div className="flex flex-col gap-2">
        {parsed.map((m: any, idx: number) => {
          // STRICT MATCHING LOGIC:
          // Only show name if Excel Name matches Database Name
          const normMatchName = (m.name || '').trim().toLowerCase();
          const isNameMatch = normExcelName && normMatchName && (normExcelName === normMatchName);

          return (
            <div 
              key={idx} 
              className="flex flex-wrap items-center gap-2 text-xs bg-white border border-gray-200 rounded-md p-2 shadow-sm"
            >
              {/* Only render this block if names match exactly */}
              {isNameMatch && (
                <div className="flex items-center font-bold text-gray-900 border-r pr-2 mr-1">
                  <span className="truncate max-w-[100px]" title={m.name}>{m.name}</span>
                </div>
              )}

              <div className="flex items-center gap-1 text-gray-600">
                <span className="text-gray-400 font-normal">Event:</span>
                <span className="font-mono">{m.event_id}</span>
              </div>

              {m.stage && (
                <span className="px-2 py-0.5 rounded-full bg-blue-50 text-blue-700 border border-blue-100 font-medium">
                  {m.stage}
                </span>
              )}

              {m.status && (
                <span className={`px-2 py-0.5 rounded-full border font-medium ${getMatchStatusStyles(m.status)}`}>
                  {m.status}
                </span>
              )}
            </div>
          );
        })}
      </div>
    );
  };

  // Loading state
  if (previewResults === null) {
    return (
      <div className="fixed inset-0 flex items-center justify-center z-50 bg-black/20 backdrop-blur-sm">
        <div className="bg-white border rounded p-6 shadow-xl w-11/12 max-w-4xl">
          <h3 className="text-lg font-semibold mb-2">Upload Validation Results</h3>
          <div className="flex items-center gap-2 text-sm text-gray-600">
            <div className="animate-spin rounded-full h-4 w-4 border-b-2 border-blue-500"></div>
            <span>Processing...</span>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="fixed inset-0 flex items-center justify-center z-50 bg-black/20 backdrop-blur-sm">
      <div className="bg-white border rounded-lg shadow-2xl w-11/12 max-w-6xl max-h-[85vh] flex flex-col">
        <div className="p-6 border-b">
          <h3 className="text-xl font-bold text-gray-900">Upload Validation Results</h3>
          <p className="text-sm text-gray-500 mt-1">Showing {previewResults.length} rows.</p>
        </div>

        <div className="p-6 overflow-auto flex-1">
          {previewResults.every((r: any) => !(r.matches && (Array.isArray(r.matches) ? r.matches.length > 0 : (typeof r.matches === 'string' && r.matches !== '[]')))) && (
             <div className="p-4 rounded-md bg-green-50 border border-green-200 text-green-800 flex items-center gap-2">
               <span className="text-xl">✓</span> 
               No matches found — all rows appear new.
             </div>
          )}

          <table className="min-w-full text-sm border-collapse">
            <thead className="sticky top-0 bg-gray-50 z-10 shadow-sm">
              <tr>
                <th className="px-4 py-3 text-left font-semibold text-gray-600 border-b">Row</th>
                <th className="px-4 py-3 text-left font-semibold text-gray-600 border-b">Email</th>
                <th className="px-4 py-3 text-left font-semibold text-gray-600 border-b">Name (Excel)</th>
                <th className="px-4 py-3 text-left font-semibold text-gray-600 border-b">Status</th>
                <th className="px-4 py-3 text-left font-semibold text-gray-600 border-b">Validation</th>
                <th className="px-4 py-3 text-left font-semibold text-gray-600 border-b w-[40%]">Matches</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {previewResults.map((r: any, i: number) => (
                <tr key={i} className="hover:bg-gray-50 transition-colors">
                  <td className="px-4 py-3 text-gray-500 align-top">{r.row || i + 1}</td>
                  <td className="px-4 py-3 font-medium text-gray-900 align-top">{r.email || ''}</td>
                  <td className="px-4 py-3 text-gray-800 align-top">{r.name || '-'}</td>
                  <td className="px-4 py-3 align-top">{getRowStatusBadge(r.status)}</td>
                  <td className="px-4 py-3 text-gray-600 align-top">
                    {r.validation_result || ''}
                    {r.status === 'skipped' && r.reason && (
                      <div className="text-xs text-red-500 mt-1">{r.reason}</div>
                    )}
                  </td>
                  <td className="px-4 py-3 align-top">
                    {renderMatches(r.matches, r.name)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>

        <div className="p-4 border-t bg-gray-50 flex justify-end gap-2 rounded-b-lg">
          <Button onClick={close} variant="outline">Close</Button>
        </div>
      </div>
    </div>
  );
};

export default GlobalUploadResultsModal;
