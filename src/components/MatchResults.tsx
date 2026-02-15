import React from 'react';
import { Download, X } from 'lucide-react';
import { MatchResult, GroupedMatches } from '../types';
import * as XLSX from 'xlsx';

interface MatchResultsProps {
  matches: MatchResult[];
  onClose: () => void;
}

export function MatchResults({ matches, onClose }: MatchResultsProps) {
  const groupedMatches: GroupedMatches = {
    nameMatches: matches.filter(m => m.type === 'name'),
    emailMatches: matches.filter(m => m.type === 'email'),
    orgMatches: matches.filter(m => m.type === 'organization')
  };

  const handleExportMatches = () => {
    const workbook = XLSX.utils.book_new();
    
    // Name matches worksheet
    if (groupedMatches.nameMatches.length > 0) {
      const nameSheet = XLSX.utils.json_to_sheet(groupedMatches.nameMatches.map(match => ({
        'Match Type': 'Name',
        'Excel Value': match.excel_value,
        'Database Value': match.db_value,
        'Event': match.event_name,
        'Role': match.role,
        'Stage': match.stage || '',
        'Status': match.status || ''
      })));
      XLSX.utils.book_append_sheet(workbook, nameSheet, 'Name Matches');
    }
    
    // Email matches worksheet
    if (groupedMatches.emailMatches.length > 0) {
      const emailSheet = XLSX.utils.json_to_sheet(groupedMatches.emailMatches.map(match => ({
        'Match Type': 'Email',
        'Excel Email': match.excel_value,
        'Database Email': match.db_value,
        'Event': match.event_name,
        'Stage': match.stage || '',
        'Status': match.status || ''
      })));
      XLSX.utils.book_append_sheet(workbook, emailSheet, 'Email Matches');
    }
    
    // Organization matches worksheet
    if (groupedMatches.orgMatches.length > 0) {
      const orgSheet = XLSX.utils.json_to_sheet(groupedMatches.orgMatches.map(match => ({
        'Organization': match.excel_value,
        'Match Details': match.match_details || 'Found in database'
      })));
      XLSX.utils.book_append_sheet(workbook, orgSheet, 'Organization Matches');
    }
    
    XLSX.writeFile(workbook, 'matching_results.xlsx');
  };

  return (
    <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
      <div className="bg-white rounded-lg p-8 max-w-6xl w-full max-h-[80vh] overflow-hidden flex flex-col">
        <div className="flex justify-between items-center mb-4">
          <h2 className="text-2xl font-bold">Match Results</h2>
          <div className="flex space-x-4">
            {matches.length > 0 && (
              <button
                onClick={handleExportMatches}
                className="flex items-center space-x-2 px-4 py-2 bg-green-600 text-white rounded-md hover:bg-green-700"
              >
                <Download size={20} />
                <span>Export Matches</span>
              </button>
            )}
            <button
              onClick={onClose}
              className="text-gray-500 hover:text-gray-700"
            >
              <X size={24} />
            </button>
          </div>
        </div>

        <div className="overflow-y-auto flex-1">
          {Object.values(groupedMatches).every(group => group.length === 0) ? (
            <div className="flex flex-col items-center justify-center py-16">
              <div className="text-5xl text-gray-300 mb-4">🔍</div>
              <h3 className="text-2xl font-semibold text-gray-700 mb-2">No matches found</h3>
              <p className="text-gray-500 text-center max-w-md">
                No matching records were found in the database for the uploaded data.
              </p>
            </div>
          ) : (
            <div className="space-y-6">
              {/* Name Matches Section */}
              {groupedMatches.nameMatches.length > 0 && (
                <div className="bg-gray-50 p-4 rounded-lg">
                  <h3 className="text-xl font-semibold mb-3">Name Matches</h3>
                  <div className="space-y-2">
                    {groupedMatches.nameMatches.map((match, index) => (
                      <div key={index} className="bg-white p-3 rounded border border-gray-200">
                        <div className="flex justify-between items-start">
                          <div>
                            <p className="font-medium">
                              Excel Name: {match.excel_value} → Database: {match.db_value}
                            </p>
                            <p className="text-sm text-gray-600">
                              Role: {match.role} | Event: {match.event_name}
                            </p>
                            {match.stage && (
                              <p>
                                <span className="text-sm px-2 py-1 bg-blue-100 text-blue-800 rounded-full">
                                  Stage: {match.stage}
                                </span>
                              </p>
                            )}
                            {match.status && (
                              <p>
                                <span className="text-sm px-2 py-1 bg-green-100 text-green-800 rounded-full">
                                  Status: {match.status}
                                </span>
                              </p>
                            )}
                          </div>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Email Matches Section */}
              {groupedMatches.emailMatches.length > 0 && (
                <div className="bg-gray-50 p-4 rounded-lg">
                  <h3 className="text-xl font-semibold mb-3">Email Matches</h3>
                  <div className="space-y-2">
                    {groupedMatches.emailMatches.map((match, index) => (
                      <div key={index} className="bg-white p-3 rounded border border-gray-200">
                        <p className="font-medium">
                          Excel Email: {match.excel_value} → Database: {match.db_value}
                        </p>
                        <p className="text-sm text-gray-600">Event: {match.event_name}</p>
                        {match.stage && (
                          <p>
                            <span className="text-sm px-2 py-1 bg-blue-100 text-blue-800 rounded-full">
                              Stage: {match.stage}
                            </span>
                          </p>
                        )}
                        {match.status && (
                          <p>
                            <span className="text-sm px-2 py-1 bg-green-100 text-green-800 rounded-full">
                              Status: {match.status}
                            </span>
                          </p>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* Organization Matches Section */}
              {groupedMatches.orgMatches.length > 0 && (
                <div className="bg-gray-50 p-4 rounded-lg">
                  <h3 className="text-xl font-semibold mb-3">Organization Matches</h3>
                  <div className="space-y-2">
                    {groupedMatches.orgMatches.map((match, index) => (
                      <div key={index} className="bg-white p-3 rounded border border-gray-200">
                        <p className="font-medium">{match.excel_value}</p>
                        {match.match_details && (
                          <p className="text-sm text-gray-600">{match.match_details}</p>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}