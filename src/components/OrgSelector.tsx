import React, { useEffect, useState } from 'react';
import { authFetch } from '@/utils/authFetch';
import stringSimilarity from 'string-similarity';

type Props = {
  value?: any;
  onChange?: (val: any) => void;
  onCreated?: () => void;
};

export default function OrgSelector({ value, onChange, onCreated }: Props) {
  const [orgs, setOrgs] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [showAdd, setShowAdd] = useState(false);
  const [newName, setNewName] = useState('');
  const [isAdmin, setIsAdmin] = useState(false);
  const [similarOrgs, setSimilarOrgs] = useState<any[]>([]);

  useEffect(() => {
    const API_BASE = import.meta.env.VITE_API_URL || 'https://conferencecare.org/api';
    const fetchOrgs = async () => {
      try {
        const res = await authFetch(`${API_BASE}/organizations/`);
        if (res.ok) {
          const data = await res.json();
          setOrgs(data || []);
        }
      } catch {}
    };
    fetchOrgs();

    try {
      const token = localStorage.getItem('token');
      if (token) {
        const payload = JSON.parse(atob(token.split('.')[1]));
        setIsAdmin(Boolean(payload.is_admin));
      }
    } catch {}
  }, []);

  const handleAdd = async () => {
    if (!newName.trim()) return;
    setLoading(true);
    try {
      const API_BASE = import.meta.env.VITE_API_URL || 'https://conferencecare.org/api';
      const dataRes = await authFetch(`${API_BASE}/organizations/add`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: newName }),
      });

      if (!dataRes.ok) {
        let detail = '';
        try {
          const clone = dataRes.clone();
          const j = await clone.json();
          detail = j.detail || JSON.stringify(j);
        } catch {
          try {
            detail = await dataRes.text();
          } catch {
            detail = `HTTP ${dataRes.status}`;
          }
        }
        throw new Error(`Server returned ${dataRes.status}: ${detail}`);
      }

      const created = await dataRes.json();
      try {
        const refetch = await authFetch('/organizations/');
        if (refetch.ok) setOrgs(await refetch.json());
      } catch {}
      onChange?.(created.id);
      setShowAdd(false);
      setNewName('');
      onCreated?.();
    } catch (err: any) {
      alert('Failed to create organization: ' + (err.message || err));
    } finally {
      setLoading(false);
    }
  };

  const handleNewNameChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const input = e.target.value;
    setNewName(input);

    const matches = orgs.filter((org: { id: number; name: string }) => {
      const similarity = stringSimilarity.compareTwoStrings(input.toLowerCase(), org.name.toLowerCase());
      return similarity > 0.6;
    });

    setSimilarOrgs(matches);
  };

  return (
    <div className="flex items-center gap-2">
      <select
        className="border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-500 w-full"
        value={value ?? ''}
        onChange={(e: React.ChangeEvent<HTMLSelectElement>) =>
          onChange?.(e.target.value ? Number(e.target.value) : '')
        }
      >
        <option value="">-- Select organization --</option>
        {orgs.map((o: any) => (
          <option key={o.id} value={o.id}>
            {o.name}
          </option>
        ))}
      </select>

      {isAdmin && (
        <button
          type="button"
          onClick={() => setShowAdd(true)}
          className="bg-blue-600 text-white text-sm px-3 py-2 rounded-lg hover:bg-blue-700 transition"
        >
          + Add Org
        </button>
      )}

      {showAdd && (
        <div className="fixed inset-0 flex items-center justify-center bg-black bg-opacity-40 z-50">
          <div className="bg-white rounded-2xl shadow-xl p-6 w-full max-w-md">
            <h3 className="text-lg font-semibold mb-4">Add Organization</h3>

            <input
              value={newName}
              onChange={handleNewNameChange}
              placeholder="Organization name"
              className="border border-gray-300 rounded-lg w-full p-2 focus:outline-none focus:ring-2 focus:ring-blue-500"
            />

            {similarOrgs.length > 0 && (
              <div className="mt-3 bg-gray-50 border border-gray-200 rounded-lg p-2">
                <p className="text-sm font-medium text-gray-600 mb-1">
                  Similar organizations found:
                </p>
                <ul className="text-sm text-gray-700 list-disc pl-5 space-y-1">
                  {similarOrgs.map((org: { id: number; name: string }) => (
                    <li key={org.id}>{org.name}</li>
                  ))}
                </ul>
              </div>
            )}

            <div className="flex justify-end gap-3 mt-5">
              <button
                onClick={() => setShowAdd(false)}
                className="px-4 py-2 text-gray-600 hover:bg-gray-100 rounded-lg"
              >
                Cancel
              </button>
              <button
                onClick={handleAdd}
                disabled={loading}
                className="px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition disabled:opacity-50"
              >
                {loading ? 'Adding...' : 'Add'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
