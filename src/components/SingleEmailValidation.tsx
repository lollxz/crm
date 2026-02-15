import React, { useState } from 'react';
// Using the backend /validate API instead of local helper
import { Button } from '@/components/ui/button';
import { toast } from 'react-toastify';

export default function SingleEmailValidation() {
  const [email, setEmail] = useState('');
  const [loading, setLoading] = useState(false);
  const [result, setResult] = useState<any>(null);

  const handleValidate = async () => {
    if (!email) {
      toast.error('Please enter an email');
      return;
    }
    setLoading(true);
    setResult(null);
    try {
  const API_BASE = 'https://62.171.152.239:5000';
  const apiUrl = `${API_BASE}/validate`;
      const resp = await fetch(apiUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email: email.trim() })
      });
      if (!resp.ok) {
        const txt = await resp.text();
        throw new Error(txt || `HTTP ${resp.status}`);
      }
      const resJson = await resp.json();
      // Normalize older/newer response shapes
      const normalized: any = {
        email: resJson.email || resJson.data?.email || email.trim(),
        valid: resJson.valid ?? (resJson.status === 'valid'),
        reason: resJson.reason || resJson.validation_result || resJson.status || null,
        score: resJson.score ?? null,
        risk_level: resJson.risk_level ?? null,
        is_catch_all: resJson.is_catch_all ?? resJson.catch_all ?? false,
        smtp_code: resJson.smtp_code ?? null,
        details: resJson.details ?? { log: resJson.log ?? [] },
        raw: resJson
      };
      // Ensure details.log is an array
      if (!normalized.details || !Array.isArray(normalized.details.log)) {
        normalized.details = { log: [] };
      }
      setResult(normalized);
      toast.success('Validation completed');
    } catch (err: any) {
      console.error('Validation error', err);
      toast.error('Validation failed: ' + (err.message || err));
    } finally {
      setLoading(false);
    }
  };

  const handleCopy = () => {
    if (!result) return;
    navigator.clipboard.writeText(JSON.stringify(result, null, 2));
    toast.info('Result copied to clipboard');
  };

  const handleSaveToContacts = async () => {
    if (!result) return;
    try {
      const token = localStorage.getItem('token');
  const API_BASE = 'https://62.171.152.239:5000';
  const resp = await fetch(`${API_BASE}/validation/save-to-contacts`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`
        },
        body: JSON.stringify({
          email: result.email,
          validation_result: result.reason || result.validation_result || null,
          score: result.score,
          risk_level: result.risk_level,
          catchall_analysis: result.catchall_analysis || null
        })
      });
      if (!resp.ok) {
        const txt = await resp.text();
        throw new Error(txt || `HTTP ${resp.status}`);
      }
      toast.success('Saved validation to contacts');
    } catch (err: any) {
      console.error('Save to contacts failed', err);
      toast.error('Save failed: ' + (err.message || err));
    }
  };

  return (
    <div className="p-4 max-w-xl">
      <h2 className="text-lg font-semibold mb-2">Single Email Validation</h2>
      <div className="flex gap-2 mb-4">
        <input
          type="email"
          value={email}
          onChange={(e: React.ChangeEvent<HTMLInputElement>) => setEmail(e.target.value)}
          placeholder="Enter email to validate"
          className="flex-1 border rounded px-2 py-1"
        />
        <Button onClick={handleValidate} disabled={loading}>
          {loading ? 'Validating...' : 'Validate'}
        </Button>
      </div>

      {result && (
        <div className="border rounded p-3 bg-white">
          <div className="mb-2">
            <strong>Email:</strong> {result.email}
          </div>
          <div className="mb-2 grid grid-cols-2 gap-2">
            <div><strong>Valid:</strong> {String(result.valid)}</div>
            <div><strong>Catch-All:</strong> {String(result.is_catch_all)}</div>
            <div><strong>Reason:</strong> {result.reason}</div>
            <div><strong>SMTP code:</strong> {result.smtp_code ?? '-'}</div>
            <div><strong>Score:</strong> {result.score ?? '-'}</div>
            <div><strong>Risk level:</strong> {result.risk_level ?? '-'}</div>
          </div>

          {/* Logs */}
          <div className="mt-3">
            <strong>Logs:</strong>
            <div className="max-h-56 overflow-auto bg-gray-50 border rounded p-2 mt-2 text-sm">
              {result.details && Array.isArray(result.details.log) && result.details.log.length > 0 ? (
                result.details.log.map((line: string, idx: number) => (
                  <div key={idx} className="whitespace-pre-wrap">{line}</div>
                ))
              ) : (
                <div className="text-muted">No logs available</div>
              )}
            </div>
          </div>
          <div className="flex gap-2 mt-2">
            <Button onClick={handleCopy}>Copy JSON</Button>
            <Button onClick={handleSaveToContacts}>Save to Contacts</Button>
          </div>
        </div>
      )}
    </div>
  );
}
