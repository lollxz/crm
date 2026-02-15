import { useState, useEffect } from 'react';
import { authFetch } from '../utils/authFetch';
import '../styles/CustomMessagesModal.css';

interface Message {
  type: string;
  label: string;
  is_custom: boolean;
  subject: string;
  body: string;
  created_at?: string;
  updated_at?: string;
}

interface CustomMessagesModalProps {
  contactId: number;
  contactName: string;
  isOpen: boolean;
  onClose: () => void;
}

export function CustomMessagesModal({
  contactId,
  contactName,
  isOpen,
  onClose,
}: CustomMessagesModalProps) {
  const [messages, setMessages] = useState<Message[]>([]);
  const [selectedMessage, setSelectedMessage] = useState<Message | null>(null);
  const [subject, setSubject] = useState('');
  const [body, setBody] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState(false);

  // Load messages when modal opens
  useEffect(() => {
    if (isOpen) {
      loadMessages();
    }
  }, [isOpen, contactId]);

  // Update subject/body when selected message changes
  useEffect(() => {
    if (selectedMessage) {
      setSubject(selectedMessage.subject);
      setBody(selectedMessage.body);
    }
  }, [selectedMessage]);

  const loadMessages = async () => {
    setLoading(true);
    setError('');
    try {
      console.log(`[CustomMessagesModal] Loading messages for contact ${contactId}`);
      const response = await authFetch(
        `/campaign_contacts/${contactId}/messages`
      );
      console.log(`[CustomMessagesModal] Response status: ${response.status}, ok: ${response.ok}`);
      console.log(`[CustomMessagesModal] Response headers:`, response.headers);
      
      if (!response.ok) {
        const responseText = await response.text();
        console.error(`[CustomMessagesModal] Error response body:`, responseText.substring(0, 200));
        throw new Error(`Failed to load messages (${response.status})`);
      }
      
      const data = await response.json();
      console.log(`[CustomMessagesModal] Successfully loaded ${data.length} messages`);
      setMessages(data);
      if (data.length > 0) {
        setSelectedMessage(data[0]);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load messages');
      console.error('Error loading messages:', err);
    } finally {
      setLoading(false);
    }
  };

  const handleSave = async () => {
    if (!selectedMessage) return;
    if (!subject.trim()) {
      setError('Subject cannot be empty');
      return;
    }
    if (!body.trim()) {
      setError('Body cannot be empty');
      return;
    }

    setSaving(true);
    setError('');
    try {
      const response = await authFetch(
        `/campaign_contacts/${contactId}/messages/${selectedMessage.type}`,
        {
          method: 'POST',
          body: JSON.stringify({
            subject: subject.trim(),
            body: body.trim(),
          }),
        }
      );

      if (!response.ok) {
        throw new Error('Failed to save message');
      }

      // Reload messages to reflect changes
      await loadMessages();
      setError('');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to save message');
      console.error('Error saving message:', err);
    } finally {
      setSaving(false);
    }
  };

  const handleDelete = async () => {
    if (!selectedMessage) return;
    if (!window.confirm(`Delete custom message for ${selectedMessage.label}?`)) {
      return;
    }

    setDeleting(true);
    setError('');
    try {
      const response = await authFetch(
        `/campaign_contacts/${contactId}/messages/${selectedMessage.type}`,
        {
          method: 'DELETE',
        }
      );

      if (!response.ok) {
        throw new Error('Failed to delete message');
      }

      // Reload messages to reflect changes
      await loadMessages();
      setError('');
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to delete message');
      console.error('Error deleting message:', err);
    } finally {
      setDeleting(false);
    }
  };

  if (!isOpen) return null;

  return (
    <div className="custom-messages-modal-overlay" onClick={onClose}>
      <div
        className="custom-messages-modal-content"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="modal-header">
          <h2>Edit Messages for {contactName}</h2>
          <button
            className="close-button"
            onClick={onClose}
            aria-label="Close modal"
          >
            ✕
          </button>
        </div>

        {/* Error message */}
        {error && <div className="error-message">{error}</div>}

        <div className="modal-body">
          {/* Left panel: Message list */}
          <div className="messages-list-panel">
            <div className="panel-title">Message Flows</div>
            {loading ? (
              <div className="loading">Loading messages...</div>
            ) : messages.length === 0 ? (
              <div className="no-messages">No messages available</div>
            ) : (
              <div className="messages-list">
                {messages.map((msg) => (
                  <div
                    key={msg.type}
                    className={`message-item ${
                      selectedMessage?.type === msg.type ? 'active' : ''
                    }`}
                    onClick={() => setSelectedMessage(msg)}
                  >
                    <div className="message-item-label">{msg.label}</div>
                    {msg.is_custom && (
                      <span className="badge custom">Custom</span>
                    )}
                    {!msg.is_custom && (
                      <span className="badge template">Template</span>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Right panel: Message editor */}
          <div className="message-editor-panel">
            {selectedMessage ? (
              <>
                <div className="form-group">
                  <label htmlFor="subject">Subject</label>
                  <input
                    id="subject"
                    type="text"
                    maxLength={500}
                    value={subject}
                    onChange={(e) => setSubject(e.target.value)}
                    placeholder="Enter message subject"
                  />
                  <div className="char-count">
                    {subject.length}/500 characters
                  </div>
                </div>

                <div className="form-group">
                  <label htmlFor="body">Message Body</label>
                  <textarea
                    id="body"
                    rows={12}
                    value={body}
                    onChange={(e) => setBody(e.target.value)}
                    placeholder="Enter message body"
                  />
                </div>

                {selectedMessage.updated_at && (
                  <div className="message-meta">
                    Last updated:{' '}
                    {new Date(selectedMessage.updated_at).toLocaleString()}
                  </div>
                )}
              </>
            ) : (
              <div className="no-selection">
                Select a message to edit
              </div>
            )}
          </div>
        </div>

        {/* Footer */}
        <div className="modal-footer">
          <button
            className="button button-secondary"
            onClick={onClose}
            disabled={saving || deleting}
          >
            Close
          </button>
          {selectedMessage && selectedMessage.is_custom && (
            <button
              className="button button-danger"
              onClick={handleDelete}
              disabled={saving || deleting}
            >
              {deleting ? 'Deleting...' : 'Delete Custom'}
            </button>
          )}
          <button
            className="button button-primary"
            onClick={handleSave}
            disabled={saving || deleting || !selectedMessage}
          >
            {saving ? 'Saving...' : 'Save Message'}
          </button>
        </div>
      </div>
    </div>
  );
}
