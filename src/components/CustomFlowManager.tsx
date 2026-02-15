import React, { useState, useEffect } from 'react';
import { Card, Button, Tag, Timeline, Spin, message, Popconfirm } from 'antd';
import { PlusOutlined, PauseCircleOutlined, PlayCircleOutlined } from '@ant-design/icons';
import { authFetch } from '../utils/authFetch';
import CustomFlowModal from './CustomFlowModal';

interface Step {
  id: number;
  type: 'email' | 'task' | 'notification';
  subject: string;
  body: string;
  delay_days: number;
  status: 'pending' | 'in_progress' | 'completed' | 'error' | 'cancelled';
  last_execution?: string;
  next_execution?: string;
  error_message?: string;
}

interface Flow {
  id: number;
  contact_id: number;
  active: boolean;
  created_at: string;
  updated_at: string;
  steps: Step[];
}

interface CustomFlowManagerProps {
  contactId: number;
}

const CustomFlowManager: React.FC<CustomFlowManagerProps> = ({ contactId }) => {
  const [flow, setFlow] = useState<Flow | null>(null);
  const [loading, setLoading] = useState(true);
  const [modalVisible, setModalVisible] = useState(false);

  const fetchFlow = async () => {
    try {
      const response = await authFetch(`/api/contacts/${contactId}/custom_flow`);
      if (response.ok) {
        const data = await response.json();
        setFlow(data.flow_steps.length > 0 ? {
          id: data.flow_steps[0].flow_id,
          contact_id: contactId,
          active: true, // You'll need to add this to the API response
          created_at: new Date().toISOString(), // Add to API
          updated_at: new Date().toISOString(), // Add to API
          steps: data.flow_steps
        } : null);
      }
    } catch (error) {
      message.error('Error fetching flow: ' + (error as Error).message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchFlow();
  }, [contactId]);

  const handleToggleActive = async () => {
    if (!flow) return;

    try {
      const action = flow.active ? 'pause' : 'resume';
      const response = await authFetch(
        `/api/contacts/${contactId}/custom_flow/${flow.id}/${action}`,
        { method: 'POST' }
      );

      if (response.ok) {
        setFlow(prev => prev ? { ...prev, active: !prev.active } : null);
        message.success(`Flow ${action}d successfully`);
      } else {
        throw new Error(`Failed to ${action} flow`);
      }
    } catch (error) {
      message.error('Error updating flow: ' + (error as Error).message);
    }
  };

  const handleDelete = async () => {
    if (!flow) return;

    try {
      const response = await authFetch(
        `/api/contacts/${contactId}/custom_flow/${flow.id}`,
        { method: 'DELETE' }
      );

      if (response.ok) {
        setFlow(null);
        message.success('Flow deleted successfully');
      } else {
        throw new Error('Failed to delete flow');
      }
    } catch (error) {
      message.error('Error deleting flow: ' + (error as Error).message);
    }
  };

  const getStepIcon = (type: Step['type']) => {
    switch (type) {
      case 'email':
        return '📧';
      case 'task':
        return '📋';
      case 'notification':
        return '🔔';
      default:
        return '•';
    }
  };

  const getStatusColor = (status: Step['status']) => {
    switch (status) {
      case 'completed':
        return 'success';
      case 'in_progress':
        return 'processing';
      case 'error':
        return 'error';
      case 'cancelled':
        return 'default';
      default:
        return 'warning';
    }
  };

  if (loading) {
    return <Spin />;
  }

  return (
    <Card
      title="Custom Follow-up Flow"
      extra={
        <Button
          type="primary"
          icon={<PlusOutlined />}
          onClick={() => setModalVisible(true)}
        >
          {flow ? 'Edit Flow' : 'Create Flow'}
        </Button>
      }
    >
      {flow ? (
        <>
          <div style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between' }}>
            <div>
              <Tag color={flow.active ? 'green' : 'orange'}>
                {flow.active ? 'Active' : 'Paused'}
              </Tag>
              <Button
                type="text"
                icon={flow.active ? <PauseCircleOutlined /> : <PlayCircleOutlined />}
                onClick={handleToggleActive}
              >
                {flow.active ? 'Pause' : 'Resume'}
              </Button>
            </div>
            <Popconfirm
              title="Are you sure you want to delete this flow?"
              onConfirm={handleDelete}
              okText="Yes"
              cancelText="No"
            >
              <Button danger>Delete Flow</Button>
            </Popconfirm>
          </div>

          <Timeline>
            {flow.steps.map((step, index) => (
              <Timeline.Item
                key={step.id}
                dot={getStepIcon(step.type)}
              >
                <div style={{ marginBottom: 16 }}>
                  <div style={{ marginBottom: 8 }}>
                    <strong>{step.subject}</strong>
                    <Tag 
                      color={getStatusColor(step.status)}
                      style={{ marginLeft: 8 }}
                    >
                      {step.status}
                    </Tag>
                    {step.delay_days > 0 && (
                      <Tag>Delay: {step.delay_days} days</Tag>
                    )}
                  </div>
                  <div style={{ whiteSpace: 'pre-wrap' }}>{step.body}</div>
                  {step.error_message && (
                    <div style={{ color: '#ff4d4f', marginTop: 8 }}>
                      Error: {step.error_message}
                    </div>
                  )}
                  {step.last_execution && (
                    <div style={{ color: '#8c8c8c', fontSize: '0.9em', marginTop: 4 }}>
                      Last executed: {new Date(step.last_execution).toLocaleString()}
                    </div>
                  )}
                  {step.next_execution && step.status === 'pending' && (
                    <div style={{ color: '#8c8c8c', fontSize: '0.9em', marginTop: 4 }}>
                      Next execution: {new Date(step.next_execution).toLocaleString()}
                    </div>
                  )}
                </div>
              </Timeline.Item>
            ))}
          </Timeline>
        </>
      ) : (
        <div style={{ textAlign: 'center', padding: '32px 0' }}>
          <p>No custom follow-up flow configured</p>
          <Button
            type="primary"
            icon={<PlusOutlined />}
            onClick={() => setModalVisible(true)}
          >
            Create Flow
          </Button>
        </div>
      )}

      <CustomFlowModal
        visible={modalVisible}
        contactId={contactId}
        flowId={flow?.id}
        initialSteps={flow?.steps}
        onCancel={() => setModalVisible(false)}
        onSuccess={() => {
          setModalVisible(false);
          fetchFlow();
        }}
      />
    </Card>
  );
};

export default CustomFlowManager;
