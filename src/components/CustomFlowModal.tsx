import React, { useState, useEffect } from 'react';
import { Button, Modal, Input, Select, Space, Form, InputNumber, message } from 'antd';
import { PlusOutlined, DeleteOutlined, ArrowUpOutlined, ArrowDownOutlined } from '@ant-design/icons';
import { authFetch } from '../utils/authFetch';

const { TextArea } = Input;
const { Option } = Select;

interface Step {
  type: 'email';
  subject: string;
  body: string;
  delay_days: number;
}

interface CustomFlowModalProps {
  visible: boolean;
  contactId: number;
  flowId?: number;
  onCancel: () => void;
  onSuccess: () => void;
  initialSteps?: Step[];
}

const CustomFlowModal: React.FC<CustomFlowModalProps> = ({
  visible,
  contactId,
  flowId,
  onCancel,
  onSuccess,
  initialSteps = []
}) => {
  const [form] = Form.useForm();
  const [steps, setSteps] = useState<Step[]>(initialSteps);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    if (visible) {
      setSteps(initialSteps);
      form.setFieldsValue({ steps: initialSteps });
    }
  }, [visible, initialSteps, form]);

  const handleAddStep = () => {
    setSteps([
      ...steps,
      {
        type: 'email',
        subject: '',
        body: '',
        delay_days: 0
      }
    ]);
  };

  const handleRemoveStep = (index: number) => {
    const newSteps = [...steps];
    newSteps.splice(index, 1);
    setSteps(newSteps);
  };

  const handleMoveStep = (index: number, direction: 'up' | 'down') => {
    if (
      (direction === 'up' && index === 0) ||
      (direction === 'down' && index === steps.length - 1)
    ) {
      return;
    }

    const newSteps = [...steps];
    const targetIndex = direction === 'up' ? index - 1 : index + 1;
    [newSteps[index], newSteps[targetIndex]] = [newSteps[targetIndex], newSteps[index]];
    setSteps(newSteps);
  };

  const handleSubmit = async () => {
    try {
      const values = await form.validateFields();
      setLoading(true);

      const endpoint = flowId
        ? `/api/contacts/${contactId}/custom_flow/${flowId}`
        : `/api/contacts/${contactId}/custom_flow`;

      const method = flowId ? 'PUT' : 'POST';

      // For create (POST) the backend expects { contact_id, steps }
      const bodyPayload = flowId ? steps : { contact_id: contactId, steps };

      console.debug('[CustomFlowModal] Sending payload to', endpoint, bodyPayload);
      const response = await authFetch(endpoint, {
        method,
        body: JSON.stringify(bodyPayload),
        headers: { 'Content-Type': 'application/json' }
      });

      if (response.ok) {
        message.success(flowId ? 'Flow updated successfully' : 'Flow created successfully');
        onSuccess();
      } else {
        // Try to extract server validation details
        let text = '';
        try {
          const data = await response.json();
          console.error('[CustomFlowModal] Server validation error', data);
          text = JSON.stringify(data);
          message.error((data && (data.detail || data.message)) ? (data.detail || JSON.stringify(data.detail)) : 'Failed to save flow');
        } catch (e) {
          const raw = await response.text();
          console.error('[CustomFlowModal] Server returned non-json:', raw);
          text = raw;
          message.error('Failed to save flow: ' + raw);
        }
        throw new Error('Failed to save flow: ' + text);
      }
    } catch (error) {
      message.error('Error saving flow: ' + (error as Error).message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <Modal
      title={flowId ? "Edit Follow-up Flow" : "Create Follow-up Flow"}
      open={visible}
      width={800}
      onCancel={onCancel}
      footer={[
        <Button key="cancel" onClick={onCancel}>
          Cancel
        </Button>,
        <Button 
          key="submit" 
          type="primary" 
          loading={loading}
          onClick={handleSubmit}
        >
          {flowId ? 'Update' : 'Create'}
        </Button>
      ]}
    >
      <Form
        form={form}
        layout="vertical"
        initialValues={{ steps }}
      >
        <div style={{ maxHeight: '60vh', overflowY: 'auto', padding: '0 20px' }}>
          {steps.map((step, index) => (
            <div 
              key={index}
              style={{ 
                border: '1px solid #d9d9d9',
                borderRadius: '4px',
                padding: '16px',
                marginBottom: '16px',
                position: 'relative'
              }}
            >
              <Space 
                style={{ 
                  position: 'absolute',
                  right: '16px',
                  top: '16px'
                }}
              >
                <Button
                  icon={<ArrowUpOutlined />}
                  disabled={index === 0}
                  onClick={() => handleMoveStep(index, 'up')}
                />
                <Button
                  icon={<ArrowDownOutlined />}
                  disabled={index === steps.length - 1}
                  onClick={() => handleMoveStep(index, 'down')}
                />
                <Button
                  danger
                  icon={<DeleteOutlined />}
                  onClick={() => handleRemoveStep(index)}
                />
              </Space>

              <Form.Item
                label="Step Type"
                required
                style={{ marginBottom: '12px' }}
              >
                <Select
                  value={step.type}
                  onChange={(value) => {
                    const newSteps = [...steps];
                    newSteps[index].type = 'email';
                    setSteps(newSteps);
                  }}
                >
                  <Option value="email">Email</Option>
                </Select>
              </Form.Item>

              <Form.Item
                label="Delay (days)"
                required
                style={{ marginBottom: '12px' }}
              >
                <InputNumber
                  min={0}
                  value={index === 0 ? 0 : step.delay_days}
                  disabled={index === 0}
                  onChange={(value) => {
                    if (index === 0) return; // first step delay fixed to 0
                    const newSteps = [...steps];
                    newSteps[index].delay_days = value || 0;
                    setSteps(newSteps);
                  }}
                />
              </Form.Item>

              <Form.Item
                label="Subject"
                required
                style={{ marginBottom: '12px' }}
              >
                <Input
                  value={step.subject}
                  onChange={(e) => {
                    const newSteps = [...steps];
                    newSteps[index].subject = e.target.value;
                    setSteps(newSteps);
                  }}
                />
              </Form.Item>

              <Form.Item
                label="Content"
                required
                style={{ marginBottom: '12px' }}
              >
                <TextArea
                  rows={4}
                  value={step.body}
                  onChange={(e) => {
                    const newSteps = [...steps];
                    newSteps[index].body = e.target.value;
                    setSteps(newSteps);
                  }}
                />
              </Form.Item>
            </div>
          ))}
        </div>

        <Button 
          type="dashed" 
          onClick={handleAddStep} 
          block
          icon={<PlusOutlined />}
          style={{ marginTop: '16px' }}
        >
          Add Step
        </Button>
      </Form>
    </Modal>
  );
};

export default CustomFlowModal;
