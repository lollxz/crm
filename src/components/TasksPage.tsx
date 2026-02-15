import React, { useState, useEffect } from 'react';
import { useAuth } from '../contexts/AuthContext';
import { authFetch } from '../utils/authFetch';
import { format, isValid } from 'date-fns';
import DatePicker from 'react-datepicker';
import "react-datepicker/dist/react-datepicker.css";
import { CheckCircle, XCircle, Clock, AlertCircle } from 'lucide-react';
import { toast } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import { STAGE_OPTIONS, DEFAULT_STATUS_OPTIONS } from '../types';

interface Task {
  id: string;
  title: string;
  description: string;
  status: string;
  updated_at?: string;
  assigned_username?: string;
  creator_username?: string;
  customer_email?: string;
  customer_date?: string;
  customer_stage?: string;
  customer_status?: string;
  customer_notes?: string;
  assigned_to?: string;
  customer_id?: string | number;
  type?: string;
  due_date?: string | null;
}

// Safe date parser that prevents invalid dates from being passed to DatePicker
const safelyParseDate = (dateString: string | null | undefined): Date | null => {
  if (!dateString) return null;
  try {
    // Expecting DD/MM/YYYY
    const match = String(dateString).match(/^(\d{2})\/(\d{2})\/(\d{4})$/);
    if (match) {
      const [, day, month, year] = match;
      const date = new Date(Number(year), Number(month) - 1, Number(day));
      return isValid(date) ? date : null;
    }

    // Fallback to Date constructor
    const parsed = new Date(dateString);
    return isValid(parsed) ? parsed : null;
  } catch (e) {
    console.error(`Error parsing date: ${dateString}`, e);
    return null;
  }
};

export function TasksPage() {
  const { user } = useAuth();
  const [tasks, setTasks] = useState<Task[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [showNewTaskForm, setShowNewTaskForm] = useState(false);
  const [newTask, setNewTask] = useState({
    title: '',
    description: '',
    assigned_to: '',
    due_date: null as Date | null,
  });
  const [users, setUsers] = useState<{ id: string; username: string }[]>([]);
  const [deletingIds, setDeletingIds] = useState<string[]>([]);

  useEffect(() => {
    fetchTasks();
    if (user?.is_admin) {
      fetchUsers();
    }
  }, [user]);

  const fetchTasks = async () => {
    console.log("Fetching tasks...");
    setError(null);
    try {
      const response = await authFetch(`${import.meta.env.VITE_API_URL}/tasks`);
      
      if (!response.ok) {
        const errorText = await response.text();
        console.error('Error fetching tasks:', errorText);
        throw new Error(`Failed to fetch tasks: ${response.status} ${response.statusText}`);
      }
      
      const data = await response.json();
      console.log("Received tasks data:", data);
      
      if (!data || !Array.isArray(data.tasks)) {
        console.error('Invalid tasks data format:', data);
        throw new Error('Received invalid task data from server');
      }
      
      // Process the tasks data to ensure valid date formats
      const processedTasks = data.tasks.map((task: Task) => {
        // Handle customer_date safely
        if (task.customer_date) {
          try {
            // Keep the original string but ensure it's in DD/MM/YYYY format
            if (typeof task.customer_date === 'string') {
              // If already in DD/MM/YYYY format, keep it as is
              if (task.customer_date.match(/^\d{2}\/\d{2}\/\d{4}$/)) {
                console.log(`Date already in correct format: ${task.customer_date}`);
              } else {
                // Try to parse and format the date
                const parsedDate = safelyParseDate(task.customer_date);
                if (parsedDate) {
                  task.customer_date = format(parsedDate, 'dd/MM/yyyy');
                  console.log(`Formatted date: ${task.customer_date}`);
                } else {
                  console.warn(`Invalid date detected: ${task.customer_date}`);
                  // Set a placeholder empty string that won't crash DatePicker
                  task.customer_date = '';
                }
              }
            }
      } catch (e) {
        console.error(`Error handling date for customer ${task.customer_email || task.id}:`, e);
            // Set to empty string to avoid DatePicker errors
            task.customer_date = '';
          }
        }
        return task;
      });
      
      console.log("Processed tasks:", processedTasks);
      setTasks(processedTasks);
    } catch (error) {
      console.error('Failed to fetch tasks:', error);
      setError(error instanceof Error ? error.message : 'Unknown error fetching tasks');
      toast.error('Failed to fetch tasks');
    } finally {
      setLoading(false);
    }
  };

  const fetchUsers = async () => {
    try {
      const response = await authFetch(`${import.meta.env.VITE_API_URL}/auth/users`);
      const data = await response.json();
      setUsers(data.users);
    } catch (error) {
      toast.error('Failed to fetch users');
    }
  };

  const handleCreateTask = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      const response = await authFetch(`${import.meta.env.VITE_API_URL}/tasks`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...newTask,
          due_date: newTask.due_date ? format(newTask.due_date, "yyyy-MM-dd'T'HH:mm:ss") : null,
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to create task');
      }

      toast.success('Task created successfully');
      setShowNewTaskForm(false);
      setNewTask({ title: '', description: '', assigned_to: '', due_date: null });
      fetchTasks();
    } catch (error) {
      toast.error('Failed to create task');
    }
  };

  const handleUpdateTaskStatus = async (taskId: string, newStatus: string) => {
    try {
      const response = await authFetch(`${import.meta.env.VITE_API_URL}/tasks/${taskId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ 
          status: newStatus,
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to update task');
      }

      toast.success('Task status updated');
      fetchTasks();
    } catch (error) {
      toast.error('Failed to update task status');
    }
  };

  const handleDeleteTask = async (taskId: string) => {
    if (!window.confirm('Are you sure you want to delete this task?')) {
      return;
    }
    // Optimistic UI: remove from state immediately and mark as deleting
  setDeletingIds((prev: string[]) => [...prev, taskId]);
  const previousTasks = tasks;
  setTasks((prev: Task[]) => prev.filter((t: Task) => t.id !== taskId));

    try {
      const response = await authFetch(`${import.meta.env.VITE_API_URL}/tasks/${taskId}`, {
        method: 'DELETE',
      });

      if (!response.ok) {
        throw new Error('Failed to delete task');
      }

      toast.success('Task deleted successfully');
      // Remove from deletingIds
  setDeletingIds((prev: string[]) => prev.filter((id: string) => id !== taskId));
    } catch (error) {
      // Rollback UI and show error
      setTasks(previousTasks);
  setDeletingIds((prev: string[]) => prev.filter((id: string) => id !== taskId));
      toast.error('Failed to delete task');
    }
  };

  const handleCompleteTask = async (taskId: string) => {
    if (!window.confirm('Mark this task as complete? This will remove it.')) {
      return;
    }

    // Optimistic UI: mark as deleting and remove from list
    setDeletingIds((prev: string[]) => [...prev, taskId]);
    const previousTasks = tasks;
    setTasks((prev: Task[]) => prev.filter((t: Task) => t.id !== taskId));

    try {
      const response = await authFetch(`${import.meta.env.VITE_API_URL}/tasks/${taskId}/complete`, {
        method: 'POST',
      });

      if (!response.ok) {
        throw new Error('Failed to complete task');
      }

      toast.success('Task completed');
      setDeletingIds((prev: string[]) => prev.filter((id: string) => id !== taskId));
    } catch (error) {
      // Rollback UI
      setTasks(previousTasks);
      setDeletingIds((prev: string[]) => prev.filter((id: string) => id !== taskId));
      toast.error('Failed to complete task');
    }
  };

  const handleUpdateCustomerStage = async (_taskId: string, customerId: string, newStage: string) => {
    try {
      const response = await authFetch(`${import.meta.env.VITE_API_URL}/contacts/${customerId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ stage: newStage }),
      });

      if (!response.ok) {
        throw new Error('Failed to update customer stage');
      }

      toast.success('Customer stage updated');
      fetchTasks();
    } catch (error) {
      toast.error('Failed to update customer stage');
    }
  };

  const handleUpdateCustomerStatus = async (_taskId: string, customerId: string, newStatus: string) => {
    try {
      const response = await authFetch(`${import.meta.env.VITE_API_URL}/contacts/${customerId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ status: newStatus }),
      });

      if (!response.ok) {
        throw new Error('Failed to update customer status');
      }

      toast.success('Customer status updated');
      fetchTasks();
    } catch (error) {
      toast.error('Failed to update customer status');
    }
  };

  const handleUpdateCustomerNotes = async (_taskId: string, customerId: string, newNotes: string) => {
    try {
      const response = await authFetch(`${import.meta.env.VITE_API_URL}/contacts/${customerId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ notes: newNotes }),
      });

      if (!response.ok) {
        throw new Error('Failed to update customer notes');
      }

      fetchTasks();
    } catch (error) {
      console.error('Failed to update customer notes:', error);
    }
  };

  const handleUpdateCustomerSenderEmail = async (_taskId: string, customerId: string, newEmail: string) => {
    try {
      const response = await authFetch(`${import.meta.env.VITE_API_URL}/contacts/${customerId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ sender_email: newEmail }),
      });

      if (!response.ok) {
        throw new Error('Failed to update customer sender email');
      }

      toast.success('Customer sender email updated');
      fetchTasks();
    } catch (error) {
      toast.error('Failed to update customer sender email');
    }
  };

  const handleUpdateCustomerDate = async (_taskId: string, customerId: string | number, newDate: string) => {
    try {
      console.log('Updating customer date:', { customerId, newDate });
      
  const response = await authFetch(`${import.meta.env.VITE_API_URL}/contacts/${customerId}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ date: newDate }),
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({}));
        console.error('Failed to update customer date:', errorData);
        throw new Error(errorData.detail || 'Failed to update customer date');
      }

      toast.success('Customer date updated');
      await fetchTasks(); // Ensure tasks are refreshed
      
      // Force UI refresh
      setTasks((prevTasks: Task[]) => {
        return prevTasks.map((task: Task) => {
          if (task.id === _taskId) {
            return { ...task, customer_date: newDate };
          }
          return task;
        });
      });
    } catch (error) {
      console.error('Error updating customer date:', error);
      toast.error(error instanceof Error ? error.message : 'Failed to update customer date');
    }
  };

  const getStatusIcon = (status: string) => {
    switch ((status || '').toLowerCase()) {
      case 'completed':
        return <CheckCircle className="text-green-500" />;
      case 'pending':
        return <Clock className="text-yellow-500" />;
      case 'in progress':
        return <AlertCircle className="text-blue-500" />;
      default:
        return <AlertCircle className="text-gray-500" />;
    }
  };

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500"></div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="min-h-screen flex items-center justify-center flex-col">
        <div className="text-red-500 font-bold mb-4">Error loading tasks</div>
        <div className="text-gray-700">{error}</div>
        <button 
          onClick={() => fetchTasks()} 
          className="mt-4 px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
        >
          Retry
        </button>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-gray-100 py-8">
      <div className="max-w-7xl mx-auto px-4">
        <div className="flex justify-between items-center mb-6">
          <h1 className="text-2xl font-bold">My Tasks</h1>
          {user?.is_admin && (
            <button
              onClick={() => setShowNewTaskForm(true)}
              className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
            >
              Create New Task
            </button>
          )}
        </div>

        {showNewTaskForm && (
          <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
            <div className="bg-white rounded-lg p-8 max-w-md w-full">
              <h2 className="text-xl font-bold mb-4">Create New Task</h2>
              <form onSubmit={handleCreateTask} className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700">Title</label>
                  <input
                    type="text"
                    value={newTask.title}
                    onChange={(e: React.ChangeEvent<HTMLInputElement>) => setNewTask({ ...newTask, title: e.target.value })}
                    className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    required
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700">Description</label>
                  <textarea
                    value={newTask.description}
                    onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => setNewTask({ ...newTask, description: e.target.value })}
                    className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    rows={3}
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700">Assign To</label>
                  <select
                    value={newTask.assigned_to}
                    onChange={(e: React.ChangeEvent<HTMLSelectElement>) => setNewTask({ ...newTask, assigned_to: e.target.value })}
                    className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    required
                  >
                    <option value="">Select User</option>
                    {users.map((user: { id: string; username: string }) => (
                      <option key={user.id} value={user.id}>
                        {user.username}
                      </option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700">Due Date</label>
                  <DatePicker
                    selected={newTask.due_date}
                    onChange={(date: Date | null) => setNewTask({ ...newTask, due_date: date })}
                    className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                    dateFormat="dd/MM/yyyy"
                    placeholderText="Select due date"
                  />
                </div>
                <div className="flex justify-end space-x-3">
                  <button
                    type="button"
                    onClick={() => setShowNewTaskForm(false)}
                    className="px-4 py-2 border border-gray-300 rounded-md text-gray-700 hover:bg-gray-50"
                  >
                    Cancel
                  </button>
                  <button
                    type="submit"
                    className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700"
                  >
                    Create Task
                  </button>
                </div>
              </form>
            </div>
          </div>
        )}

        <div className="bg-white rounded-lg shadow overflow-hidden">
          <div className="divide-y divide-gray-200">
              {tasks.length === 0 ? (
              <div className="p-6 text-center text-gray-500">
                No tasks found
              </div>
            ) : (
              tasks.map((task: Task) => (
                <div key={task.id} className="p-6 hover:bg-gray-50">
                  <div className="flex justify-between items-start">
                    <div className="flex items-start space-x-3">
                      {getStatusIcon(task.status)}
                      <div>
                        <h3 className="text-lg font-medium">{task.title}</h3>
                        <p className="text-gray-600 mt-1">{task.description}</p>
                        <div className="mt-2 text-sm text-gray-500">
                          <p>Assigned to: {task.assigned_username}</p>
                          <p>Created by: {task.creator_username}</p>
                          {task.due_date && (
                            <p>Due: {format(new Date(task.due_date), 'PPP')}</p>
                          )}
                          {task.type === 'client' && (
                            <>
                              <div className="flex items-center space-x-2">
                                <p>Date:</p>
                                <DatePicker
                                  selected={safelyParseDate(task.customer_date)}
                                    onChange={(date: Date | null) => handleUpdateCustomerDate(task.id, String(task.customer_id || ''), date ? format(date, 'dd/MM/yyyy') : '')}
                                  dateFormat="dd/MM/yyyy"
                                  className="rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                                  placeholderText="Select date"
                                  isClearable
                                />
                                {task.customer_date && (
                                  <span className="text-sm text-gray-600 ml-2">
                                    {task.customer_date}
                                  </span>
                                )}
                              </div>
                              <div className="flex items-center space-x-2 mt-2">
                                <p>Stage:</p>
                                <select
                                  value={task.customer_stage}
                                  onChange={(e: React.ChangeEvent<HTMLSelectElement>) => handleUpdateCustomerStage(task.id, String(task.customer_id || ''), e.target.value)}
                                  className="rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                                >
                                  {STAGE_OPTIONS.map((option) => (
                                    <option key={option} value={option}>
                                      {option === 'custom' ? 'Custom Stage' : option}
                                    </option>
                                  ))}
                                </select>
                              </div>
                              <div className="flex items-center space-x-2 mt-2">
                                <p>Status:</p>
                                <select
                                  value={task.customer_status}
                                  onChange={(e: React.ChangeEvent<HTMLSelectElement>) => handleUpdateCustomerStatus(task.id, String(task.customer_id || ''), e.target.value)}
                                  className="rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                                >
                                  {DEFAULT_STATUS_OPTIONS.map((option) => (
                                    <option key={option} value={option}>
                                      {option === 'custom' ? 'Custom Status' : option}
                                    </option>
                                  ))}
                                </select>
                              </div>
                              <div className="mt-2">
                                <p>Sender Email:</p>
                                <input
                                  type="email"
                                  value={(task as any).customer_sender_email || ''}
                                  onChange={(e: React.ChangeEvent<HTMLInputElement>) => handleUpdateCustomerSenderEmail(task.id, String(task.customer_id || ''), e.target.value)}
                                  className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                                />
                              </div>
                              <div className="mt-2">
                                <p>Notes:</p>
                                <textarea
                                  value={task.customer_notes || ''}
                                  onChange={(e: React.ChangeEvent<HTMLTextAreaElement>) => handleUpdateCustomerNotes(task.id, String(task.customer_id || ''), e.target.value)}
                                  className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                                  rows={3}
                                />
                              </div>
                            </>
                          )}
                        </div>
                      </div>
                    </div>
                    <div className="flex items-center space-x-2">
                      {(user?.is_admin || task.assigned_to === user?.id) && (
                        <select
                          value={task.status}
                          onChange={(e: React.ChangeEvent<HTMLSelectElement>) => handleUpdateTaskStatus(task.id, e.target.value)}
                          className="rounded-md border-gray-300 shadow-sm focus:border-blue-500 focus:ring-blue-500"
                        >
                          <option value="pending">Pending</option>
                          <option value="in progress">In Progress</option>
                          <option value="completed">Completed</option>
                        </select>
                      )}
                      {(user?.is_admin || task.assigned_to === user?.id) && (
                        <>
                          <button
                            onClick={() => handleCompleteTask(task.id)}
                            className="text-green-600 hover:text-green-800 flex items-center mr-3"
                            disabled={deletingIds.includes(task.id)}
                          >
                            Complete
                          </button>
                          <button
                            onClick={() => handleDeleteTask(task.id)}
                            className="text-red-600 hover:text-red-800 flex items-center"
                            disabled={deletingIds.includes(task.id)}
                          >
                            {deletingIds.includes(task.id) ? (
                              <span className="animate-spin mr-2">⏳</span>
                            ) : null}
                            Delete
                          </button>
                        </>
                      )}
                    </div>
                  </div>
                </div>
              ))
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
