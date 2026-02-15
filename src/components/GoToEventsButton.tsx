import React from 'react';
import { useNavigate } from 'react-router-dom';

const GoToEventsButton: React.FC = () => {
  const navigate = useNavigate();
  const token = localStorage.getItem('token');
  if (!token) return null;
  return (
    <button
      className="bg-blue-600 text-white px-4 py-2 rounded hover:bg-blue-700 mt-4"
      onClick={() => navigate('/events')}
    >
      Go to My Events
    </button>
  );
};

export default GoToEventsButton; 