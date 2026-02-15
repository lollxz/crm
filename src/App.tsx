import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
import { AuthProvider } from './contexts/AuthContext';
import { PreviewResultsProvider } from './contexts/PreviewResultsContext';
import { ProtectedRoute } from './components/ProtectedRoute';
import { AdminDashboard } from './components/AdminDashboard';
import { LoginPage } from './components/LoginPage';
import { MainApp } from './components/MainApp';
import { TasksPage } from './components/TasksPage';
import QueuePage from './components/QueuePage';
import { ToastContainer } from 'react-toastify';
import 'react-toastify/dist/ReactToastify.css';
import MyEventsDashboard from './components/MyEventsDashboard';
import { EventDetailsPage } from './components/EventDetailsPage';
import GoToEventsButton from './components/GoToEventsButton';
import { EnhancedDashboard } from './components/EnhancedDashboard';
import OrganizationsPage from './components/OrganizationsPage';

function App() {
  return (
    <Router>
      <AuthProvider>
        <PreviewResultsProvider>
          <ToastContainer position="top-right" />
          <Routes>
          <Route path="/login" element={<LoginPage />} />
          <Route
            path="/admin"
            element={
              <ProtectedRoute requireAdmin>
                <AdminDashboard />
              </ProtectedRoute>
            }
          />
          <Route
            path="/tasks"
            element={
              <ProtectedRoute>
                <TasksPage />
              </ProtectedRoute>
            }
          />
          <Route
            path="/events"
            element={
              <ProtectedRoute>
                <MyEventsDashboard />
              </ProtectedRoute>
            }
          />
          <Route
            path="/organizations"
            element={
              <ProtectedRoute>
                <OrganizationsPage />
              </ProtectedRoute>
            }
          />
          <Route
            path="/events/:event_id"
            element={
              <ProtectedRoute>
                <EventDetailsPage />
              </ProtectedRoute>
            }
          />
          <Route path="/monitoring" element={<EnhancedDashboard />} />
          <Route
            path="/"
            element={
              <ProtectedRoute>
                <MainApp />
              </ProtectedRoute>
            }
          />
          <Route
            path="/queue"
            element={
              <ProtectedRoute>
                <QueuePage />
              </ProtectedRoute>
            }
          />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
        </PreviewResultsProvider>
      </AuthProvider>
    </Router>
  );
}

export default App;
