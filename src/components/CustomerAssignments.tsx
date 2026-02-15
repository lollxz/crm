import React, { useEffect, useState } from 'react';
import {
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Paper,
  Typography,
  Box,
  CircularProgress,
  Alert,
} from '@mui/material';
import axios from 'axios';

interface Assignment {
  customer_id: number;
  customer_name: string;
  customer_email: string;
  event_name: string;
  date: string;
  status: string;
  assigned_to: string;
  assigned_username: string;
  assigned_user_is_admin: boolean;
}

const CustomerAssignments: React.FC = () => {
  const [assignments, setAssignments] = useState<Assignment[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const fetchAssignments = async () => {
      try {
        const response = await axios.get('/api/customers/assignments', {
          headers: {
            Authorization: `Bearer ${localStorage.getItem('token')}`,
          },
        });
        setAssignments(response.data.assignments);
        setError(null);
      } catch (err) {
        setError('Failed to fetch customer assignments');
        console.error('Error fetching assignments:', err);
      } finally {
        setLoading(false);
      }
    };

    fetchAssignments();
  }, []);

  if (loading) {
    return (
      <Box display="flex" justifyContent="center" alignItems="center" minHeight="200px">
        <CircularProgress />
      </Box>
    );
  }

  if (error) {
    return (
      <Box mt={2}>
        <Alert severity="error">{error}</Alert>
      </Box>
    );
  }

  return (
    <Box>
      <Typography variant="h5" gutterBottom>
        Customer Assignments
      </Typography>
      <TableContainer component={Paper}>
        <Table>
          <TableHead>
            <TableRow>
              <TableCell>Customer Name</TableCell>
              <TableCell>Email</TableCell>
              <TableCell>Event</TableCell>
              <TableCell>Date</TableCell>
              <TableCell>Status</TableCell>
              <TableCell>Assigned To</TableCell>
              <TableCell>User Type</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {assignments.map((assignment) => (
              <TableRow key={assignment.customer_id}>
                <TableCell>{assignment.customer_name}</TableCell>
                <TableCell>{assignment.customer_email}</TableCell>
                <TableCell>{assignment.event_name}</TableCell>
                <TableCell>{assignment.date}</TableCell>
                <TableCell>{assignment.status}</TableCell>
                <TableCell>{assignment.assigned_username}</TableCell>
                <TableCell>{assignment.assigned_user_is_admin ? 'Admin' : 'User'}</TableCell>
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Box>
  );
};

export default CustomerAssignments; 