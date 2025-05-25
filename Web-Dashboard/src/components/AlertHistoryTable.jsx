// AlertHistoryTable.jsx
import React, { useEffect, useState } from 'react';
import {
  Paper,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  Typography,
} from '@mui/material';

const AlertHistoryTable = ({ alertData }) => {
  const [rows, setRows] = useState([]);
  const [index, setIndex] = useState(0);

  useEffect(() => {
    const interval = setInterval(() => {
      if (index < alertData.length) {
        setRows((prev) => [...prev, alertData[index]]);
        setIndex((i) => i + 1);
      } else {
        clearInterval(interval);
      }
    }, 500);
    return () => clearInterval(interval);
  }, [alertData, index]);

  const columns = [
    { id: 'timestamp', label: 'Time', minWidth: 150 },
    { id: 'heading', label: 'Heading', minWidth: 250 },
    { id: 'message', label: 'Message', minWidth: 400 },
  ];

  return (
    <Paper sx={{ width: '100%', overflow: 'hidden', my: 2 }}>
      <Typography variant="h6" sx={{ p: 2, fontWeight: 'bold' }}>
        Alert History
      </Typography>
      <TableContainer sx={{ maxHeight: 400 }}>
        <Table stickyHeader size="small" aria-label="alert history table">
          <TableHead>
            <TableRow>
              {columns.map((column) => (
                <TableCell
                  key={column.id}
                  align="left"
                  style={{ minWidth: column.minWidth }}
                >
                  {column.label}
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {rows.map((row, rowIndex) => (
              <TableRow hover role="checkbox" tabIndex={-1} key={rowIndex}>
                {columns.map((column) => (
                  <TableCell key={column.id} align="left">
                    {row[column.id]}
                  </TableCell>
                ))}
              </TableRow>
            ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Paper>
  );
};

export default AlertHistoryTable;
