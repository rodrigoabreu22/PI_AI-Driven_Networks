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

const StreamingTable = ({ title, columns, data, onRowClick }) => {
  const [rows, setRows] = useState([]);
  const [index, setIndex] = useState(0);

  useEffect(() => {
    const interval = setInterval(() => {
      if (index < data.length) {
        setRows((prev) => [...prev, data[index]]);
        setIndex((i) => i + 1);
      } else {
        clearInterval(interval);
      }
    }, 500);
    return () => clearInterval(interval);
  }, [data, index]);

  return (
    <Paper sx={{ width: '100%', overflow: 'hidden', my: 2 }}>
      <Typography variant="h6" sx={{ p: 2, fontWeight: 'bold' }}>
        {title}
      </Typography>
      <TableContainer sx={{ maxHeight: 400 }}>
        <Table stickyHeader size="small" aria-label={`${title} table`}>
          <TableHead>
            <TableRow>
              {columns.map((column) => (
                <TableCell
                  key={column.id}
                  align={column.align || 'left'}
                  style={{ minWidth: column.minWidth || 100 }}
                >
                  {column.label}
                </TableCell>
              ))}
            </TableRow>
          </TableHead>
          <TableBody>
            {rows.map((row, rowIndex) => (
              <TableRow
                hover
                role="checkbox"
                tabIndex={-1}
                key={rowIndex}
                onClick={() => onRowClick?.(row)}  // Add this
                sx={{ cursor: onRowClick ? 'pointer' : 'default' }}  // Optional: make it visually clickable
              >
                {columns.map((column) => (
                  <TableCell key={column.id} align={column.align || 'left'}>
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

export default StreamingTable;
