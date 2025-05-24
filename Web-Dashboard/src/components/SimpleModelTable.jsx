import React from 'react';
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

const metrics = ['Name', 'Accuracy', 'Precision', 'Recall', 'F1_Score', 'Mcc'];

const SimpleModelTable = ({ title, row }) => {
  return (
    <Paper sx={{ width: '100%', maxWidth: 400, overflow: 'hidden', my: 2 }}>
      <Typography variant="h6" sx={{ p: 2, fontWeight: 'bold' }}>
        {title}
      </Typography>
      <TableContainer>
        <Table size="small">
          <TableHead>
            <TableRow>
              <TableCell sx={{ width: 150, fontWeight: 'bold' }}>Metric</TableCell>
              <TableCell sx={{ width: 150, fontWeight: 'bold' }}>Value</TableCell>
            </TableRow>
          </TableHead>
          <TableBody>
            {row &&
              metrics.map((metric) => (
                <TableRow key={metric}>
                  <TableCell sx={{ width: 150 }}>{metric}</TableCell>
                  <TableCell sx={{ width: 150 }}>{row[metric]}</TableCell>
                </TableRow>
              ))}
          </TableBody>
        </Table>
      </TableContainer>
    </Paper>
  );
};

export default SimpleModelTable;
