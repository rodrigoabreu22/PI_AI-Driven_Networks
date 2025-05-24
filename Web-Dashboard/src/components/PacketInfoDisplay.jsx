import React from 'react';
import { Paper, Typography, Box } from '@mui/material';

const PacketInfoDisplay = ({ packet }) => {
  return (
    <Paper sx={{ p: 2, height: '100%', overflow: 'auto' }}>
      <Typography variant="h6" sx={{ mb: 2, fontWeight: 'bold' }}>
        Packet Info
      </Typography>
      <Box
        component="pre"
        sx={{
          whiteSpace: 'pre-wrap',
          wordBreak: 'break-all',
          fontFamily: 'monospace',
          fontSize: '0.875rem',
        }}
      >
        {packet
          ? JSON.stringify(packet, null, 2)
          : 'Click on a row from PCAP\'s Received to view packet data.'}
      </Box>
    </Paper>
  );
};

export default PacketInfoDisplay;
