import { useState } from 'react';
import StreamingTable from './components/StreamingTable';
import SimpleModelTable from './components/SimpleModelTable';
import PacketInfoDisplay from './components/PacketInfoDisplay';
import AlertSection from './components/AlertSection'; // ✅ NEW IMPORT
import AlertHistoryTable from './components/AlertHistoryTable';
import useCsvLoader from './hooks/useCsvLoader';
import { Button, Box } from '@mui/material';
import Navbar from './components/Navbar'; // ✅ NEW IMPORT

function App() {
  const pcapData = useCsvLoader('/pcap.csv');
  const flowData = useCsvLoader('/flow.csv');
  const modelData = useCsvLoader('/model.csv');

  const [modelIndex, setModelIndex] = useState(0);
  const [selectedPacket, setSelectedPacket] = useState(null);
  const [alertHistory, setAlertHistory] = useState([]);


  const handleNewAlert = (alert) => {
    setAlertHistory((prev) => [...prev, alert]);
  };

  const pcapColumns = [
    { id: 'timestamp', label: 'Timestamp', minWidth: 150 },
    { id: 'string', label: 'Data', minWidth: 300 },
  ];

  const flowColumns = [
    { id: 'FLOW_START_MILLISECONDS', label: 'Start', minWidth: 180 },
    { id: 'FLOW_END_MILLISECONDS', label: 'End', minWidth: 180 },
    { id: 'IPV4_SRC_ADDR', label: 'Src IP', minWidth: 120 },
    { id: 'L4_SRC_PORT', label: 'Src Port', minWidth: 80 },
    { id: 'IPV4_DST_ADDR', label: 'Dst IP', minWidth: 120 },
    { id: 'L4_DST_PORT', label: 'Dst Port', minWidth: 80 },
    { id: 'PROTOCOL', label: 'Proto', minWidth: 60 },
    { id: 'L7_PROTO', label: 'L7 Proto', minWidth: 80 },
    { id: 'IN_BYTES', label: 'In Bytes', minWidth: 80 },
    { id: 'IN_PKTS', label: 'In Pkts', minWidth: 80 },
    { id: 'OUT_BYTES', label: 'Out Bytes', minWidth: 80 },
    { id: 'OUT_PKTS', label: 'Out Pkts', minWidth: 80 },
    { id: 'FLOW_DURATION_MILLISECONDS', label: 'Duration', minWidth: 100 },
    { id: 'DURATION_IN', label: 'In Duration', minWidth: 100 },
    { id: 'DURATION_OUT', label: 'Out Duration', minWidth: 100 },
  ];

  const handleUpdateModel = () => {
    setModelIndex((prev) => (prev + 1) % modelData.length);
  };

  const lastModel = modelData[modelIndex];
  const newModel = modelData[modelIndex + 1] || null;

  const handleRowClick = (row) => {
    try {
      const parsed = JSON.parse(row.packet);
      setSelectedPacket(parsed);
    } catch (e) {
      setSelectedPacket({ error: 'Invalid JSON' });
    }
  };

  return (
    <Box>
      <Navbar /> {/* ✅ Render Navbar at the top */}

      <AlertSection onNewAlert={handleNewAlert} />
      <Box sx={{ padding: 4 }}>
        <Box sx={{ display: 'flex', gap: 4, mb: 4, height: '400px' }}>
          <Box sx={{ width: '1400px', height: '100%', display: 'flex', flexDirection: 'column' }}>
            <StreamingTable
              title="PCAP's Received"
              columns={pcapColumns}
              data={pcapData}
              onRowClick={handleRowClick}
            />
          </Box>
          <Box sx={{ width: '680px', height: '100%' }}>
            <PacketInfoDisplay packet={selectedPacket} />
          </Box>
          <Box
            sx={{
              width: '320px',
              height: '100%',
              overflow: 'auto',
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'space-between',
            }}
          >
            <SimpleModelTable
              title={`New Model${newModel?.Name ? ` (${newModel.Name})` : ''}`}
              row={newModel}
            />

          </Box>
        </Box>

        <Box sx={{ display: 'flex', gap: 4, height: '400px' }}>
          <Box sx={{ width: '2080px', height: '100%' }}>
            <StreamingTable
              title="Features Extracted"
              columns={flowColumns}
              data={flowData}
            />
          </Box>
          <Box
            sx={{
              width: '320px',
              height: '100%',
              overflow: 'auto',
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'space-between',
            }}
          >
            <SimpleModelTable
              title={`Last Model${lastModel?.Name ? ` (${lastModel.Name})` : ''}`}
              row={lastModel}
            />

            <Button
              variant="contained"
              onClick={handleUpdateModel}
              sx={{ mt: 2 }}
              disabled={!newModel}
            >
              Update
            </Button>
          </Box>
        </Box>
      </Box>
      <div>‎ </div>
      <div>‎ </div>
      <div>‎ </div>

      <AlertHistoryTable alertData={alertHistory} />
    </Box>
  );
}

export default App;
