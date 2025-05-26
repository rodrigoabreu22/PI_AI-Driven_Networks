import { useEffect, useState } from 'react';
import StreamingTable from './components/StreamingTable';
import SimpleModelTable from './components/SimpleModelTable';
import PacketInfoDisplay from './components/PacketInfoDisplay';
import AlertSection from './components/AlertSection';
import AlertHistoryTable from './components/AlertHistoryTable';
import useCsvLoader from './hooks/useCsvLoader';
import { Button, Box, Typography } from '@mui/material';
import Navbar from './components/Navbar';

function App() {
  const pcapData = useCsvLoader('/pcap.csv');
  const flowData = useCsvLoader('/flow.csv');
  const modelData = useCsvLoader('/model.csv');

  const [appliedModelIndex, setAppliedModelIndex] = useState(0);
  const [selectedPacket, setSelectedPacket] = useState(null);
  const [alertHistory, setAlertHistory] = useState([]);
  const [flowStep, setFlowStep] = useState(0); // 0 to 5

  // Track update button clicks
  const [updateCount, setUpdateCount] = useState(0);

  // === FLOW CONTROL ===
  useEffect(() => {
    let timer;

    if (flowStep === 0) {
      // Step 1: Show new model after 30s
      timer = setTimeout(() => setFlowStep(1), 10000);
    } else if (flowStep === 1) {
      // Step 2: Show alert 30s after model appears
      timer = setTimeout(() => {
        triggerAlert({
          variant: 'danger',
          heading: 'Alert Triggered - Insufficient Performance',
          message: 'The current model is underperforming. Please update to the latest model',
        });
        setFlowStep(2);
      }, 10000);
    } else if (flowStep === 3) {
      // Step 4: Second alert 30s after update
      timer = setTimeout(() => {
        triggerAlert({
          variant: 'danger',
          heading: 'Alert Triggered - Insufficient Performance',
          message: 'The current model is underperforming. Please update to the latest model',
        });
        setFlowStep(4);
      }, 10000);
    }

    return () => clearTimeout(timer);
  }, [flowStep]);

  // === ALERT HANDLER ===
  const triggerAlert = ({ variant, heading, message }) => {
    const alert = {
      id: Date.now() + Math.random(),
      variant,
      heading,
      message,
      timestamp: new Date().toLocaleTimeString(),
    };
    setAlertHistory((prev) => [...prev, alert]);
  };

  // === UPDATE MODEL ===
  const handleUpdateModel = () => {
    if (appliedModelIndex + 1 < modelData.length) {
      setAppliedModelIndex((prev) => prev + 1);    
      setUpdateCount((prev) => prev + 1);

      if (flowStep === 2) setFlowStep(3); // First update
      else if (flowStep === 4) setFlowStep(5); // Second update (final)
    }
  };

  const lastModel = modelData[appliedModelIndex];
  const newModel = modelData[appliedModelIndex + 1] || null;


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
      <Navbar />

      <AlertSection onNewAlert={() => {}} alerts={alertHistory} />

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
              row={flowStep >= 1 ? newModel : null}
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
              row={flowStep >= 3 ? lastModel : null}
            />

            <Button
              variant="contained"
              onClick={handleUpdateModel}
              sx={{ mt: 2 }}
              disabled={!newModel || flowStep === 0 || flowStep === 1}
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

      <Typography variant="caption" sx={{ position: 'absolute', bottom: 10, right: 10 }}>
        Flow Step: {flowStep}
      </Typography>
    </Box>
  );
}

export default App;
