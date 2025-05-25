import { useEffect, useState } from 'react';
import Alert from 'react-bootstrap/Alert';
import { Container } from 'react-bootstrap';

function AlertSection({ onNewAlert }) {
  const [alerts, setAlerts] = useState([]);

  // Your timers array with delays and alert info
  const timers = [
    {
      delay: 7000,
      variant: 'danger',
      heading: 'Alert Triggered - Insufficient Performance',
      message: 'The current model is underperforming. Please update to the latest model',
    },
    {
      delay: 20000,
      variant: 'danger',
      heading: 'Alert Triggered - Insufficient Performance',
      message: 'The current model is underperforming. Please update to the latest model',
    },
    {
      delay: 27000,
      variant: 'danger',
      heading: 'Alert Triggered - Insufficient Performance',
      message: 'The current model is underperforming. Please update to the latest model',
    },
    {
      delay: 40000,
      variant: 'danger',
      heading: 'Alert Triggered - Insufficient Performance',
      message: 'The current model is underperforming. Please update to the latest model',
    },
    {
      delay: 47000,
      variant: 'danger',
      heading: 'Alert Triggered - Insufficient Performance',
      message: 'The current model is underperforming. Please update to the latest model',
    },
    {
      delay: 57000,
      variant: 'danger',
      heading: 'Alert Triggered - Insufficient Performance',
      message: 'The current model is underperforming. Please update to the latest model',
    },
  ];

  useEffect(() => {
    const timeoutIds = timers.map(({ delay, variant, heading, message }) =>
      setTimeout(() => {
        const newAlert = {
          id: Date.now() + Math.random(), // unique id
          variant,
          heading,
          message,
          timestamp: new Date().toLocaleTimeString(),
        };
        setAlerts((prev) => [...prev, newAlert]);
        if (onNewAlert) onNewAlert(newAlert); // notify parent
      }, delay)
    );

    return () => timeoutIds.forEach(clearTimeout);
  }, []);

  const dismissAlert = (id) => {
    setAlerts((prev) => prev.filter((alert) => alert.id !== id));
  };

  return (
    <Container
      fluid
      className="my-2"
      style={{
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        gap: '1rem',
      }}
    >
      {alerts.map((alert) => (
        <Alert
          key={alert.id}
          variant={alert.variant}
          onClose={() => dismissAlert(alert.id)}
          dismissible
          style={{
            width: '30%',
            textAlign: 'center',
          }}
        >
          <Alert.Heading>{alert.heading}</Alert.Heading>
          <p>{alert.message}</p>
        </Alert>
      ))}
    </Container>
  );
}

export default AlertSection;
