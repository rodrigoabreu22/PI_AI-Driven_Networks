import Alert from 'react-bootstrap/Alert';
import { Container } from 'react-bootstrap';

function AlertSection({ alerts }) {
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
