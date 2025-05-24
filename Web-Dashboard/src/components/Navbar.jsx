// components/Navbar.jsx
import { AppBar, Toolbar, Typography, Box } from '@mui/material';

export default function Navbar() {
  return (
    <AppBar position="static" sx={{ mb: 4, height: 100, justifyContent: 'center' }}>
      <Toolbar sx={{ height: '100%', display: 'flex', justifyContent: 'space-between' }}>
        {/* Left-aligned image */}
        <Box sx={{ display: 'flex', alignItems: 'center' }}>
            <Typography variant="h4" component="div" sx={{ fontWeight: 'bold' }}>
            ☰
            </Typography>
        </Box>

        {/* Centered title */}
        <Box sx={{ flex: 1, textAlign: 'center' }}>
          <Typography variant="h4" component="div" sx={{ fontWeight: 'bold' }}>
            Network Data Analytics Pipeline
          </Typography>
        </Box>

        {/* Spacer box to balance layout */}
        <Box sx={{ width: '66px' }} /> {/* same width as image + margin */}
      </Toolbar>
    </AppBar>
  );
}
