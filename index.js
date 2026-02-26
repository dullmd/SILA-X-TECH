const express = require('express');
const path = require('path');
const app = express();

const PORT = process.env.PORT || 8000;
const code = require('./manaofc');

require('events').EventEmitter.defaultMaxListeners = 500;

// ===== Middleware (MUST be before routes) =====
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// ===== Routes =====
app.use('/code', code);

app.get('/bot', (req, res) => {
    res.sendFile(path.join(__dirname, 'manaofc.js'));
});

app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'main.html'));
});

// ===== Server =====
app.listen(PORT, () => {
    console.log(`Server running on http://localhost:${PORT}`);
});

module.exports = app;
