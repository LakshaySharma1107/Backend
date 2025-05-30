const express = require('express');
const { Pool } = require('pg');
require('dotenv').config();

const app = express();
app.use(express.json()); // Parse JSON bodies

// Connect to PostgreSQL on Render
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false }
});

// Test DB connection
pool.connect((err, client, release) => {
    if (err) {
        console.error('Error connecting to PostgreSQL:', err.stack);
        return;
    }
    console.log('Connected to PostgreSQL database');
    release();
});

// POST /identify endpoint
app.post('/identify', async (req, res) => {
    const { email, phoneNumber } = req.body;

    if (!email || !phoneNumber) {
        return res.status(400).json({ error: 'Email and phoneNumber are required' });
    }

    let client;
    try {
        client = await pool.connect();

        // Call the PostgreSQL FUNCTION (not PROCEDURE)
        const result = await client.query(
            'SELECT identify_contact($1, $2) AS result_json',
            [email, phoneNumber]
        );

        const response = result.rows[0].result_json;
        res.status(200).json(response);

    } catch (error) {
        console.error('Error executing query:', error.stack);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        if (client) client.release();
    }
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});
