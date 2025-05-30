const express = require('express');
const { Pool } = require('pg');
require('dotenv').config();

const app = express();
app.use(express.json()); // Parse incoming JSON data from user

// Connect to Render PostgreSQL using DATABASE_URL
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: { rejectUnauthorized: false } // Required for Render's PostgreSQL
});

// Test database connection
pool.connect((err, client, release) => {
    if (err) {
        console.error('Error connecting to PostgreSQL:', err.stack);
        return;
    }
    console.log('Connected to PostgreSQL database');
    release();
});

// /identify endpoint: Receive user data and process it
app.post('/identify', async (req, res) => {
    const { email, phoneNumber } = req.body; // Get email and phoneNumber from user request
    let client;

    try {
        client = await pool.connect();
        // Call the stored procedure in PostgreSQL
        await client.query('CALL identify_contact($1, $2, NULL)', [email, phoneNumber]);
        // Fetch the output JSON from the procedure
        const result = await client.query('SELECT $1 AS result_json', ['result_json']);
        const response = result.rows[0].result_json;

        // Send the response back to the user
        res.status(200).json(response);
    } catch (error) {
        console.error('Error executing procedure:', error.stack);
        res.status(500).json({ error: 'Internal server error' });
    } finally {
        if (client) client.release(); // Release connection back to pool
    }
});

// Start the server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});