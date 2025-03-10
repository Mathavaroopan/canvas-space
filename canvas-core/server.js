// canvas-core/server.js
const express = require('express');
const cors = require('cors');
const path = require('path');
const mongoose = require('mongoose');
require('dotenv').config();

const app = express();

// Connect to MongoDB.
const connectionString = process.env.MONGO_URI;
mongoose.connect(connectionString, {
  useNewUrlParser: true,
  useUnifiedTopology: true
})
  .then(() => console.log('Connected to MongoDB'))
  .catch(err => console.error('Error connecting to MongoDB:', err));

// Middleware.
app.use(cors({
  origin: ["http://localhost:5173", "https://canvas-demo-client.vercel.app/"],
  credentials: true,
}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Serve static files from hls_output.
app.use(express.static(path.join(__dirname, "hls_output")));

// Import controllers.
const aesController = require('./controllers/aesController');
const s3Controller = require('./controllers/s3Controller');
const dbController = require('./controllers/dbController');

// Routes.
app.post('/create-AES', aesController.createAES);
app.post('/modify-AES', aesController.modifyAES);
app.post('/delete-AES', aesController.deleteAES);

app.post('/get-video-names', s3Controller.getVideoNames);
app.post('/download-video', s3Controller.downloadVideo);

app.get('/get-lock-by-contentid/:contentId', dbController.getLockByContentId);
app.get('/get-lockjsonobject/:lockId', dbController.getLockJsonObject);

// Start the server.
const PORT = 3000;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
