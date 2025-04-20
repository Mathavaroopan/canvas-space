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
  origin: ["http://localhost:5173", "http://localhost:8081", "https://canvas-demo-client.vercel.app/"],
  credentials: true,
}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// app.use((req, res, next) => {
//   res.header('Access-Control-Allow-Origin', '*');
//   res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
//   res.header('Access-Control-Allow-Methods', 'GET, OPTIONS');
//   next();
// });

// Serve static files from hls_output.
app.use('/hls_output', express.static(path.join(__dirname, "../hls_output")));
app.use('/', express.static(path.join(__dirname, "../hls_output")));

// Import the Lock model.
const Lock = require('./models/Lock');

// DELETE endpoint to remove all records in the Lock collection.
app.delete('/delete-all-locks', async (req, res) => {
  try {
    const result = await Lock.deleteMany({});
    res.status(200).json({ 
      message: 'All lock records deleted successfully.',
      result
    });
  } catch (err) {
    console.error('Error deleting lock records:', err);
    res.status(500).json({ error: 'An error occurred while deleting lock records.' });
  }
});

// Import controllers.
const aesController = require('./controllers/aesController');
const s3Controller = require('./controllers/s3Controller');
const dbController = require('./controllers/dbController');
const segmentController = require('./controllers/segmentController');
const { downloadVideoV2 } = require('./controllers/videoController');
const { getSegment, getSegmentV2 } = require('./controllers/segmentController');

// Import form routes
const formRoutes = require('./routes/formRoutes');

// Routes - create/modify/delete AES (locks)
app.post('/create-AES', aesController.createAES);
app.post('/modify-AES', aesController.modifyAES);
app.post('/delete-AES', aesController.deleteAES);

app.post('/get-video-names', s3Controller.getVideoNames);
app.post('/download-video', s3Controller.downloadVideo);
app.post('/api/get-segment', getSegment);
app.post('/api/get-segment-v2', getSegmentV2);

app.get('/get-lockId-by-contentId/:contentId', dbController.getLockIdByContentId);
app.get('/get-lockjsonobject-by-lockId/:lockId', dbController.getLockJsonObject);
app.get('/get-lockjsonobject-by-inputVideoUrl/:inputVideoUrl', dbController.getLockIdByInputVideoUrl);
app.get('/get-all-videos', dbController.getAllVideos);

// Use form routes
app.use('/api', formRoutes);

// Video download v2 endpoint
app.post('/download-video-v2', downloadVideoV2);

// Start the server.
const PORT = process.env.PORT;
app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
