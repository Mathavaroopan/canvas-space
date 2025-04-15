const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const { Readable } = require('stream');

// Helper function to format time in HH:MM:SS.mmm format
const formatTime = (ms) => {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  return `${hours.toString().padStart(2, '0')}:${(minutes % 60).toString().padStart(2, '0')}:${(seconds % 60).toString().padStart(2, '0')}.${(ms % 1000).toString().padStart(3, '0')}`;
};

const getSegment = async (req, res) => {
  try {
    let { storageType, storageMetaData, segmentPath } = req.body;

    if (!storageType || !storageMetaData) {
      return res.status(400).json({ error: "Missing storage configuration" });
    }

    if (!segmentPath) {
      return res.status(400).json({ error: "Missing segment path" });
    }
    // segmentPath = "mathav-testing/" + segmentPath;
    console.log("storagetype", storageType);
    console.log("storageMetaData", storageMetaData);
    console.log("segmentPath", segmentPath);

    if (storageType === "AWS") {
      const startTime = Date.now();
      const s3Client = new S3Client({
        region: storageMetaData.awsRegion,
        credentials: {
          accessKeyId: storageMetaData.awsAccessKeyId,
          secretAccessKey: storageMetaData.awsSecretAccessKey,
        },
      });
      console.log(`Connected to AWS S3 in ${formatTime(Date.now() - startTime)}`);

      // Determine if this is a blackout segment or regular segment
      const isBlackoutSegment = segmentPath.includes("blackout");
      const fullS3Key = isBlackoutSegment
        ? `${storageMetaData.folderPrefix}${segmentPath}`
        : `${storageMetaData.folderPrefix}${segmentPath}`;

      console.log("Fetching segment:", fullS3Key);

      try {
        const command = new GetObjectCommand({
          Bucket: storageMetaData.awsBucketName,
          Key: fullS3Key,
        });

        const response = await s3Client.send(command);
        
        if (!response.Body) {
          throw new Error("No data received from S3");
        }

        // Set appropriate headers for streaming
        res.setHeader('Content-Type', 'video/MP2T');
        res.setHeader('Content-Length', response.ContentLength);
        res.setHeader('Accept-Ranges', 'bytes');
        res.setHeader('Cache-Control', 'no-cache');
        
        // Create a readable stream from the S3 response
        const stream = response.Body;
        
        // Handle stream errors
        stream.on('error', (error) => {
          console.error('Stream error:', error);
          if (!res.headersSent) {
            res.status(500).json({ error: "Stream error occurred" });
          }
        });

        // Pipe the stream to the response
        stream.pipe(res);

        // Handle stream end
        stream.on('end', () => {
          console.log(`Finished streaming segment: ${fullS3Key}`);
        });

      } catch (error) {
        console.error("Error fetching from S3:", error);
        if (!res.headersSent) {
          res.status(500).json({ error: "Failed to fetch segment from S3" });
        }
      }
    } else {
      res.status(400).json({ error: "Unsupported storage type" });
    }
  } catch (error) {
    console.error("Error in /get-segment:", error);
    if (!res.headersSent) {
      res.status(500).json({ error: "Internal server error" });
    }
  }
};

module.exports = {
  getSegment,
}; 