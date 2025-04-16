const { S3Client, GetObjectCommand } = require('@aws-sdk/client-s3');
const { Readable } = require('stream');
const path = require('path');
const fs = require('fs');
const Lock = require('../models/Lock');

// Helper function to format time in HH:MM:SS.mmm format
const formatTime = (seconds) => {
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);
  const msecs = Math.floor((seconds % 1) * 1000);
  return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}.${msecs.toString().padStart(3, '0')}`;
};

// Helper function to download a file from S3 with timeout
const downloadFromS3 = async (s3Client, bucket, key, outputPath) => {
  const command = new GetObjectCommand({
    Bucket: bucket,
    Key: key
  });

  // Create a timeout promise
  const timeoutPromise = new Promise((_, reject) => {
    setTimeout(() => {
      reject(new Error(`Download timeout for ${key}`));
    }, 30000); // 30 second timeout
  });

  try {
    // Race between the download and the timeout
    const response = await Promise.race([
      s3Client.send(command),
      timeoutPromise
    ]);

    const writeStream = fs.createWriteStream(outputPath);
    
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        writeStream.end();
        reject(new Error(`Write timeout for ${key}`));
      }, 30000); // 30 second timeout for writing

      response.Body.pipe(writeStream)
        .on('finish', () => {
          clearTimeout(timeout);
          writeStream.end();
          resolve();
        })
        .on('error', (error) => {
          clearTimeout(timeout);
          writeStream.end();
          reject(error);
        });
    });
  } catch (error) {
    // If the file was partially written, clean it up
    if (fs.existsSync(outputPath)) {
      fs.unlinkSync(outputPath);
    }
    throw error;
  }
};

// Helper function to process m3u8 file and download segments
const processM3u8File = async (s3Client, bucket, m3u8Content, baseUrl, outputDir) => {
  console.log("Starting processM3u8File");
  const lines = m3u8Content.split('\n');
  const processedLines = [];
  const segmentDownloads = [];
  
  console.log(`Processing ${lines.length} lines in m3u8 file`);
  console.log(`Base URL: ${baseUrl}`);
  console.log(`Bucket: ${bucket}`);

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i].trim();
    
    if (line.endsWith('.ts')) {
      // This is a segment line
      let segmentUrl = line;
      console.log(`Processing segment: ${segmentUrl}`);
      
      // If the URL doesn't start with http, prepend the base URL
      if (!segmentUrl.startsWith('http')) {
        segmentUrl = `${baseUrl}/${segmentUrl}`;
        console.log(`Prepending base URL: ${segmentUrl}`);
      }
      
      // Extract the S3 key by removing the bucket and region parts
      // Format: https://bucket-name.s3.region.amazonaws.com/path/to/segment.ts
      const urlParts = segmentUrl.split('/');
      const bucketIndex = urlParts.findIndex(part => part.includes(bucket));
      
      if (bucketIndex !== -1) {
        // Remove everything before and including the bucket name
        const segmentKey = urlParts.slice(bucketIndex + 1).join('/');
        const segmentFilename = path.basename(segmentUrl);
        const segmentPath = path.join(outputDir, segmentFilename);
        
        console.log(`Downloading segment: ${segmentKey} to ${segmentPath}`);
        
        // Add to download queue
        segmentDownloads.push(
          downloadFromS3(s3Client, bucket, segmentKey, segmentPath)
            .then(() => {
              console.log(`Successfully downloaded segment: ${segmentFilename}`);
              return segmentFilename;
            })
            .catch(error => {
              console.error(`Error downloading segment ${segmentFilename}:`, error);
              // Create an empty file to prevent errors
              if (!fs.existsSync(segmentPath)) {
                fs.writeFileSync(segmentPath, '');
              }
              return segmentFilename; // Continue with other segments even if one fails
            })
        );
        
        // Update the line to use just the filename
        processedLines.push(segmentFilename);
      } else {
        // If we can't find the bucket in the URL, use the original line
        console.log(`Could not find bucket in URL: ${segmentUrl}`);
        processedLines.push(line);
      }
    } else {
      // Keep other lines as is
      processedLines.push(line);
    }
  }

  console.log(`Waiting for ${segmentDownloads.length} segment downloads to complete`);
  
  // Wait for all segment downloads to complete with a timeout
  try {
    await Promise.race([
      Promise.all(segmentDownloads),
      new Promise((_, reject) => setTimeout(() => reject(new Error('Overall segment download timeout')), 60000))
    ]);
    console.log("All segment downloads completed");
  } catch (error) {
    console.error("Error during segment downloads:", error);
    // Continue anyway, we'll use whatever segments we have
  }
  
  // Return the processed m3u8 content
  return processedLines.join('\n');
};

// Download video v2 endpoint
const downloadVideoV2 = async (req, res) => {
  try {
    console.log("Starting downloadVideoV2");
    const { storageType, storageMetaData, originalVideoUrl, lockId } = req.body;

    // Validate required parameters
    if (!storageType || !storageMetaData || !originalVideoUrl || !lockId) {
      return res.status(400).json({
        success: false,
        message: 'Missing required parameters: storageType, storageMetaData, originalVideoUrl, or lockId'
      });
    }

    // Validate AWS credentials
    if (!storageMetaData.awsAccessKeyId || !storageMetaData.awsSecretAccessKey || !storageMetaData.awsRegion || !storageMetaData.awsBucketName) {
      return res.status(400).json({
        success: false,
        message: 'Invalid AWS credentials in storageMetaData'
      });
    }

    // Get lock information from database
    console.log(`Finding lock with ID: ${lockId}`);
    const lock = await Lock.findOne({ lock_id: lockId });
    if (!lock) {
      return res.status(404).json({
        success: false,
        message: 'Lock not found'
      });
    }
    console.log("Lock found:", lock);

    // Initialize S3 client
    console.log("Initializing S3 client");
    const s3Client = new S3Client({
      region: storageMetaData.awsRegion,
      credentials: {
        accessKeyId: storageMetaData.awsAccessKeyId,
        secretAccessKey: storageMetaData.awsSecretAccessKey
      }
    });

    // Create output directory if it doesn't exist
    const outputDir = path.join(__dirname, '../../hls_output');
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }
    console.log(`Output directory: ${outputDir}`);
    
    // Download and process locked m3u8
    const lockedFilename = path.basename(lock.LockedContentUrl);
    const lockedVideoKey = lock.LockedContentUrl.split('/').slice(3).join('/');
    const lockedVideoPath = path.join(outputDir, lockedFilename);
    
    console.log(`Downloading locked m3u8: ${lockedVideoKey}`);
    // Download locked m3u8
    await downloadFromS3(s3Client, storageMetaData.awsBucketName, lockedVideoKey, lockedVideoPath);
    console.log("Locked m3u8 downloaded");
    
    // Read locked m3u8 content
    const lockedM3u8Content = fs.readFileSync(lockedVideoPath, 'utf8');
    console.log("Locked m3u8 content read");
    
    // Process locked m3u8 and download segments
    console.log("Starting to process locked m3u8");
    const baseUrl = lock.LockedContentUrl.substring(0, lock.LockedContentUrl.lastIndexOf('/'));
    console.log(`Base URL for segments: ${baseUrl}`);
    
    const processedLockedM3u8Content = await processM3u8File(
      s3Client, 
      storageMetaData.awsBucketName, 
      lockedM3u8Content, 
      baseUrl, 
      outputDir
    );
    console.log("Finished processing locked m3u8");
    
    // Write processed locked m3u8 content
    fs.writeFileSync(lockedVideoPath, processedLockedM3u8Content);
    console.log("Written processed locked m3u8 content");

    // Return success response with file paths
    console.log("Sending success response");
    return res.status(200).json({
      success: true,
      message: 'Videos downloaded successfully',
      data: {
        originalVideoPath: "output.m3u8",
        blackoutVideoPath: lockedFilename
      }
    });

  } catch (error) {
    console.error('Error in downloadVideoV2:', error);
    return res.status(500).json({
      success: false,
      message: 'Error downloading videos',
      error: error.message
    });
  }
};

module.exports = {
  downloadVideoV2
}; 