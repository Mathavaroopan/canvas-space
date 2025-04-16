const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
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

// Get segment endpoint
const getSegment = async (req, res) => {
  try {
    const { storageType, storageMetaData, segmentPath } = req.body;

    // Validate required parameters
    if (!storageType || !storageMetaData || !segmentPath) {
      return res.status(400).json({
        success: false,
        message: 'Missing required parameters: storageType, storageMetaData, or segmentPath'
      });
    }

    // Validate AWS credentials
    if (!storageMetaData.awsAccessKeyId || !storageMetaData.awsSecretAccessKey || !storageMetaData.awsRegion || !storageMetaData.awsBucketName) {
      return res.status(400).json({
        success: false,
        message: 'Invalid AWS credentials in storageMetaData'
      });
    }

    // Initialize S3 client
    const s3Client = new S3Client({
      region: storageMetaData.awsRegion,
      credentials: {
        accessKeyId: storageMetaData.awsAccessKeyId,
        secretAccessKey: storageMetaData.awsSecretAccessKey
      }
    });

    // Determine if this is a blackout segment or a regular segment
    const isBlackoutSegment = segmentPath.includes('blackout');
    
    // Construct the full S3 key for the segment
    let fullSegmentKey;
    if (isBlackoutSegment) {
      // For blackout segments, use the segment path directly
      fullSegmentKey = segmentPath;
    } else {
      // For regular segments, we need to find the corresponding segment in the original video
      // This assumes the segment path format is consistent between blackout and original videos
      fullSegmentKey = segmentPath;
    }

    console.log(`Fetching segment: ${fullSegmentKey} from bucket: ${storageMetaData.awsBucketName}`);

    // Get the segment from S3
    const command = new GetObjectCommand({
      Bucket: storageMetaData.awsBucketName,
      Key: fullSegmentKey
    });

    const response = await s3Client.send(command);
    
    // Set response headers for streaming
    res.setHeader('Content-Type', 'video/mp2t');
    res.setHeader('Content-Length', response.ContentLength);
    res.setHeader('Cache-Control', 'no-cache');
    
    // Stream the segment to the client
    if (response.Body instanceof Readable) {
      response.Body.pipe(res);
    } else {
      // If the body is not a readable stream, convert it to one
      const stream = Readable.from(response.Body);
      stream.pipe(res);
    }
  } catch (error) {
    console.error('Error in getSegment:', error);
    return res.status(500).json({
      success: false,
      message: 'Error fetching segment',
      error: error.message
    });
  }
};

// Get segment v2 endpoint - returns segment data as binary
const getSegmentV2 = async (req, res) => {
  try {
    console.log(req.body);
    const { storageType, storageMetaData, segmentPath, lockJsonObject } = req.body;

    // Validate required parameters
    if (!storageType || !storageMetaData || !segmentPath || !lockJsonObject) {
      return res.status(400).json({
        success: false,
        message: 'Missing required parameters: storageType, storageMetaData, segmentPath, or lockJsonObject'
      });
    }

    // Validate AWS credentials
    if (!storageMetaData.awsAccessKeyId || !storageMetaData.awsSecretAccessKey || !storageMetaData.awsRegion || !storageMetaData.awsBucketName) {
      return res.status(400).json({
        success: false,
        message: 'Invalid AWS credentials in storageMetaData'
      });
    }

    // Initialize S3 client
    const s3Client = new S3Client({
      region: storageMetaData.awsRegion,
      credentials: {
        accessKeyId: storageMetaData.awsAccessKeyId,
        secretAccessKey: storageMetaData.awsSecretAccessKey
      }
    });

    // Extract the segment number from the blackout segment path
    const segmentMatch = segmentPath.match(/blackout_(\d+)\.ts$/);
    if (!segmentMatch || !segmentMatch[1]) {
      return res.status(400).json({
        success: false,
        message: 'Invalid blackout segment path format'
      });
    }
    
    const blackoutSegmentNumber = segmentMatch[1];
    
    // Get the locked video URL from the lockJsonObject (this is the blackout.m3u8 URL)
    const lockedVideoUrl = lockJsonObject.lockedContentUrl;
    if (!lockedVideoUrl) {
      return res.status(400).json({
        success: false,
        message: 'Locked video URL not found in lockJsonObject'
      });
    }
    
    console.log(`Locked Video URL: ${lockedVideoUrl}`);
    
    // Extract just the path part without domain for S3 key
    let folderPath;
    try {
      // Parse the URL to extract just the path part
      const url = new URL(lockedVideoUrl);
      // Remove the m3u8 filename from the path
      const pathWithoutFile = url.pathname.replace(/\/[^\/]+\.m3u8$/, '/');
      // Remove leading slash if present
      folderPath = pathWithoutFile.startsWith('/') ? pathWithoutFile.substring(1) : pathWithoutFile;
    } catch (error) {
      console.error('Error parsing URL:', error);
      // Fallback to the old method if URL parsing fails
      folderPath = lockedVideoUrl.replace(/^https?:\/\/[^\/]+\//, '').replace(/\/[^\/]+\.m3u8$/, '/');
    }
    
    // Check if the m3u8 filename is one of our special cases
    let m3u8Filename;
    try {
      const url = new URL(lockedVideoUrl);
      m3u8Filename = url.pathname.split('/').pop();
    } catch (error) {
      // Fallback method
      m3u8Filename = lockedVideoUrl.split('/').pop();
    }
    
    console.log(`M3U8 filename: ${m3u8Filename}`);
    
    // Determine original segment name based on m3u8 file type
    let originalSegmentName;
    
    if (m3u8Filename === 'output.m3u8' || m3u8Filename === 'some-name.m3u8') {
      // For output.m3u8 or some-name.m3u8, use segment_%03d.ts format
      originalSegmentName = `segment_${blackoutSegmentNumber.padStart(3, '0')}.ts`;
    } else {
      // Default format for other m3u8 files
      originalSegmentName = `segment_${blackoutSegmentNumber.padStart(3, '0')}.ts`;
    }
    
    const unlockSegmentName = `unlocked_${blackoutSegmentNumber.padStart(3, '0')}.ts`;
    const blackoutSegmentName = `blackout_${blackoutSegmentNumber.padStart(3, '0')}.ts`;
    
    // Construct the full segment key for the original segment
    const fullSegmentKey = `${folderPath}${originalSegmentName}`;
    
    console.log(`Folder Path: ${folderPath}`);
    console.log(`Original Segment Name: ${originalSegmentName}`);
    console.log(`Full Segment Key: ${fullSegmentKey}`);

    console.log(`Fetching segment v2: ${fullSegmentKey} from bucket: ${storageMetaData.awsBucketName}`);

    // Get the segment from S3
    const command = new GetObjectCommand({
      Bucket: storageMetaData.awsBucketName,
      Key: fullSegmentKey
    });

    const response = await s3Client.send(command);
    
    // Create the local hls_output directory if it doesn't exist
    const hlsOutputDir = path.join(__dirname, '..', '..', 'hls_output');
    if (!fs.existsSync(hlsOutputDir)) {
      fs.mkdirSync(hlsOutputDir, { recursive: true });
    }
    
    // Define the local path for saving the segment
    const localSegmentPath = path.join(hlsOutputDir, unlockSegmentName);
    
    // Convert the segment data to a buffer and save to local file
    const chunks = [];
    for await (const chunk of response.Body) {
      chunks.push(chunk);
    }
    const segmentData = Buffer.concat(chunks);
    
    // Write the file to disk
    fs.writeFileSync(localSegmentPath, segmentData);
    
    console.log(`Segment saved to: ${localSegmentPath}`);
    
    // Now, update the blackout.m3u8 file to replace the blackout segment with the unlocked segment
    try {
      // Path to the local m3u8 file
      const m3u8FilePath = path.join(hlsOutputDir, m3u8Filename);
      
      // Check if the m3u8 file exists
      if (fs.existsSync(m3u8FilePath)) {
        // Read the file content
        const m3u8Content = fs.readFileSync(m3u8FilePath, 'utf8');
        
        // Replace the blackout segment with the unlocked segment
        const updatedContent = m3u8Content.replace(
          new RegExp(blackoutSegmentName, 'g'), 
          unlockSegmentName
        );
        
        // Write back the updated content
        fs.writeFileSync(m3u8FilePath, updatedContent);
        
        console.log(`Updated m3u8 file: ${m3u8FilePath}`);
        console.log(`Replaced ${blackoutSegmentName} with ${unlockSegmentName}`);
      } else {
        console.log(`M3U8 file not found: ${m3u8FilePath}`);
      }
    } catch (error) {
      console.error('Error updating m3u8 file:', error);
      // Don't fail the entire request if updating the m3u8 fails
    }
    
    // Return a success response with the URL to access the segment
    return res.status(200).json({
      success: true,
      message: 'Segment saved successfully and m3u8 updated',
      segmentUrl: `/${unlockSegmentName}` // Simplified URL path to access the segment
    });
  } catch (error) {
    console.error('Error in getSegmentV2:', error);
    return res.status(500).json({
      success: false,
      message: 'Error fetching segment',
      error: error.message
    });
  }
};

module.exports = {
  getSegment,
  getSegmentV2
}; 