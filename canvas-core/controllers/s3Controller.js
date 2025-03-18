const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const { pipeline } = require("stream");
const { promisify } = require("util");
const streamPipeline = promisify(pipeline);
const path = require('path');
const fs = require('fs');
const { outputDir, TMP_DIR } = require('../../canvas-processing/videoProcessing');

// Helper function to format time as HH:MM:SS.mmm.
function formatTime(milliseconds) {
  const totalSeconds = Math.floor(milliseconds / 1000);
  const ms = milliseconds % 1000;
  const seconds = totalSeconds % 60;
  const totalMinutes = Math.floor(totalSeconds / 60);
  const minutes = totalMinutes % 60;
  const hours = Math.floor(totalMinutes / 60);
  return `${hours.toString().padStart(2, '0')}:${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}.${ms.toString().padStart(3, '0')}`;
}

async function getVideoNames(req, res) {
  try {
    const { storageType, storageMetaData } = req.body;
    console.log(storageMetaData);
    if (storageType === "AWS") {
      const { awsAccessKeyId, awsSecretAccessKey, awsRegion, awsBucketName, folderPrefix } = storageMetaData;
      if (!awsAccessKeyId || !awsSecretAccessKey || !awsRegion || !awsBucketName) {
        return res.status(400).json({ message: "Invalid or missing AWS storageMetaData." });
      }
      if (!folderPrefix) {
        return res.status(400).json({ message: "Missing folderPrefix in request body." });
      }
      const prefix = folderPrefix.endsWith('/') ? folderPrefix : folderPrefix + '/';
      
      // Connect to S3 and log time.
      const s3ConnectStart = Date.now();
      const s3Client = new S3Client({
        region: awsRegion,
        credentials: { accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey }
      });
      console.log(`Connected to AWS S3 in ${formatTime(Date.now() - s3ConnectStart)}`);
      
      const listParams = { Bucket: awsBucketName, Delimiter: '/', Prefix: prefix };
      const listStart = Date.now();
      const command = new ListObjectsV2Command(listParams);
      const data = await s3Client.send(command);
      console.log(`Listed S3 objects in ${formatTime(Date.now() - listStart)}`);
      let folders = [];
      if (data.CommonPrefixes) {
        folders = data.CommonPrefixes.map(p => p.Prefix);
      }
      return res.status(200).json({ folders });
    } else {
      return res.send("unsupported");
    }
  } catch (error) {
    console.error("Error in /get-video-names:", error);
    return res.status(500).json({ message: error.message });
  }
}

async function downloadVideo(req, res) {
  try {
    const { storageType, storageMetaData } = req.body;
    const { folderPrefix } = req.body;
    if (storageType === "AWS") {
      const { awsAccessKeyId, awsSecretAccessKey, awsRegion, awsBucketName } = storageMetaData;
      if (!awsAccessKeyId || !awsSecretAccessKey || !awsRegion || !awsBucketName) {
        return res.status(400).json({ message: "Invalid or missing AWS storageMetaData." });
      }
      // Connect to S3.
      const s3ConnectStart = Date.now();
      const s3Client = new S3Client({
        region: awsRegion,
        credentials: { accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey }
      });
      console.log(`Connected to AWS S3 in ${formatTime(Date.now() - s3ConnectStart)}`);
      
      // Clean output directory.
      const cleanStart = Date.now();
      if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir);
      } else {
        const oldFiles = fs.readdirSync(outputDir);
        for (const file of oldFiles) {
          fs.unlinkSync(path.join(outputDir, file));
        }
      }
      console.log(`Cleaned output directory in ${formatTime(Date.now() - cleanStart)}`);
      
      const listParams = { Bucket: awsBucketName, Prefix: folderPrefix };
      const listStart = Date.now();
      const listCommand = new ListObjectsV2Command(listParams);
      const data = await s3Client.send(listCommand);
      console.log(`Listed S3 objects in ${formatTime(Date.now() - listStart)}`);
      
      if (!data.Contents || data.Contents.length === 0) {
        return res.status(404).json({ message: "No files found in that prefix." });
      }
      const s3UrlPrefix = `https://${awsBucketName}.s3.${awsRegion}.amazonaws.com/${folderPrefix}`;
      for (const obj of data.Contents) {
        if (obj.Key.endsWith('/')) continue;
        const getObjectParams = { Bucket: awsBucketName, Key: obj.Key };
        const getObjectCommand = new GetObjectCommand(getObjectParams);
        const fileDownloadStart = Date.now();
        const fileResponse = await s3Client.send(getObjectCommand);
        const relative = obj.Key.substring(folderPrefix.length);
        const localFilePath = path.join(outputDir, relative);
        // Ensure directory exists
        const dirPath = path.dirname(localFilePath);
        if (!fs.existsSync(dirPath)) {
          fs.mkdirSync(dirPath, { recursive: true });
        }
        await streamPipeline(fileResponse.Body, fs.createWriteStream(localFilePath));
        console.log(`Downloaded file ${relative} in ${formatTime(Date.now() - fileDownloadStart)}`);
        if (localFilePath.endsWith('.m3u8')) {
          let content = fs.readFileSync(localFilePath, 'utf-8');
          // Strip any S3 URLs from the m3u8 file - only keep segment filenames
          content = content.split('\n').map(line => {
            if (line.trim().endsWith('.ts')) {
              const parts = line.trim().split('/');
              return parts[parts.length - 1];
            }
            return line;
          }).join('\n');
          fs.writeFileSync(localFilePath, content);
        }
      }
      return res.status(200).json({ message: "Folder downloaded successfully" });
    } else {
      return res.send("unsupported");
    }
  } catch (error) {
    console.error("Error in /download-video:", error);
    return res.status(500).json({ message: error.message });
  }
}

module.exports = {
  getVideoNames,
  downloadVideo
};
