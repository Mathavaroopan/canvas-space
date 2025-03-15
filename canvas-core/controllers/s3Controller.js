// canvas-core/controllers/s3Controller.js
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require("@aws-sdk/client-s3");
const { pipeline } = require("stream");
const { promisify } = require("util");
const streamPipeline = promisify(pipeline);
const path = require('path');
const fs = require('fs');
const { outputDir } = require('../../canvas-processing/videoProcessing');

async function getVideoNames(req, res) {
  try {
    const { storage_type, MetaData } = req.body;
    console.log(MetaData);
    if (storage_type === "AWS") {
      const { awsAccessKeyId, awsSecretAccessKey, awsRegion, awsBucketName, folderPrefix } = MetaData;
      if (!awsAccessKeyId || !awsSecretAccessKey || !awsRegion || !awsBucketName) {
        return res.status(400).json({ message: "Invalid or missing AWS MetaData." });
      }
      if (!folderPrefix) {
        return res.status(400).json({ message: "Missing folderPrefix in request body." });
      }
      const prefix = folderPrefix.endsWith('/') ? folderPrefix : folderPrefix + '/';
      const s3Client = new S3Client({
        region: awsRegion,
        credentials: { accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey }
      });
      const listParams = { Bucket: awsBucketName, Delimiter: '/', Prefix: prefix };
      const command = new ListObjectsV2Command(listParams);
      const data = await s3Client.send(command);
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
    console.log(req.body);
    const { storage_type, MetaData, folderPrefix } = req.body;
    // Check storage type from the request body
    if (storage_type === "AWS") {
      const { awsAccessKeyId, awsSecretAccessKey, awsRegion, awsBucketName } = MetaData;
      if (!awsAccessKeyId || !awsSecretAccessKey || !awsRegion || !awsBucketName) {
        return res.status(400).json({ message: "Invalid or missing AWS MetaData." });
      }
      const s3Client = new S3Client({
        region: awsRegion,
        credentials: { accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey }
      });
      if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir);
      } else {
        const oldFiles = fs.readdirSync(outputDir);
        for (const file of oldFiles) {
          fs.unlinkSync(path.join(outputDir, file));
        }
      }
      console.log(MetaData, folderPrefix);
      const listParams = { Bucket: awsBucketName, Prefix: folderPrefix };
      const listCommand = new ListObjectsV2Command(listParams);
      const data = await s3Client.send(listCommand);
      if (!data.Contents || data.Contents.length === 0) {
        return res.status(404).json({ message: "No files found in that prefix." });
      }
      const s3UrlPrefix = `https://${awsBucketName}.s3.${awsRegion}.amazonaws.com/${folderPrefix}`;
      for (const obj of data.Contents) {
        if (obj.Key.endsWith('/')) continue;
        const getObjectParams = { Bucket: awsBucketName, Key: obj.Key };
        const getObjectCommand = new GetObjectCommand(getObjectParams);
        const fileResponse = await s3Client.send(getObjectCommand);
        const relative = obj.Key.substring(folderPrefix.length);
        const localFilePath = path.join(outputDir, relative);
        // Ensure directory exists
        const dirPath = path.dirname(localFilePath);
        if (!fs.existsSync(dirPath)) {
          fs.mkdirSync(dirPath, { recursive: true });
        }
        await streamPipeline(fileResponse.Body, fs.createWriteStream(localFilePath));
        if (localFilePath.endsWith('.m3u8')) {
          let content = fs.readFileSync(localFilePath, 'utf-8');
          content = content.split('\n').map(line => {
            if (line.startsWith(s3UrlPrefix)) {
              return line.replace(s3UrlPrefix, '');
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
