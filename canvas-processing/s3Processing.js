const fs = require('fs');
const { pipeline } = require("stream");
const { promisify } = require("util");
const streamPipeline = promisify(pipeline);
const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const { Upload } = require("@aws-sdk/lib-storage");
const path = require('path');
const { outputDir } = require('./config');

async function downloadFileFromS3(s3Client, bucketName, key, localPath) {
  const getParams = { Bucket: bucketName, Key: key };
  const getCommand = new GetObjectCommand(getParams);
  const data = await s3Client.send(getCommand);
  await streamPipeline(data.Body, fs.createWriteStream(localPath));
}

/**
 * Uploads a file buffer to S3.
 */
async function uploadToS3(s3Client, fileBuffer, bucketName, key, contentType) {
  try {
    const uploadParams = {
      Bucket: bucketName,
      Key: key,
      Body: fileBuffer,
      ContentType: contentType,
    };
    const parallelUploads3 = new Upload({
      client: s3Client,
      params: uploadParams,
    });
    const result = await parallelUploads3.done();
    return result.Location || `https://${bucketName}.s3.amazonaws.com/${key}`;
  } catch (error) {
    console.error("S3 upload error:", error);
    throw error;
  }
}

/**
 * Uploads all files in the output directory to S3 under the given prefix.
 * Returns a mapping from local file names to S3 URLs.
 */
async function uploadHlsFilesToS3(s3Client, bucketName, prefix) {
  const files = fs.readdirSync(outputDir);
  const fileUrlMapping = {};
  for (const file of files) {
    const filePath = path.join(outputDir, file);
    const fileBuffer = fs.readFileSync(filePath);
    let contentType = 'application/octet-stream';
    if (file.endsWith('.m3u8')) {
      contentType = 'application/vnd.apple.mpegurl';
    } else if (file.endsWith('.ts')) {
      contentType = 'video/MP2T';
    }
    const key = `${prefix}${file}`;
    const url = await uploadToS3(s3Client, fileBuffer, bucketName, key, contentType);
    fileUrlMapping[file] = url;
  }
  return fileUrlMapping;
}

module.exports = {
  downloadFileFromS3,
  uploadToS3,
  uploadHlsFilesToS3
};
