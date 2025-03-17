const { GetObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require("@aws-sdk/lib-storage");
const fs = require('fs');
const path = require('path');
const { outputDir } = require('./config');

/**
 * Uploads a file buffer to S3.
 * @param {S3Client} s3Client 
 * @param {Buffer} fileBuffer 
 * @param {string} bucketName 
 * @param {string} key 
 * @param {string} contentType 
 * @returns {string} URL of the uploaded file.
 */
async function uploadToS3(s3Client, fileBuffer, bucketName, key, contentType) {
  const params = {
    Bucket: bucketName,
    Key: key,
    Body: fileBuffer,
    ContentType: contentType,
  };
  const command = new PutObjectCommand(params);
  await s3Client.send(command);
  return `https://${bucketName}.s3.amazonaws.com/${key}`;
}

/**
 * Uploads all files from the local HLS output directory to S3 under the specified folder.
 * @param {S3Client} s3Client 
 * @param {string} bucketName 
 * @param {string} folder - S3 folder (should end with a slash).
 * @returns {object} Mapping of local filenames to their S3 URLs.
 */
async function uploadHlsFilesToS3(s3Client, bucketName, folder) {
  const localOutputDir = outputDir;
  const files = fs.readdirSync(localOutputDir);
  const fileUrlMapping = {};

  for (const file of files) {
    const filePath = path.join(localOutputDir, file);
    const fileStream = fs.createReadStream(filePath);
    const s3Folder = folder.endsWith('/') ? folder : folder + '/';
    const key = s3Folder + file;
    const upload = new Upload({
      client: s3Client,
      params: {
        Bucket: bucketName,
        Key: key,
        Body: fileStream,
      },
    });
    await upload.done();
    fileUrlMapping[file] = `https://${bucketName}.s3.amazonaws.com/${key}`;
  }
  return fileUrlMapping;
}

/**
 * Downloads a file from S3 to a specified local path.
 * @param {S3Client} s3Client 
 * @param {string} bucketName 
 * @param {string} key 
 * @param {string} localPath 
 */
async function downloadFileFromS3(s3Client, bucketName, key, localPath) {
  const command = new GetObjectCommand({ Bucket: bucketName, Key: key });
  const response = await s3Client.send(command);
  const stream = response.Body;
  const writeStream = fs.createWriteStream(localPath);
  return new Promise((resolve, reject) => {
    stream.pipe(writeStream)
      .on('finish', resolve)
      .on('error', reject);
  });
}

module.exports = {
  uploadToS3,
  uploadHlsFilesToS3,
  downloadFileFromS3
};
