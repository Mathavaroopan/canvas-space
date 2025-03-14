// canvas-core/controllers/awsController.js
const { execSync } = require('child_process');
const { S3Client, ListObjectsV2Command, GetObjectCommand, DeleteObjectsCommand } = require("@aws-sdk/client-s3");
const { pipeline } = require("stream");
const { promisify } = require("util");
const streamPipeline = promisify(pipeline);
const path = require('path');
const fs = require('fs');
const { TMP_DIR } = require('../../canvas-processing/videoProcessing');

/**
 * Returns an AWS S3 client.
 * MetaData must include: awsAccessKeyId, awsSecretAccessKey, awsRegion.
 */
function getAWSClient(metaData) {
  return new S3Client({
    region: metaData.awsRegion,
    credentials: { 
      accessKeyId: metaData.awsAccessKeyId, 
      secretAccessKey: metaData.awsSecretAccessKey 
    }
  });
}

/**
 * Downloads a file from S3 to a local path.
 */
async function downloadFileFromS3(s3Client, bucketName, key, localPath) {
  const getParams = { Bucket: bucketName, Key: key };
  const getCommand = new GetObjectCommand(getParams);
  const data = await s3Client.send(getCommand);
  await streamPipeline(data.Body, fs.createWriteStream(localPath));
}

/**
 * Downloads all objects in the given S3 folder (prefix) into a local folder.
 * Returns the local folder path.
 */
async function downloadM3U8Folder(s3Client, bucketName, folderKey) {
  const localFolder = path.join(TMP_DIR, `m3u8_download_${Date.now()}`);
  if (!fs.existsSync(localFolder)) {
    fs.mkdirSync(localFolder, { recursive: true });
  }
  const listParams = { Bucket: bucketName, Prefix: folderKey };
  const listCommand = new ListObjectsV2Command(listParams);
  const listData = await s3Client.send(listCommand);
  if (!listData.Contents || listData.Contents.length === 0) {
    throw new Error("No files found in the provided folder key.");
  }
  for (const obj of listData.Contents) {
    if (obj.Key.endsWith('/')) continue;
    const filename = path.basename(obj.Key);
    const localFilePath = path.join(localFolder, filename);
    const getParams = { Bucket: bucketName, Key: obj.Key };
    const getCommand = new GetObjectCommand(getParams);
    const data = await s3Client.send(getCommand);
    await streamPipeline(data.Body, fs.createWriteStream(localFilePath));
  }
  return localFolder;
}

/**
 * Reads and sanitizes a local m3u8 file so that TS segment lines contain only the filename.
 */
function sanitizeLocalM3U8(m3u8Path) {
  let content = fs.readFileSync(m3u8Path, 'utf8');
  const sanitized = content.split('\n').map(line => {
    if (line.trim().endsWith('.ts')) {
      const parts = line.trim().split('/');
      return parts[parts.length - 1];
    }
    return line;
  }).join('\n');
  fs.writeFileSync(m3u8Path, sanitized);
}

/**
 * Processes the S3 original key and downloads the file/folder to a local MP4 path.
 * Handles .m3u8 file, folder containing m3u8, or an MP4 file.
 */
async function processSourceToLocalMp4(s3Client, bucketName, awsOriginalKey) {
  let localMp4Path;
  const ext = path.extname(awsOriginalKey).toLowerCase();
  if (ext === '.m3u8') {
    const folderKey = path.dirname(awsOriginalKey) + '/';
    const localFolder = await downloadM3U8Folder(s3Client, bucketName, folderKey);
    const m3u8Filename = path.basename(awsOriginalKey);
    const localM3u8Path = path.join(localFolder, m3u8Filename);
    sanitizeLocalM3U8(localM3u8Path);
    localMp4Path = path.join(TMP_DIR, `${Date.now()}-converted.mp4`);
    execSync(`ffmpeg -protocol_whitelist "file,http,https,tcp,tls" -i "${localM3u8Path}" -c copy "${localMp4Path}"`);
    fs.rmSync(localFolder, { recursive: true, force: true });
  } else if (!ext) {
    let folderKey = awsOriginalKey;
    if (!folderKey.endsWith('/')) folderKey += '/';
    const localFolder = await downloadM3U8Folder(s3Client, bucketName, folderKey);
    const files = fs.readdirSync(localFolder);
    const m3u8File = files.find(file => file.endsWith('.m3u8'));
    if (!m3u8File) {
      throw new Error("No m3u8 file found in the provided folder.");
    }
    const localM3u8Path = path.join(localFolder, m3u8File);
    sanitizeLocalM3U8(localM3u8Path);
    localMp4Path = path.join(TMP_DIR, `${Date.now()}-converted.mp4`);
    execSync(`ffmpeg -protocol_whitelist "file,http,https,tcp,tls" -i "${localM3u8Path}" -c copy "${localMp4Path}"`);
    fs.rmSync(localFolder, { recursive: true, force: true });
  } else {
    localMp4Path = path.join(TMP_DIR, `${Date.now()}-original.mp4`);
    await downloadFileFromS3(s3Client, bucketName, awsOriginalKey, localMp4Path);
  }
  try {
    execSync(`ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of default=noprint_wrappers=1:nokey=1 "${localMp4Path}"`);
  } catch (error) {
    throw new Error(`Invalid or corrupted video file: ${error.message}`);
  }
  return localMp4Path;
}

/**
 * Deletes all objects within a specified folder from S3.
 */
async function deleteFolder(s3Client, bucketName, folderToDelete) {
  const listParams = { Bucket: bucketName, Prefix: folderToDelete };
  const listCommand = new ListObjectsV2Command(listParams);
  const listData = await s3Client.send(listCommand);
  if (!listData.Contents || listData.Contents.length === 0) {
    throw new Error("No objects found in the specified folder.");
  }
  const objectsToDelete = listData.Contents.map(obj => ({ Key: obj.Key }));
  const deleteParams = { Bucket: bucketName, Delete: { Objects: objectsToDelete, Quiet: false } };
  const deleteCommand = new DeleteObjectsCommand(deleteParams);
  await s3Client.send(deleteCommand);
}

module.exports = {
  getAWSClient,
  downloadFileFromS3,
  downloadM3U8Folder,
  sanitizeLocalM3U8,
  processSourceToLocalMp4,
  deleteFolder
};
