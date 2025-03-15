// canvas-core/controllers/awsController.js
const { execSync } = require('child_process');
const { S3Client, ListObjectsV2Command, GetObjectCommand, DeleteObjectsCommand } = require("@aws-sdk/client-s3");
const { pipeline } = require("stream");
const { promisify } = require("util");
const streamPipeline = promisify(pipeline);
const path = require('path');
const fs = require('fs');
const { TMP_DIR } = require('../../canvas-processing/videoProcessing');
const { downloadFileFromS3 } = require('../../canvas-processing/s3Processing');
/**
 * Downloads all objects in the given S3 folder (prefix) into a local folder.
 * Returns the path to the local folder.
 */
async function downloadM3U8Folder(s3Client, bucketName, folderKey) {
  // Create a unique local folder.
  const localFolder = path.join(TMP_DIR, `m3u8_download_${Date.now()}`);
  if (!fs.existsSync(localFolder)) {
    fs.mkdirSync(localFolder, { recursive: true });
  }
  // List all objects with the given folderKey prefix.
  const listParams = { Bucket: bucketName, Prefix: folderKey };
  const listCommand = new ListObjectsV2Command(listParams);
  const listData = await s3Client.send(listCommand);
  if (!listData.Contents || listData.Contents.length === 0) {
    throw new Error("No files found in the provided folder key.");
  }
  // Download each object into the local folder.
  for (const obj of listData.Contents) {
    if (obj.Key.endsWith('/')) continue; // skip folder markers
    const filename = path.basename(obj.Key);
    const localFilePath = path.join(localFolder, filename);
    const getObjectParams = { Bucket: bucketName, Key: obj.Key };
    const getObjectCommand = new GetObjectCommand(getObjectParams);
    const fileResponse = await s3Client.send(getObjectCommand);
    await streamPipeline(fileResponse.Body, fs.createWriteStream(localFilePath));
  }
  return localFolder;
}

/**
 * Sanitizes a local m3u8 file so that all TS segment lines contain only the filename.
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
 * Handles different input types: m3u8 file, folder containing m3u8, or mp4 file.
 */
async function processSourceToLocalMp4(s3Client, bucketName, awsOriginalKey) {
  let localMp4Path;
  const ext = path.extname(awsOriginalKey).toLowerCase();

  if (ext === '.m3u8') {
    // awsOriginalKey is a file. Get its folder.
    const folderKey = path.dirname(awsOriginalKey) + '/';
    const localFolder = await downloadM3U8Folder(s3Client, bucketName, folderKey);
    const m3u8Filename = path.basename(awsOriginalKey);
    const localM3u8Path = path.join(localFolder, m3u8Filename);
    sanitizeLocalM3U8(localM3u8Path);
    localMp4Path = path.join(TMP_DIR, `${Date.now()}-converted.mp4`);
    execSync(`ffmpeg -protocol_whitelist "file,http,https,tcp,tls" -i "${localM3u8Path}" -c copy "${localMp4Path}"`);
    fs.rmSync(localFolder, { recursive: true, force: true });
  } else if (!ext) {
    // awsOriginalKey is assumed to be a folder.
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
    // awsOriginalKey is assumed to be an MP4 file.
    localMp4Path = path.join(TMP_DIR, `${Date.now()}-original.mp4`);
    await downloadFileFromS3(s3Client, bucketName, awsOriginalKey, localMp4Path);
  }

  // Validate the MP4 file.
  try {
    execSync(`ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of default=noprint_wrappers=1:nokey=1 "${localMp4Path}"`);
  } catch (error) {
    throw new Error(`Invalid or corrupted video file: ${error.message}`);
  }

  return localMp4Path;
}

module.exports = {
  downloadM3U8Folder,
  sanitizeLocalM3U8,
  processSourceToLocalMp4,
};
