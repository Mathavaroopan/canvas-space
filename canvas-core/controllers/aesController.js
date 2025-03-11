// canvas-core/controllers/aesController.js
const { execSync } = require('child_process');
const { S3Client, ListObjectsV2Command, GetObjectCommand, DeleteObjectsCommand } = require("@aws-sdk/client-s3");
const { Upload } = require("@aws-sdk/lib-storage");
const { pipeline } = require("stream");
const { promisify } = require("util");
const streamPipeline = promisify(pipeline);
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');

// Import processing functions.
const { uploadToS3, uploadHlsFilesToS3 } = require('../../canvas-processing/s3Processing');
const { createM3U8WithExactSegments, updatePlaylistContent, outputDir, TMP_DIR } = require('../../canvas-processing/videoProcessing');

// Import Mongoose model.
const Lock = require('../models/Lock');

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
    // Skip if the object key ends with '/' (i.e. a folder marker).
    if (obj.Key.endsWith('/')) continue;
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
    // If awsOriginalKey is a file (ends with .m3u8), determine its folder.
    const folderKey = path.dirname(awsOriginalKey) + '/';
    // Download all objects from that folder.
    const localFolder = await downloadM3U8Folder(s3Client, bucketName, folderKey);
    // Identify the m3u8 file in the local folder (assume it matches the basename of awsOriginalKey).
    const m3u8Filename = path.basename(awsOriginalKey);
    const localM3u8Path = path.join(localFolder, m3u8Filename);
    // Sanitize the m3u8 file: remove URL prefixes so only TS filenames remain.
    sanitizeLocalM3U8(localM3u8Path);
    // Convert the local m3u8 file to MP4.
    localMp4Path = path.join(TMP_DIR, `${Date.now()}-converted.mp4`);
    execSync(`ffmpeg -protocol_whitelist "file,http,https,tcp,tls" -i "${localM3u8Path}" -c copy "${localMp4Path}"`);
    // Optionally, delete the local folder with the m3u8 and segments.
    fs.rmSync(localFolder, { recursive: true, force: true });
  } else if (!ext) {
    // If no extension is provided, assume awsOriginalKey is a folder.
    let folderKey = awsOriginalKey;
    if (!folderKey.endsWith('/')) folderKey += '/';
    const localFolder = await downloadM3U8Folder(s3Client, bucketName, folderKey);
    // Find the first m3u8 file in the local folder.
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
    // Assume awsOriginalKey is already an MP4 file.
    localMp4Path = path.join(TMP_DIR, `${Date.now()}-original.mp4`);
    await downloadFileFromS3(s3Client, bucketName, awsOriginalKey, localMp4Path);
  }

  // Validate the MP4 file
  try {
    execSync(`ffprobe -v error -select_streams v:0 -show_entries stream=codec_name -of default=noprint_wrappers=1:nokey=1 "${localMp4Path}"`);
  } catch (error) {
    throw new Error(`Invalid or corrupted video file: ${error.message}`);
  }

  return localMp4Path;
}

// POST /create-AES
async function createAES(req, res) {
  try {
    const {
      awsData,
      platformId,
      userId,
      contentId,
      blackoutLocks
    } = req.body || {};

    if (!awsData) {
      return res.status(400).json({ message: "Missing awsData in request body." });
    }
    const {
      awsAccessKeyId,
      awsSecretAccessKey,
      awsRegion,
      awsBucketName,
      awsOriginalKey,
      awsDestinationFolder
    } = awsData;
    if (!awsAccessKeyId || !awsSecretAccessKey) {
      return res.status(400).json({ message: "Missing AWS credentials." });
    }
    if (!awsBucketName || !awsOriginalKey || !awsDestinationFolder) {
      return res.status(400).json({ message: "Missing bucketName, awsOriginalKey and/or awsDestinationFolder." });
    }
    if (!contentId) {
      return res.status(400).json({ message: "Missing contentId." });
    }
    
    const s3Client = new S3Client({
      region: awsRegion,
      credentials: { accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey }
    });

    // Process source and get local MP4 path
    const localMp4Path = await processSourceToLocalMp4(s3Client, awsBucketName, awsOriginalKey);

    // Process the MP4 into HLS playlists.
    const { normalPlaylistPath, blackoutPlaylistPath } = createM3U8WithExactSegments(localMp4Path, blackoutLocks || []);

    // Create a subfolder for the content in S3.
    const baseFolder = awsDestinationFolder.endsWith('/') ? awsDestinationFolder : awsDestinationFolder + '/';
    const uniqueSubfolder = baseFolder + contentId + '/';

    // Upload HLS files to S3.
    const fileUrlMapping = await uploadHlsFilesToS3(s3Client, awsBucketName, uniqueSubfolder);

    // Update playlists with S3 URLs.
    const updatedNormalPlaylist = updatePlaylistContent(normalPlaylistPath, fileUrlMapping);
    const updatedBlackoutPlaylist = updatePlaylistContent(blackoutPlaylistPath, fileUrlMapping);

    const finalNormalKey = uniqueSubfolder + 'output.m3u8';
    const finalBlackoutKey = uniqueSubfolder + 'blackout.m3u8';
    const normalUrl = await uploadToS3(
      s3Client,
      Buffer.from(updatedNormalPlaylist, 'utf8'),
      awsBucketName,
      finalNormalKey,
      'application/vnd.apple.mpegurl'
    );
    const blackoutUrl = await uploadToS3(
      s3Client,
      Buffer.from(updatedBlackoutPlaylist, 'utf8'),
      awsBucketName,
      finalBlackoutKey,
      'application/vnd.apple.mpegurl'
    );

    // Clean up the local MP4 file and HLS output files.
    fs.unlinkSync(localMp4Path);
    const hlsFiles = fs.readdirSync(outputDir);
    for (const file of hlsFiles) {
      fs.unlinkSync(path.join(outputDir, file));
    }

    // Save record in the database.
    const newLockUuid = uuidv4();
    const lockJsonObject = {
      lockId: newLockUuid,
      originalcontentUrl: `https://${awsBucketName}.s3.${awsRegion}.amazonaws.com/${awsOriginalKey}`,
      contentId,
      lockedcontenturl: blackoutUrl,
      locks: {
        "replacement-video-locks": [],
        "image-locks": [],
        "blackout-locks": (blackoutLocks || []).map(lock => ({
          bl_id: uuidv4(),
          startTime: Number(lock.startTime),
          endTime: Number(lock.endTime)
        }))
      }
    };

    const newLock = new Lock({
      PlatformID: platformId,
      UserID: userId,
      OriginalContentUrl: lockJsonObject.originalcontentUrl,
      LockedContentUrl: blackoutUrl,
      contentId: contentId,
      LockJsonObject: lockJsonObject
    });
    await newLock.save();

    return res.status(201).json({
      message: 'Lock created successfully',
      lock_id: newLockUuid,
      normalUrl,
      blackoutUrl
    });
  } catch (error) {
    console.error("Error in /create-AES:", error);
    return res.status(500).json({ message: 'Server error', error: error.message });
  }
}

// POST /modify-AES
async function modifyAES(req, res) {
  try {
    const { awsData, lockId, newBlackoutLocks, folder } = req.body;
    console.log("Lock ID:", lockId);
    if (!awsData || !lockId || !newBlackoutLocks || !folder) {
      return res.status(400).json({ message: "Missing required fields." });
    }
    const { awsAccessKeyId, awsSecretAccessKey, awsRegion, awsBucketName } = awsData;
    if (!awsAccessKeyId || !awsSecretAccessKey || !awsRegion || !awsBucketName) {
      return res.status(400).json({ message: "Missing required AWS data." });
    }
    // Find the lock document using the UUID stored in LockJsonObject.lockId
    const lock = await Lock.findOne({ "LockJsonObject.lockId": lockId });
    if (!lock) {
      return res.status(404).json({ message: "Lock not found." });
    }
    // Extract original video key.
    const originalUrl = lock.LockJsonObject.originalcontentUrl;
    const urlParts = originalUrl.split('.amazonaws.com/');
    if (urlParts.length < 2) {
      return res.status(500).json({ message: "Invalid original content URL." });
    }
    const awsOriginalKey = urlParts[1];
    const s3Client = new S3Client({
      region: awsRegion,
      credentials: {
        accessKeyId: awsAccessKeyId,
        secretAccessKey: awsSecretAccessKey
      }
    });

    // Process source and get local MP4 path - using the shared function
    const localMp4Path = await processSourceToLocalMp4(s3Client, awsBucketName, awsOriginalKey);
    console.log(`Local MP4 path: ${localMp4Path}, file size: ${fs.statSync(localMp4Path).size} bytes`);

    const { normalPlaylistPath, blackoutPlaylistPath } = createM3U8WithExactSegments(localMp4Path, newBlackoutLocks);
    const uniqueSubfolder = folder;
    
    // Delete existing folder content in S3.
    const listParams = { Bucket: awsBucketName, Prefix: uniqueSubfolder };
    const listCommand = new ListObjectsV2Command(listParams);
    const listData = await s3Client.send(listCommand);
    if (listData.Contents && listData.Contents.length > 0) {
      const objectsToDelete = listData.Contents.map(obj => ({ Key: obj.Key }));
      const deleteParams = { Bucket: awsBucketName, Delete: { Objects: objectsToDelete, Quiet: false } };
      const deleteCommand = new DeleteObjectsCommand(deleteParams);
      await s3Client.send(deleteCommand);
    }
    
    const fileUrlMapping = await uploadHlsFilesToS3(s3Client, awsBucketName, uniqueSubfolder);
    const updatedNormalPlaylist = updatePlaylistContent(normalPlaylistPath, fileUrlMapping);
    const updatedBlackoutPlaylist = updatePlaylistContent(blackoutPlaylistPath, fileUrlMapping);
    
    const finalNormalKey = uniqueSubfolder + 'output.m3u8';
    const finalBlackoutKey = uniqueSubfolder + 'blackout.m3u8';
    const normalUrl = await uploadToS3(
      s3Client,
      Buffer.from(updatedNormalPlaylist, 'utf8'),
      awsBucketName,
      finalNormalKey,
      'application/vnd.apple.mpegurl'
    );
    const blackoutUrl = await uploadToS3(
      s3Client,
      Buffer.from(updatedBlackoutPlaylist, 'utf8'),
      awsBucketName,
      finalBlackoutKey,
      'application/vnd.apple.mpegurl'
    );
    
    fs.unlinkSync(localMp4Path);
    const hlsFiles = fs.readdirSync(outputDir);
    for (const file of hlsFiles) {
      fs.unlinkSync(path.join(outputDir, file));
    }
    
    // Update lock record.
    const updatedBlackoutLocks = newBlackoutLocks.map(b => ({
      bl_id: uuidv4(),
      startTime: Number(b.startTime),
      endTime: Number(b.endTime)
    }));
    
    lock.LockJsonObject.locks["blackout-locks"] = updatedBlackoutLocks;
    lock.LockJsonObject.lockedcontenturl = blackoutUrl;
    lock.LockedContentUrl = blackoutUrl; // update top-level field
    await lock.save();
    
    return res.status(200).json({
      message: "Lock modified successfully",
      lock: lock,
      normalUrl,
      blackoutUrl
    });
  } catch (error) {
    console.error("Error in /modify-AES:", error);
    return res.status(500).json({ message: error.message });
  }
}
  
// POST /delete-AES
async function deleteAES(req, res) {
  try {
    const { awsData, lockId } = req.body;
    if (!awsData || !lockId) {
      return res.status(400).json({ message: "Missing awsData or lockId in request body." });
    }
    const { awsAccessKeyId, awsSecretAccessKey, awsRegion, awsBucketName, folderPrefix } = awsData;
    if (!awsAccessKeyId || !awsSecretAccessKey || !awsRegion || !awsBucketName || !folderPrefix) {
      return res.status(400).json({ message: "Missing required AWS data." });
    }
    const lock = await Lock.findOne({ "LockJsonObject.lockId": lockId });
    if (!lock) {
      return res.status(404).json({ message: "Lock not found." });
    }
    const contentId = lock.contentId;
    if (!contentId) {
      return res.status(400).json({ message: "Content ID not found in lock document." });
    }
    const normalizedPrefix = folderPrefix.endsWith('/') ? folderPrefix : folderPrefix + '/';
    const folderToDelete = normalizedPrefix + contentId + '/';
    const s3Client = new S3Client({
      region: awsRegion,
      credentials: { accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey }
    });
    const listParams = { Bucket: awsBucketName, Prefix: folderToDelete };
    const listCommand = new ListObjectsV2Command(listParams);
    const listData = await s3Client.send(listCommand);
    if (!listData.Contents || listData.Contents.length === 0) {
      return res.status(404).json({ message: "No objects found in the specified folder." });
    }
    const objectsToDelete = listData.Contents.map(obj => ({ Key: obj.Key }));
    const deleteParams = {
      Bucket: awsBucketName,
      Delete: { Objects: objectsToDelete, Quiet: false }
    };
    const deleteCommand = new DeleteObjectsCommand(deleteParams);
    const deleteResult = await s3Client.send(deleteCommand);
    return res.status(200).json({ message: "Folder deleted successfully", deleteResult });
  } catch (error) {
    console.error("Error in /delete-AES:", error);
    return res.status(500).json({ message: error.message });
  }
}

module.exports = {
  createAES,
  modifyAES,
  deleteAES
};