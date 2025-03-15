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
const { 
  uploadToS3, 
  uploadHlsFilesToS3 
} = require('../../canvas-processing/s3Processing');

const { 
  createM3U8WithExactSegments, 
  updatePlaylistContent, 
  outputDir, 
  TMP_DIR 
} = require('../../canvas-processing/videoProcessing');

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

// POST /create-AES
async function createAES(req, res) {
  try {
    const { storage_type, MetaData, platformId, userId, contentId, locks } = req.body || {};

    if (!MetaData) {
      return res.status(400).json({ message: "Missing MetaData in request body." });
    }
    const { 
      awsAccessKeyId, 
      awsSecretAccessKey, 
      awsRegion, 
      awsBucketName, 
      awsOriginalKey, 
      awsDestinationFolder 
    } = MetaData;
    if (!awsAccessKeyId || !awsSecretAccessKey) {
      return res.status(400).json({ message: "Missing AWS credentials." });
    }
    if (!awsBucketName || !awsOriginalKey || !awsDestinationFolder) {
      return res.status(400).json({ message: "Missing awsBucketName, awsOriginalKey and/or awsDestinationFolder." });
    }
    if (!contentId) {
      return res.status(400).json({ message: "Missing contentId." });
    }

    const s3Client = new S3Client({
      region: awsRegion,
      credentials: { accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey }
    });

    // Process source and get local MP4 path.
    const localMp4Path = await processSourceToLocalMp4(s3Client, awsBucketName, awsOriginalKey);

    // Precompute lock lists.
    const allLocks = locks || [];
    // For HLS conversion, extract only blackout locks.
    const blackoutLocksForHLS = allLocks
      .filter(lock => lock.lock_type === 'blackout-lock')
      .map(lock => ({
        startTime: Number(lock.startTime),
        endTime: Number(lock.endTime)
      }));
    // Precompute locks for the DB record (handle all lock types).
    const dbLocks = allLocks.map(lock => {
      const base = {
        lock_type: lock.lock_type,
        starttime: Number(lock.startTime),
        endtime: Number(lock.endTime)
      };
      if (lock.lock_type === 'form-lock') {
        base.customJson = lock.customJson;
      } else if (lock.lock_type === 'replacement-video-lock') {
        base.replacement_video_url = lock.replacement_video_url;
      }
      return base;
    });

    // Process the MP4 into HLS playlists using the blackout locks.
    const { normalPlaylistPath, blackoutPlaylistPath } = createM3U8WithExactSegments(localMp4Path, blackoutLocksForHLS);

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

    // Clean up local MP4 and HLS output files.
    fs.unlinkSync(localMp4Path);
    const hlsFiles = fs.readdirSync(outputDir);
    for (const file of hlsFiles) {
      fs.unlinkSync(path.join(outputDir, file));
    }

    // Create and save the lock record using the updated schema.
    const newLock = new Lock({
      PlatformID: platformId,
      UserID: userId,
      OriginalContentUrl: `https://${awsBucketName}.s3.${awsRegion}.amazonaws.com/${awsOriginalKey}`,
      LockedContentUrl: blackoutUrl,
      contentId,
      storage_type,
      locks: dbLocks
    });
    await newLock.save();

    return res.status(201).json({
      message: 'Lock created successfully',
      lock_id: newLock._id,
    });
  } catch (error) {
    console.error("Error in /create-AES:", error);
    return res.status(500).json({ message: 'Server error', error: error.message });
  }
}

// POST /modify-AES
async function modifyAES(req, res) {
  try {
    const { storage_type, MetaData, lockId, newLocks, folder } = req.body;
    if (!MetaData || !lockId || !newLocks || !folder) {
      return res.status(400).json({ message: "Missing required fields." });
    }
    const { awsAccessKeyId, awsSecretAccessKey, awsRegion, awsBucketName } = MetaData;
    if (!awsAccessKeyId || !awsSecretAccessKey || !awsRegion || !awsBucketName) {
      return res.status(400).json({ message: "Missing required AWS data." });
    }

    // Find the lock document by its _id.
    const lock = await Lock.findById(lockId);
    if (!lock) {
      return res.status(404).json({ message: "Lock not found." });
    }
    // Extract original video key from OriginalContentUrl.
    const originalUrl = lock.OriginalContentUrl;
    const urlParts = originalUrl.split('.amazonaws.com/');
    if (urlParts.length < 2) {
      return res.status(500).json({ message: "Invalid original content URL." });
    }
    const awsOriginalKey = urlParts[1];
    const s3Client = new S3Client({
      region: awsRegion,
      credentials: { accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey }
    });

    // Process source and get local MP4 path.
    const localMp4Path = await processSourceToLocalMp4(s3Client, awsBucketName, awsOriginalKey);
    console.log(`Local MP4 path: ${localMp4Path}, size: ${fs.statSync(localMp4Path).size} bytes`);

    // Precompute new lock lists.
    const allNewLocks = newLocks || [];
    const blackoutLocksForHLS = allNewLocks
      .filter(lock => lock.lock_type === 'blackout-lock')
      .map(lock => ({
        startTime: Number(lock.startTime),
        endTime: Number(lock.endTime)
      }));
    const dbNewLocks = allNewLocks.map(lock => {
      const base = {
        lock_type: lock.lock_type,
        starttime: Number(lock.startTime),
        endtime: Number(lock.endTime)
      };
      if (lock.lock_type === 'form-lock') {
        base.customJson = lock.customJson;
      } else if (lock.lock_type === 'replacement-video-lock') {
        base.replacement_video_url = lock.replacement_video_url;
      }
      return base;
    });

    // Delete existing folder content in S3.
    const listParams = { Bucket: awsBucketName, Prefix: folder };
    const listCommand = new ListObjectsV2Command(listParams);
    const listData = await s3Client.send(listCommand);
    if (listData.Contents && listData.Contents.length > 0) {
      const objectsToDelete = listData.Contents.map(obj => ({ Key: obj.Key }));
      const deleteParams = { Bucket: awsBucketName, Delete: { Objects: objectsToDelete, Quiet: false } };
      const deleteCommand = new DeleteObjectsCommand(deleteParams);
      await s3Client.send(deleteCommand);
    }
    
    // Process HLS files.
    const { normalPlaylistPath, blackoutPlaylistPath } = createM3U8WithExactSegments(localMp4Path, blackoutLocksForHLS);
    const fileUrlMapping = await uploadHlsFilesToS3(s3Client, awsBucketName, folder);
    const updatedNormalPlaylist = updatePlaylistContent(normalPlaylistPath, fileUrlMapping);
    const updatedBlackoutPlaylist = updatePlaylistContent(blackoutPlaylistPath, fileUrlMapping);
    
    const finalNormalKey = folder + 'output.m3u8';
    const finalBlackoutKey = folder + 'blackout.m3u8';
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
    
    // Update the lock record with new locks (all types).
    lock.locks = dbNewLocks;
    lock.LockedContentUrl = blackoutUrl;
    await lock.save();
    
    return res.status(200).json({
      message: "Lock modified successfully",
      lock_id: lock._id,
    });
  } catch (error) {
    console.error("Error in /modify-AES:", error);
    return res.status(500).json({ message: error.message });
  }
}
  
// POST /delete-AES
async function deleteAES(req, res) {
  try {
    const { storage_type, MetaData, lockId, folderPrefix } = req.body;
    console.log(storage_type, MetaData, lockId);
    if (!MetaData || !lockId) {
      return res.status(400).json({ message: "Missing MetaData or lockId in request body." });
    }
    // Find the lock document by its _id.
    const lock = await Lock.findById(lockId);
    if (!lock) {
      return res.status(404).json({ message: "Lock not found." });
    }
    const contentId = lock.contentId;
    if (!contentId) {
      return res.status(400).json({ message: "Content ID not found in lock document." });
    }

    const { awsAccessKeyId, awsSecretAccessKey, awsRegion, awsBucketName } = MetaData;
    if (!awsAccessKeyId || !awsSecretAccessKey || !awsRegion || !awsBucketName || !folderPrefix) {
      return res.status(400).json({ message: "Missing required AWS data." });
    }
    
    if(storage_type === "AWS"){
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
      return res.status(200).json({ message: "Folder deleted successfully", lockId });
    }else res.send("Invalid storage type");
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
