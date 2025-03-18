const { execSync } = require('child_process');
const { S3Client, ListObjectsV2Command, DeleteObjectsCommand, HeadObjectCommand, PutObjectCommand } = require("@aws-sdk/client-s3");
const { pipeline } = require("stream");
const { promisify } = require("util");
const streamPipeline = promisify(pipeline);
const path = require('path');
const fs = require('fs');

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

// Import processing functions.
const { uploadToS3, uploadHlsFilesToS3 } = require('../../canvas-processing/s3Processing');
const { createM3U8WithExactSegments, updatePlaylistContent, processSourceToLocalMp4, outputDir } = require('../../canvas-processing/videoProcessing');

// Import Mongoose models.
const Lock = require('../models/Lock');
const Platform = require('../models/Platform');
const User = require('../models/User');

// Helper function to extract S3 key from a URL.
function extractS3Key(url) {
  try {
    const urlObj = new URL(url);
    return urlObj.pathname.substring(1);
  } catch (err) {
    return null;
  }
}

// Helper function to extract folder key (without a leading slash).
function extractS3Folder(url) {
  try {
    const urlObj = new URL(url);
    const fullPath = urlObj.pathname; // e.g., "/AES-videos/platform-testing/new-blackout.m3u8"
    return fullPath.substring(1, fullPath.lastIndexOf('/') + 1);
  } catch (err) {
    return null;
  }
}

// Helper to ensure a folder exists in S3.
async function ensureFolderExists(s3Client, bucketName, folderKey) {
  try {
    await s3Client.send(new HeadObjectCommand({ Bucket: bucketName, Key: folderKey }));
  } catch (error) {
    if (error.name === 'NotFound') {
      await s3Client.send(new PutObjectCommand({ Bucket: bucketName, Key: folderKey, Body: '' }));
    } else {
      throw error;
    }
  }
}

// POST /create-AES
async function createAES(req, res) {
  try {
    const { storageType, storageMetaData, inputVideoUrl, lockedVideoUrl, platformName, userName, contentId, locks } = req.body || {};

    if (!storageMetaData) {
      return res.status(400).json({ message: "Missing storageMetaData in request body." });
    }
    if (!inputVideoUrl || !lockedVideoUrl) {
      return res.status(400).json({ message: "Missing inputVideoUrl or lockedVideoUrl." });
    }
    if (!contentId) {
      return res.status(400).json({ message: "Missing contentId." });
    }
    if (!platformName || !userName) {
      return res.status(400).json({ message: "Missing platformName or userName in request body." });
    }

    const lock = await Lock.findOne({ OriginalContentUrl: inputVideoUrl });
        if (lock) {
          return res.status(404).json({ message: "Locks are already created for the video. You can still add/modify/remove the locks using modify-AES API", lockId : lock._id });
    }

    // Query Platform and User collections.
    const platform = await Platform.findOne({ PlatformName: platformName });
    if (!platform) {
      return res.status(400).json({ message: "Platform not found." });
    }
    const user = await User.findOne({ Name: userName });
    if (!user) {
      return res.status(400).json({ message: "User not found." });
    }

    // Precompute lock lists.
    const allLocks = locks || [];
    const blackoutLocksForHLS = allLocks
      .filter(lock => lock.lock_type === 'blackout-lock')
      .map(lock => ({
        startTime: Number(lock.startTime),
        endTime: Number(lock.endTime)
      }));
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

    if (storageType === "AWS") {
      const { awsAccessKeyId, awsSecretAccessKey, awsRegion, awsBucketName } = storageMetaData;
      if (!awsAccessKeyId || !awsSecretAccessKey) {
        return res.status(400).json({ message: "Missing AWS credentials." });
      }
      if (!awsBucketName) {
        return res.status(400).json({ message: "Missing awsBucketName." });
      }

      const awsOriginalKey = extractS3Key(inputVideoUrl);
      if (!awsOriginalKey) {
        return res.status(400).json({ message: "Invalid inputVideoUrl." });
      }
      const destinationFolder = extractS3Folder(lockedVideoUrl);
      if (!destinationFolder) {
        return res.status(400).json({ message: "Invalid lockedVideoUrl." });
      }

      // Split destinationFolder into parent and child.
      const parts = destinationFolder.split('/').filter(Boolean); // e.g., ["AES-videos", "new-filename"]
      const parentFolder = parts[0] + '/'; // e.g., "AES-videos/"
      const childFolder = destinationFolder; // e.g., "AES-videos/new-filename/"
      // Derive custom playlist file names from URLs.
      const normalFileName = path.basename(inputVideoUrl);   // e.g., "new-name.m3u8"
      const blackoutFileName = path.basename(lockedVideoUrl);  // e.g., "new-blackout.m3u8"

      // Connect to S3 and log time taken.
      const s3ConnectStart = Date.now();
      const s3Client = new S3Client({
        region: awsRegion,
        credentials: { accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey }
      });
      console.log(`Connected to AWS S3 in ${formatTime(Date.now() - s3ConnectStart)}`);

      // Ensure folders exist in S3.
      const folderParentStart = Date.now();
      await ensureFolderExists(s3Client, awsBucketName, parentFolder);
      console.log(`Ensured parent folder (${parentFolder}) exists in ${formatTime(Date.now() - folderParentStart)}`);
      
      const folderChildStart = Date.now();
      await ensureFolderExists(s3Client, awsBucketName, childFolder);
      console.log(`Ensured child folder (${childFolder}) exists in ${formatTime(Date.now() - folderChildStart)}`);

      // Convert source to local MP4.
      const mp4ConversionStart = Date.now();
      const localMp4Path = await processSourceToLocalMp4(s3Client, awsBucketName, awsOriginalKey);
      console.log(`Converted source to local MP4 in ${formatTime(Date.now() - mp4ConversionStart)}`);

      // Create playlists (mp4 to m3u8 conversion).
      const m3u8ConversionStart = Date.now();
      const { normalPlaylistPath, blackoutPlaylistPath } = createM3U8WithExactSegments(
        localMp4Path,
        blackoutLocksForHLS,
        normalFileName,
        blackoutFileName
      );
      console.log(`Converted MP4 to HLS playlists in ${formatTime(Date.now() - m3u8ConversionStart)}`);

      // Upload HLS files to S3.
      const hlsUploadStart = Date.now();
      const fileUrlMapping = await uploadHlsFilesToS3(s3Client, awsBucketName, childFolder);
      console.log(`Uploaded HLS segments to S3 in ${formatTime(Date.now() - hlsUploadStart)}`);

      // Update playlist contents.
      const normalPlaylistUpdateStart = Date.now();
      const updatedNormalPlaylist = updatePlaylistContent(normalPlaylistPath, fileUrlMapping);
      console.log(`Updated normal playlist content in ${formatTime(Date.now() - normalPlaylistUpdateStart)}`);

      const blackoutPlaylistUpdateStart = Date.now();
      const updatedBlackoutPlaylist = updatePlaylistContent(blackoutPlaylistPath, fileUrlMapping);
      console.log(`Updated blackout playlist content in ${formatTime(Date.now() - blackoutPlaylistUpdateStart)}`);

      // Upload normal playlist to S3.
      const normalUploadStart = Date.now();
      const finalNormalKey = childFolder + normalFileName;
      const normalUrl = await uploadToS3(
        s3Client,
        Buffer.from(updatedNormalPlaylist, 'utf8'),
        awsBucketName,
        finalNormalKey,
        'application/vnd.apple.mpegurl'
      );
      console.log(`Uploaded normal playlist to S3 in ${formatTime(Date.now() - normalUploadStart)}`);

      // Upload blackout playlist to S3.
      const blackoutUploadStart = Date.now();
      const finalBlackoutKey = childFolder + blackoutFileName;
      const blackoutUrl = await uploadToS3(
        s3Client,
        Buffer.from(updatedBlackoutPlaylist, 'utf8'),
        awsBucketName,
        finalBlackoutKey,
        'application/vnd.apple.mpegurl'
      );
      console.log(`Uploaded blackout playlist to S3 in ${formatTime(Date.now() - blackoutUploadStart)}`);

      // Cleanup local files.
      fs.unlinkSync(localMp4Path);
      const hlsFiles = fs.readdirSync(outputDir);
      for (const file of hlsFiles) {
        fs.unlinkSync(path.join(outputDir, file));
      }
      const newLock = new Lock({
        PlatformID: platform._id,
        UserID: user._id,
        OriginalContentUrl: inputVideoUrl,
        LockedContentUrl: blackoutUrl,
        contentId: contentId,
        storageType: storageType,
        locks: dbLocks
      });
      await newLock.save();

      return res.status(201).json({
        message: 'Lock created successfully',
        lock_id: newLock._id
      });
    } else {
      return res.status(400).json({ message: "Invalid storage type" });
    }
  } catch (error) {
    console.error("Error in /create-AES:", error);
    return res.status(500).json({ message: 'Server error', error: error.message });
  }
}

// POST /modify-AES
async function modifyAES(req, res) {
  try {
    const { storageType, storageMetaData, lockId, newLocks } = req.body;
    if (!storageMetaData || !lockId || !newLocks) {
      return res.status(400).json({ message: "Missing required fields." });
    }
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
    if (storageType === "AWS") {
      const { awsAccessKeyId, awsSecretAccessKey, awsRegion, awsBucketName } = storageMetaData;
      if (!awsAccessKeyId || !awsSecretAccessKey || !awsRegion || !awsBucketName) {
        return res.status(400).json({ message: "Missing required AWS data." });
      }
      const lock = await Lock.findById(lockId);
      if (!lock) {
        return res.status(404).json({ message: "Lock not found." });
      }
      const awsOriginalKey = extractS3Key(lock.OriginalContentUrl);
      if (!awsOriginalKey) {
        return res.status(500).json({ message: "Invalid OriginalContentUrl in lock document." });
      }
      // Connect to S3 and log time taken.
      const s3ConnectStart = Date.now();
      const s3Client = new S3Client({
        region: awsRegion,
        credentials: { accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey }
      });
      console.log(`Connected to AWS S3 in ${formatTime(Date.now() - s3ConnectStart)}`);

      // Convert source to local MP4.
      const mp4ConversionStart = Date.now();
      const localMp4Path = await processSourceToLocalMp4(s3Client, awsBucketName, awsOriginalKey);
      console.log(`Converted source to local MP4 in ${formatTime(Date.now() - mp4ConversionStart)}`);

      console.log(`Local MP4 path: ${localMp4Path}, size: ${fs.statSync(localMp4Path).size} bytes`);
      const lockedUrl = lock.LockedContentUrl;
      if (!lockedUrl) {
        return res.status(400).json({ message: "LockedContentUrl missing in lock document." });
      }
      const folderToDelete = extractS3Folder(lockedUrl);
      if (!folderToDelete) {
        return res.status(400).json({ message: "Invalid LockedContentUrl in lock document." });
      }
      // Delete existing folder in S3.
      const deletionStart = Date.now();
      const listParams = { Bucket: awsBucketName, Prefix: folderToDelete };
      const listCommand = new ListObjectsV2Command(listParams);
      const listData = await s3Client.send(listCommand);
      if (listData.Contents && listData.Contents.length > 0) {
        const objectsToDelete = listData.Contents.map(obj => ({ Key: obj.Key }));
        const deleteParams = { Bucket: awsBucketName, Delete: { Objects: objectsToDelete, Quiet: false } };
        const deleteCommand = new DeleteObjectsCommand(deleteParams);
        await s3Client.send(deleteCommand);
      }
      console.log(`Deleted previous S3 folder (${folderToDelete}) in ${formatTime(Date.now() - deletionStart)}`);

      const normalFileName = path.basename(lock.OriginalContentUrl);
      const blackoutFileName = path.basename(lock.LockedContentUrl);
      // Convert MP4 to new playlists.
      const m3u8ConversionStart = Date.now();
      const { normalPlaylistPath, blackoutPlaylistPath } = createM3U8WithExactSegments(
        localMp4Path,
        blackoutLocksForHLS,
        normalFileName,
        blackoutFileName
      );
      console.log(`Converted MP4 to HLS playlists in ${formatTime(Date.now() - m3u8ConversionStart)}`);

      // Upload HLS segments.
      const hlsUploadStart = Date.now();
      const fileUrlMapping = await uploadHlsFilesToS3(s3Client, awsBucketName, folderToDelete);
      console.log(`Uploaded HLS segments to S3 in ${formatTime(Date.now() - hlsUploadStart)}`);

      const normalPlaylistUpdateStart = Date.now();
      const updatedNormalPlaylist = updatePlaylistContent(normalPlaylistPath, fileUrlMapping);
      console.log(`Updated normal playlist content in ${formatTime(Date.now() - normalPlaylistUpdateStart)}`);

      const blackoutPlaylistUpdateStart = Date.now();
      const updatedBlackoutPlaylist = updatePlaylistContent(blackoutPlaylistPath, fileUrlMapping);
      console.log(`Updated blackout playlist content in ${formatTime(Date.now() - blackoutPlaylistUpdateStart)}`);

      // Upload updated playlists.
      const normalUploadStart = Date.now();
      const finalNormalKey = folderToDelete + normalFileName;
      const normalUrl = await uploadToS3(
        s3Client,
        Buffer.from(updatedNormalPlaylist, 'utf8'),
        awsBucketName,
        finalNormalKey,
        'application/vnd.apple.mpegurl'
      );
      console.log(`Uploaded updated normal playlist to S3 in ${formatTime(Date.now() - normalUploadStart)}`);

      const blackoutUploadStart = Date.now();
      const finalBlackoutKey = folderToDelete + blackoutFileName;
      const blackoutUrl = await uploadToS3(
        s3Client,
        Buffer.from(updatedBlackoutPlaylist, 'utf8'),
        awsBucketName,
        finalBlackoutKey,
        'application/vnd.apple.mpegurl'
      );
      console.log(`Uploaded updated blackout playlist to S3 in ${formatTime(Date.now() - blackoutUploadStart)}`);

      // Cleanup local files.
      fs.unlinkSync(localMp4Path);
      const hlsFiles = fs.readdirSync(outputDir);
      for (const file of hlsFiles) {
        fs.unlinkSync(path.join(outputDir, file));
      }
      lock.locks = dbNewLocks;
      lock.LockedContentUrl = blackoutUrl;
      await lock.save();
      return res.status(200).json({
        message: "Lock modified successfully",
        lock_id: lock._id,
      });
    } else {
      return res.status(400).json({ message: "Invalid storage type" });
    }
  } catch (error) {
    console.error("Error in /modify-AES:", error);
    return res.status(500).json({ message: error.message });
  }
}

// POST /delete-AES
async function deleteAES(req, res) {
  try {
    const { storageType, storageMetaData, lockId } = req.body;
    console.log("Delete request:", { storageType, storageMetaData, lockId });
    if (!storageMetaData || !lockId) {
      return res.status(400).json({ message: "Missing storageMetaData or lockId in request body." });
    }
    const lock = await Lock.findById(lockId);
    if (!lock) {
      return res.status(404).json({ message: "Lock not found." });
    }
    const contentId = lock.contentId;
    if (!contentId) {
      return res.status(400).json({ message: "Content ID not found in lock document." });
    }
    if (storageType === "AWS") {
      const { awsAccessKeyId, awsSecretAccessKey, awsRegion, awsBucketName } = storageMetaData;
      if (!awsAccessKeyId || !awsSecretAccessKey || !awsRegion || !awsBucketName) {
        return res.status(400).json({ message: "Missing required AWS data." });
      }
      const lockedUrl = lock.LockedContentUrl;
      if (!lockedUrl) {
        return res.status(400).json({ message: "LockedContentUrl missing in lock document." });
      }
      const folderToDelete = extractS3Folder(lockedUrl);
      if (!folderToDelete) {
        return res.status(400).json({ message: "Invalid LockedContentUrl in lock document." });
      }
      // Connect to S3.
      const s3ConnectStart = Date.now();
      const s3Client = new S3Client({
        region: awsRegion,
        credentials: { accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey }
      });
      console.log(`Connected to AWS S3 in ${formatTime(Date.now() - s3ConnectStart)}`);
      
      // Delete folder from S3.
      const deletionStart = Date.now();
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
      await s3Client.send(deleteCommand);
      console.log(`Deleted S3 folder (${folderToDelete}) in ${formatTime(Date.now() - deletionStart)}`);
      return res.status(200).json({ message: "Folder deleted successfully", lockId });
    } else {
      return res.status(400).json({ message: "Invalid storage type" });
    }
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
