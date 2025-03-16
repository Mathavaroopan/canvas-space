const { execSync } = require('child_process');
const { S3Client, ListObjectsV2Command, DeleteObjectsCommand, HeadObjectCommand, PutObjectCommand } = require("@aws-sdk/client-s3");
const { pipeline } = require("stream");
const { promisify } = require("util");
const streamPipeline = promisify(pipeline);
const path = require('path');
const fs = require('fs');

// Import processing functions.
const { uploadToS3, uploadHlsFilesToS3 } = require('../../canvas-processing/s3Processing');
const { createM3U8WithExactSegments, updatePlaylistContent, processSourceToLocalMp4, outputDir } = require('../../canvas-processing/videoProcessing');

// Import Mongoose models.
const Lock = require('../models/Lock');
const Platform = require('../models/Platform'); // Platform model
const User = require('../models/User');         // User model

// Helper function to extract S3 key from a URL.
function extractS3Key(url) {
  try {
    const urlObj = new URL(url);
    return urlObj.pathname.substring(1); // remove leading '/'
  } catch (err) {
    return null;
  }
}

// Helper function to extract folder key (without a leading slash).
function extractS3Folder(url) {
  try {
    const urlObj = new URL(url);
    // e.g., urlObj.pathname: "/AES-videos/platform-testing/output.m3u8"
    // We want: "AES-videos/platform-testing/"
    const fullPath = urlObj.pathname; // includes leading slash
    return fullPath.substring(1, fullPath.lastIndexOf('/') + 1);
  } catch (err) {
    return null;
  }
}

// Helper to ensure a folder exists in S3 by creating an empty object if needed.
async function ensureFolderExists(s3Client, bucketName, folderKey) {
  try {
    await s3Client.send(new HeadObjectCommand({ Bucket: bucketName, Key: folderKey }));
    // If HeadObjectCommand succeeds, folder exists.
  } catch (error) {
    if (error.name === 'NotFound') {
      // Create an empty object to simulate a folder.
      await s3Client.send(new PutObjectCommand({ Bucket: bucketName, Key: folderKey, Body: '' }));
    } else {
      throw error;
    }
  }
}

// POST /create-AES
async function createAES(req, res) {
  try {
    const {
      storageType,
      storageMetaData,
      inputVideoUrl,
      lockedVideoUrl,
      platformName,
      userName,
      contentId,
      locks
    } = req.body || {};
    console.log("createAES");
    console.log(req.body);

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

    // Query Platform and User collections to obtain their ObjectIds.
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
      // Extract AWS credentials.
      const { awsAccessKeyId, awsSecretAccessKey, awsRegion, awsBucketName } = storageMetaData;
      if (!awsAccessKeyId || !awsSecretAccessKey) {
        return res.status(400).json({ message: "Missing AWS credentials." });
      }
      if (!awsBucketName) {
        return res.status(400).json({ message: "Missing awsBucketName." });
      }

      // Derive S3 keys.
      const awsOriginalKey = extractS3Key(inputVideoUrl);
      if (!awsOriginalKey) {
        return res.status(400).json({ message: "Invalid inputVideoUrl." });
      }
      const destinationFolder = extractS3Folder(lockedVideoUrl);
      if (!destinationFolder) {
        return res.status(400).json({ message: "Invalid lockedVideoUrl." });
      }

      // Split destinationFolder to determine parent folder and child folder.
      // For example, if destinationFolder is "AES-videos/platform-testing/",
      // then parentFolder is "AES-videos/" and childFolder is the same.
      const parts = destinationFolder.split('/').filter(Boolean); // e.g., ["AES-videos", "platform-testing"]
      const parentFolder = parts[0] + '/'; // "AES-videos/"
      const childFolder = destinationFolder; // "AES-videos/platform-testing/"
      const contentName = parts[1]; // "platform-testing"
      // Initialize S3 client.
      const s3Client = new S3Client({
        region: awsRegion,
        credentials: { accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey }
      });

      // Ensure parent and child folders exist.
      await ensureFolderExists(s3Client, awsBucketName, parentFolder);
      await ensureFolderExists(s3Client, awsBucketName, childFolder);

      // Process source and get local MP4 path.
      const localMp4Path = await processSourceToLocalMp4(s3Client, awsBucketName, awsOriginalKey);

      // Process the MP4 into HLS playlists.
      const { normalPlaylistPath, blackoutPlaylistPath } = createM3U8WithExactSegments(localMp4Path, blackoutLocksForHLS);

      // Upload HLS files into the child folder.
      const fileUrlMapping = await uploadHlsFilesToS3(s3Client, awsBucketName, childFolder);

      // Update playlists with S3 URLs.
      const updatedNormalPlaylist = updatePlaylistContent(normalPlaylistPath, fileUrlMapping);
      const updatedBlackoutPlaylist = updatePlaylistContent(blackoutPlaylistPath, fileUrlMapping);

      const finalNormalKey = childFolder + 'output.m3u8';
      const finalBlackoutKey = childFolder + 'blackout.m3u8';
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

      // Clean up local files.
      fs.unlinkSync(localMp4Path);
      const hlsFiles = fs.readdirSync(outputDir);
      for (const file of hlsFiles) {
        fs.unlinkSync(path.join(outputDir, file));
      }

      // Create and save the lock record.
      const newLock = new Lock({
        PlatformID: platform._id,
        UserID: user._id,
        OriginalContentUrl: inputVideoUrl,
        LockedContentUrl: blackoutUrl,
        contentId: contentName,
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

    if (storageType === "AWS") {
      const { awsAccessKeyId, awsSecretAccessKey, awsRegion, awsBucketName } = storageMetaData;
      if (!awsAccessKeyId || !awsSecretAccessKey || !awsRegion || !awsBucketName) {
        return res.status(400).json({ message: "Missing required AWS data." });
      }

      // Find the lock document.
      const lock = await Lock.findById(lockId);
      if (!lock) {
        return res.status(404).json({ message: "Lock not found." });
      }
      // Extract original video key.
      const awsOriginalKey = extractS3Key(lock.OriginalContentUrl);
      if (!awsOriginalKey) {
        return res.status(500).json({ message: "Invalid OriginalContentUrl in lock document." });
      }
      const s3Client = new S3Client({
        region: awsRegion,
        credentials: { accessKeyId: awsAccessKeyId, secretAccessKey: awsSecretAccessKey }
      });

      // Process source and get local MP4 path.
      const localMp4Path = await processSourceToLocalMp4(s3Client, awsBucketName, awsOriginalKey);
      console.log(`Local MP4 path: ${localMp4Path}, size: ${fs.statSync(localMp4Path).size} bytes`);

      // Derive folder to delete from LockedContentUrl.
      const lockedUrl = lock.LockedContentUrl;
      if (!lockedUrl) {
        return res.status(400).json({ message: "LockedContentUrl missing in lock document." });
      }
      const folderToDelete = extractS3Folder(lockedUrl);
      if (!folderToDelete) {
        return res.status(400).json({ message: "Invalid LockedContentUrl in lock document." });
      }
      // Delete existing folder content in S3.
      const listParams = { Bucket: awsBucketName, Prefix: folderToDelete };
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
      const fileUrlMapping = await uploadHlsFilesToS3(s3Client, awsBucketName, folderToDelete);
      const updatedNormalPlaylist = updatePlaylistContent(normalPlaylistPath, fileUrlMapping);
      const updatedBlackoutPlaylist = updatePlaylistContent(blackoutPlaylistPath, fileUrlMapping);
      
      const finalNormalKey = folderToDelete + 'output.m3u8';
      const finalBlackoutKey = folderToDelete + 'blackout.m3u8';
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
      
      // Update the lock record with new locks.
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
    // Find the lock document.
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
      
      // Derive folder to delete from LockedContentUrl.
      const lockedUrl = lock.LockedContentUrl;
      if (!lockedUrl) {
        return res.status(400).json({ message: "LockedContentUrl missing in lock document." });
      }
      const folderToDelete = extractS3Folder(lockedUrl);
      if (!folderToDelete) {
        return res.status(400).json({ message: "Invalid LockedContentUrl in lock document." });
      }
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
      await s3Client.send(deleteCommand);
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