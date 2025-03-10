const { S3Client, ListObjectsV2Command, GetObjectCommand, DeleteObjectsCommand } = require("@aws-sdk/client-s3");
const { Upload } = require("@aws-sdk/lib-storage");
const { pipeline } = require("stream");
const { promisify } = require("util");
const streamPipeline = promisify(pipeline);
const path = require('path');
const fs = require('fs');
const { v4: uuidv4 } = require('uuid');

// Import processing functions.
const { downloadFileFromS3, uploadToS3, uploadHlsFilesToS3 } = require('../../canvas-processing/s3Processing');
const { createM3U8WithExactSegments, updatePlaylistContent, outputDir, TMP_DIR } = require('../../canvas-processing/videoProcessing');

// Import Mongoose models.
const Lock = require('../models/Lock');

// GET /get-video-names
async function getVideoNames(req, res) {
  try {
    const { awsData } = req.body;
    const folderPrefix = req.body.folderPrefix || awsData.folderPrefix;
    if (!awsData) {
      return res.status(400).json({ message: "Missing awsData in request body." });
    }
    const { awsAccessKeyId, awsSecretAccessKey, awsRegion, awsBucketName } = awsData;
    if (!awsAccessKeyId || !awsSecretAccessKey || !awsRegion || !awsBucketName) {
      return res.status(400).json({ message: "Invalid or missing AWS data." });
    }
    if (!folderPrefix) {
      return res.status(400).json({ message: "Missing folderPrefix in request body." });
    }
    const prefix = folderPrefix.endsWith('/') ? folderPrefix : folderPrefix + '/';
    const s3Client = new S3Client({
      region: awsRegion,
      credentials: {
        accessKeyId: awsAccessKeyId,
        secretAccessKey: awsSecretAccessKey
      }
    });
    const listParams = {
      Bucket: awsBucketName,
      Delimiter: '/',
      Prefix: prefix
    };
    const command = new ListObjectsV2Command(listParams);
    const data = await s3Client.send(command);
    let folders = [];
    if (data.CommonPrefixes) {
      folders = data.CommonPrefixes.map((p) => p.Prefix);
    }
    return res.status(200).json({ folders });
  } catch (error) {
    console.error("Error in /get-video-names:", error);
    return res.status(500).json({ message: error.message });
  }
}

// GET /get-lock-by-contentid/:contentId
async function getLockByContentId(req, res) {
  try {
    const { contentId } = req.params;
    console.log("Got contentId:", contentId);
    const lock = await Lock.findOne({ contentId: contentId });
    if (!lock) {
      return res.status(404).json({ message: "Lock not found." });
    }
    console.log("Returning lock:");
    console.log(lock);
    return res.status(200).json({ lock });
  } catch (error) {
    console.error("Error in /get-lock-by-contentid:", error);
    return res.status(500).json({ message: error.message });
  }
}

// GET /get-lockjsonobject/:lockId
async function getLockJsonObject(req, res) {
  try {
    const { lockId } = req.params;
    const lock = await Lock.findOne({ _id: lockId });
    if (!lock) {
      return res.status(404).json({ message: "Lock not found." });
    }
    return res.status(200).json({ lockJsonObject: lock.LockJsonObject });
  } catch (error) {
    console.error("Error in /get-lockjsonobject:", error);
    return res.status(500).json({ message: error.message });
  }
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
      return res.status(400).json({ message: "Missing bucketName/originalKey/destinationFolder." });
    }
    if (!contentId) {
      return res.status(400).json({ message: "Missing contentId." });
    }
    
    const s3Client = new S3Client({
      region: awsRegion,
      credentials: {
        accessKeyId: awsAccessKeyId,
        secretAccessKey: awsSecretAccessKey
      }
    });

    // Download original MP4.
    const localMp4Path = path.join(TMP_DIR, `${Date.now()}-original.mp4`);
    await downloadFileFromS3(s3Client, awsBucketName, awsOriginalKey, localMp4Path);

    // Process video into HLS playlists.
    const { normalPlaylistPath, blackoutPlaylistPath } = createM3U8WithExactSegments(localMp4Path, blackoutLocks || []);

    // Create a subfolder for the content.
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

    // Clean up local files.
    fs.unlinkSync(localMp4Path);
    const hlsFiles = fs.readdirSync(outputDir);
    for (const file of hlsFiles) {
      fs.unlinkSync(path.join(outputDir, file));
    }

    // Save record in the database.
    const lockId = uuidv4();
    const lockJsonObject = {
      lockId,
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
    const savedLock = await newLock.save();

    return res.status(201).json({
      message: 'Lock created successfully',
      lock_id: lockJsonObject.lockId,
      normalUrl,
      blackoutUrl
    });
  } catch (error) {
    console.error("Error in /create-AES:", error);
    return res.status(500).json({ message: 'Server error', error: error.message });
  }
}

// POST /download-video
async function downloadVideo(req, res) {
  try {
    const { awsData, folderPrefix } = req.body;
    const s3Client = new S3Client({
      region: awsData.awsRegion,
      credentials: {
        accessKeyId: awsData.awsAccessKeyId,
        secretAccessKey: awsData.awsSecretAccessKey
      }
    });
    // Clean output directory.
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir);
    } else {
      const oldFiles = fs.readdirSync(outputDir);
      for (const file of oldFiles) {
        fs.unlinkSync(path.join(outputDir, file));
      }
    }
    // List objects.
    const listParams = {
      Bucket: awsData.awsBucketName,
      Prefix: folderPrefix
    };
    const listCommand = new ListObjectsV2Command(listParams);
    const data = await s3Client.send(listCommand);
    if (!data.Contents || data.Contents.length === 0) {
      return res.status(404).json({ message: "No files found in that prefix." });
    }
    const s3UrlPrefix = `https://${awsData.awsBucketName}.s3.${awsData.awsRegion}.amazonaws.com/${folderPrefix}`;
    for (const obj of data.Contents) {
      if (obj.Key.endsWith('/')) continue;
      const getObjectParams = { Bucket: awsData.awsBucketName, Key: obj.Key };
      const getObjectCommand = new GetObjectCommand(getObjectParams);
      const fileResponse = await s3Client.send(getObjectCommand);
      const relative = obj.Key.substring(folderPrefix.length);
      const localFilePath = path.join(outputDir, relative);
      await streamPipeline(fileResponse.Body, fs.createWriteStream(localFilePath));
      // If m3u8, adjust the URLs.
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
    return res.json({ message: "Folder downloaded successfully" });
  } catch (error) {
    console.error("Error in /download-video:", error);
    return res.status(500).json({ message: error.message });
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
    // Find the lock document.
    const lock = await Lock.findOne({ _id: lockId });
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
    const getCommand = new GetObjectCommand({ Bucket: awsBucketName, Key: awsOriginalKey });
    const data = await s3Client.send(getCommand);
    const localMp4Path = path.join(TMP_DIR, `${Date.now()}-original.mp4`);
    await streamPipeline(data.Body, fs.createWriteStream(localMp4Path));
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
      credentials: {
        accessKeyId: awsAccessKeyId,
        secretAccessKey: awsSecretAccessKey
      }
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
  getVideoNames,
  getLockByContentId,
  getLockJsonObject,
  createAES,
  downloadVideo,
  modifyAES,
  deleteAES
};
