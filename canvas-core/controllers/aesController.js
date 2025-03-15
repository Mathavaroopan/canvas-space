const {
  S3Client,
  ListObjectsV2Command,
  DeleteObjectsCommand,
} = require("@aws-sdk/client-s3");
const path = require("path");
const fs = require("fs");

// Import processing functions.
const {
  uploadToS3,
  uploadHlsFilesToS3,
} = require("../../canvas-processing/s3Processing");

const {
  createM3U8WithExactSegments,
  updatePlaylistContent,
  outputDir,
  TMP_DIR,
} = require("../../canvas-processing/videoProcessing");

// Import Mongoose model.
const Lock = require("../models/Lock");

const { processSourceToLocalMp4 } = require("awsController");
// POST /create-AES
async function createAES(req, res) {
  try {
    const { storage_type, MetaData, platformId, userId, contentId, locks } =
      req.body || {};

    if (!MetaData) {
      return res
        .status(400)
        .json({ message: "Missing MetaData in request body." });
    }
    if (!contentId) {
      return res.status(400).json({ message: "Missing contentId." });
    }

  
    const {
      awsAccessKeyId,
      awsSecretAccessKey,
      awsRegion,
      awsBucketName,
      awsOriginalKey,
      awsDestinationFolder,
    } = MetaData;
    if (!awsAccessKeyId || !awsSecretAccessKey) {
      return res.status(400).json({ message: "Missing AWS credentials." });
    }
    if (!awsBucketName || !awsOriginalKey || !awsDestinationFolder) {
      return res
        .status(400)
        .json({
          message:
            "Missing awsBucketName, awsOriginalKey and/or awsDestinationFolder.",
        });
    }

    const s3Client = new S3Client({
      region: awsRegion,
      credentials: {
        accessKeyId: awsAccessKeyId,
        secretAccessKey: awsSecretAccessKey,
      },
    });

    // Process source and get local MP4 path.
    const localMp4Path = await processSourceToLocalMp4(
      s3Client,
      awsBucketName,
      awsOriginalKey
    );

    // Process the MP4 into HLS playlists using the blackout locks.
    const { normalPlaylistPath, blackoutPlaylistPath } =
      createM3U8WithExactSegments(localMp4Path, blackoutLocksForHLS);

    // Create a subfolder for the content in S3.
    const baseFolder = awsDestinationFolder.endsWith("/")
      ? awsDestinationFolder
      : awsDestinationFolder + "/";
    const uniqueSubfolder = baseFolder + contentId + "/";

    // Upload HLS files to S3.
    const fileUrlMapping = await uploadHlsFilesToS3(
      s3Client,
      awsBucketName,
      uniqueSubfolder
    );

    // Update playlists with S3 URLs.
    const updatedNormalPlaylist = updatePlaylistContent(
      normalPlaylistPath,
      fileUrlMapping
    );
    const updatedBlackoutPlaylist = updatePlaylistContent(
      blackoutPlaylistPath,
      fileUrlMapping
    );

    const finalNormalKey = uniqueSubfolder + "output.m3u8";
    const finalBlackoutKey = uniqueSubfolder + "blackout.m3u8";
    const normalUrl = await uploadToS3(
      s3Client,
      Buffer.from(updatedNormalPlaylist, "utf8"),
      awsBucketName,
      finalNormalKey,
      "application/vnd.apple.mpegurl"
    );
    const blackoutUrl = await uploadToS3(
      s3Client,
      Buffer.from(updatedBlackoutPlaylist, "utf8"),
      awsBucketName,
      finalBlackoutKey,
      "application/vnd.apple.mpegurl"
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
      locks: dbLocks,
    });
    await newLock.save();

    return res.status(201).json({
      message: "Lock created successfully",
      lock_id: newLock._id,
    });
  } catch (error) {
    console.error("Error in /create-AES:", error);
    return res
      .status(500)
      .json({ message: "Server error", error: error.message });
  }
}

// POST /modify-AES
async function modifyAES(req, res) {
  try {
    const { storage_type, MetaData, lockId, newLocks, folder } = req.body;
    if (!MetaData || !lockId || !newLocks || !folder) {
      return res.status(400).json({ message: "Missing required fields." });
    }

    // Find the lock document by its _id.
    const lock = await Lock.findById(lockId);
    if (!lock) {
      return res.status(404).json({ message: "Lock not found." });
    }

    // Precompute new lock lists.
    const allNewLocks = newLocks || [];
    const blackoutLocksForHLS = allNewLocks
      .filter((lock) => lock.lock_type === "blackout-lock")
      .map((lock) => ({
        startTime: Number(lock.startTime),
        endTime: Number(lock.endTime),
      }));
    const dbNewLocks = allNewLocks.map((lock) => {
      const base = {
        lock_type: lock.lock_type,
        starttime: Number(lock.startTime),
        endtime: Number(lock.endTime),
      };
      if (lock.lock_type === "form-lock") {
        base.customJson = lock.customJson;
      } else if (lock.lock_type === "replacement-video-lock") {
        base.replacement_video_url = lock.replacement_video_url;
      }
      return base;
    });

    if (storage_type === "AWS") {
      const { awsAccessKeyId, awsSecretAccessKey, awsRegion, awsBucketName } =
        MetaData;
      if (
        !awsAccessKeyId ||
        !awsSecretAccessKey ||
        !awsRegion ||
        !awsBucketName
      ) {
        return res.status(400).json({ message: "Missing required AWS data." });
      }

      // Extract original video key from OriginalContentUrl.
      const originalUrl = lock.OriginalContentUrl;
      const urlParts = originalUrl.split(".amazonaws.com/");
      if (urlParts.length < 2) {
        return res
          .status(500)
          .json({ message: "Invalid original content URL." });
      }
      const awsOriginalKey = urlParts[1];
      const s3Client = new S3Client({
        region: awsRegion,
        credentials: {
          accessKeyId: awsAccessKeyId,
          secretAccessKey: awsSecretAccessKey,
        },
      });

      // Process source and get local MP4 path.
      const localMp4Path = await processSourceToLocalMp4(
        s3Client,
        awsBucketName,
        awsOriginalKey
      );
      console.log(
        `Local MP4 path: ${localMp4Path}, size: ${
          fs.statSync(localMp4Path).size
        } bytes`
      );

      // Delete existing folder content in S3.
      const listParams = { Bucket: awsBucketName, Prefix: folder };
      const listCommand = new ListObjectsV2Command(listParams);
      const listData = await s3Client.send(listCommand);
      if (listData.Contents && listData.Contents.length > 0) {
        const objectsToDelete = listData.Contents.map((obj) => ({
          Key: obj.Key,
        }));
        const deleteParams = {
          Bucket: awsBucketName,
          Delete: { Objects: objectsToDelete, Quiet: false },
        };
        const deleteCommand = new DeleteObjectsCommand(deleteParams);
        await s3Client.send(deleteCommand);
      }

      // Process HLS files.
      const { normalPlaylistPath, blackoutPlaylistPath } =
        createM3U8WithExactSegments(localMp4Path, blackoutLocksForHLS);
      const fileUrlMapping = await uploadHlsFilesToS3(
        s3Client,
        awsBucketName,
        folder
      );
      const updatedNormalPlaylist = updatePlaylistContent(
        normalPlaylistPath,
        fileUrlMapping
      );
      const updatedBlackoutPlaylist = updatePlaylistContent(
        blackoutPlaylistPath,
        fileUrlMapping
      );

      const finalNormalKey = folder + "output.m3u8";
      const finalBlackoutKey = folder + "blackout.m3u8";
      const normalUrl = await uploadToS3(
        s3Client,
        Buffer.from(updatedNormalPlaylist, "utf8"),
        awsBucketName,
        finalNormalKey,
        "application/vnd.apple.mpegurl"
      );
      const blackoutUrl = await uploadToS3(
        s3Client,
        Buffer.from(updatedBlackoutPlaylist, "utf8"),
        awsBucketName,
        finalBlackoutKey,
        "application/vnd.apple.mpegurl"
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
    }

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
      return res
        .status(400)
        .json({ message: "Missing MetaData or lockId in request body." });
    }
    // Find the lock document by its _id.
    const lock = await Lock.findById(lockId);
    if (!lock) {
      return res.status(404).json({ message: "Lock not found." });
    }
    const contentId = lock.contentId;
    if (!contentId) {
      return res
        .status(400)
        .json({ message: "Content ID not found in lock document." });
    }

    if (storage_type === "AWS") {
      const { awsAccessKeyId, awsSecretAccessKey, awsRegion, awsBucketName } =
        MetaData;
      if (
        !awsAccessKeyId ||
        !awsSecretAccessKey ||
        !awsRegion ||
        !awsBucketName ||
        !folderPrefix
      ) {
        return res.status(400).json({ message: "Missing required AWS data." });
      }
      const normalizedPrefix = folderPrefix.endsWith("/")
        ? folderPrefix
        : folderPrefix + "/";
      const folderToDelete = normalizedPrefix + contentId + "/";
      const s3Client = new S3Client({
        region: awsRegion,
        credentials: {
          accessKeyId: awsAccessKeyId,
          secretAccessKey: awsSecretAccessKey,
        },
      });
      const listParams = { Bucket: awsBucketName, Prefix: folderToDelete };
      const listCommand = new ListObjectsV2Command(listParams);
      const listData = await s3Client.send(listCommand);
      if (!listData.Contents || listData.Contents.length === 0) {
        return res
          .status(404)
          .json({ message: "No objects found in the specified folder." });
      }
      const objectsToDelete = listData.Contents.map((obj) => ({
        Key: obj.Key,
      }));
      const deleteParams = {
        Bucket: awsBucketName,
        Delete: { Objects: objectsToDelete, Quiet: false },
      };
      const deleteCommand = new DeleteObjectsCommand(deleteParams);
      const deleteResult = await s3Client.send(deleteCommand);
      return res
        .status(200)
        .json({ message: "Folder deleted successfully", lockId });
    } else res.send("Invalid storage type");
  } catch (error) {
    console.error("Error in /delete-AES:", error);
    return res.status(500).json({ message: error.message });
  }
}

module.exports = {
  createAES,
  modifyAES,
  deleteAES,
};
