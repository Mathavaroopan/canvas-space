const Lock = require('../models/Lock');

async function getLockIdByContentId(req, res) {
  try {
    const { contentId } = req.params;
    const lock = await Lock.findOne({ contentId });
    if (!lock) {
      return res.status(404).json({ message: "Lock not found." });
    }
    return res.status(200).json({ lock_id: lock._id });
  } catch (error) {
    console.error("Error in getLockIdByContentId:", error);
    return res.status(500).json({ message: error.message });
  }
}

async function getLockJsonObject(req, res) {
  try {
    const { lockId } = req.params;
    const lock = await Lock.findById(lockId);
    if (!lock) {
      return res.status(404).json({ message: "Lock not found." });
    }
    const result = {
      lock_id: lockId,
      lockJsonObject: {
        originalContentUrl: lock.OriginalContentUrl,
        lockedContentUrl: lock.LockedContentUrl,
        contentId: lock.contentId,
        platformName: lock.PlatformName,
        userName: lock.UserName,
        locks: lock.locks
      }
    };
    return res.status(200).json({ result });
  } catch (error) {
    console.error("Error in getLockJsonObject:", error);
    return res.status(500).json({ message: error.message });
  }
}

async function getLockIdByInputVideoUrl(req, res) {
  try {
    const { inputVideoUrl } = req.params;
    console.log(inputVideoUrl);
    const lock = await Lock.findOne({ OriginalContentUrl: inputVideoUrl });
    if (!lock) {
      return res.status(404).json({ message: "Lock not found." });
    }
    const result = {
      lock_id: lock._id,
      lockJsonObject: {
        originalContentUrl: lock.OriginalContentUrl,
        lockedContentUrl: lock.LockedContentUrl,
        contentId: lock.contentId,
        platformName: lock.PlatformName,
        userName: lock.UserName,
        locks: lock.locks
      }
    };
    return res.status(200).json({ result });
  } catch (error) {
    console.error("Error in getLockIdByInputVideoUrl:", error);
    return res.status(500).json({ message: error.message });
  }
}

async function getAllVideos(req, res) {
  try {
    const locks = await Lock.find({});
    if (!locks || locks.length === 0) {
      return res.status(200).json({ 
        success: true, 
        videos: [],
        message: "No videos found."
      });
    }

    const videos = locks.map(lock => {
      // Extract the videoName from the lockedVideoUrl
      // Example: if URL is "https://canvasapitest.s3.us-east-1.amazonaws.com/AES-videos/ui-testing/blackout.m3u8"
      // Then videoName should be "ui-testing"
      let videoName = "";
      if (lock.LockedContentUrl) {
        const urlParts = lock.LockedContentUrl.split('/');
        // The parent folder of m3u8 file should be the videoName
        if (urlParts.length >= 2) {
          videoName = urlParts[urlParts.length - 2];
        }
      }

      return {
        videoName,
        lockedVideoUrl: lock.LockedContentUrl,
        originalUrl: lock.OriginalContentUrl,
        contentId: lock.contentId
      };
    });

    return res.status(200).json({ 
      success: true, 
      videos, 
      count: videos.length 
    });
  } catch (error) {
    console.error("Error in getAllVideos:", error);
    return res.status(500).json({ 
      success: false, 
      message: error.message 
    });
  }
}

module.exports = {
  getLockIdByContentId,
  getLockJsonObject,
  getLockIdByInputVideoUrl,
  getAllVideos
};
