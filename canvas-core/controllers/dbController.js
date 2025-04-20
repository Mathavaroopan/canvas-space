const Lock = require('../models/Lock');
const Form = require('../models/Form');
const mongoose = require('mongoose');

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
    console.log(`Getting lock JSON object for lockId: ${lockId}`);
    
    // Find the lock with populated form data for each lock with formId
    const lock = await Lock.findById(lockId);
    
    if (!lock) {
      console.log(`Lock not found for ID: ${lockId}`);
      return res.status(404).json({ message: "Lock not found." });
    }
    
    console.log(`Found lock with ${lock.locks.length} segments`);
    
    // Process each lock to include form data
    const processedLocks = await Promise.all(lock.locks.map(async (lockItem, index) => {
      const lockData = lockItem.toObject();
      console.log(`Processing lock segment ${index}:`, lockData);
      
      // If the lock has a formId, get the form name
      if (lockData.formId) {
        console.log(`Lock segment ${index} has formId: ${lockData.formId}`);
        try {
          const form = await Form.findById(lockData.formId);
          if (form) {
            console.log(`Found form for segment ${index}: ${form.name}`);
            lockData.formName = form.name;
          } else {
            console.log(`Form not found for ID: ${lockData.formId}`);
          }
        } catch (err) {
          console.error(`Error fetching form for formId ${lockData.formId}:`, err);
        }
      } else {
        console.log(`Lock segment ${index} has no formId`);
      }
      
      return {
        starttime: lockData.starttime,
        endtime: lockData.endtime,
        lock_type: lockData.lock_type,
        formId: lockData.formId || null,
        formName: lockData.formName || null
      };
    }));
    
    const result = {
      lock_id: lockId,
      lockJsonObject: {
        originalContentUrl: lock.OriginalContentUrl,
        lockedContentUrl: lock.LockedContentUrl,
        contentId: lock.contentId,
        platformName: lock.PlatformName,
        userName: lock.UserName,
        locks: processedLocks
      }
    };
    
    console.log(`Returning result with ${processedLocks.length} processed locks`);
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
    
    // Process each lock to include form data
    const processedLocks = await Promise.all(lock.locks.map(async (lockItem) => {
      const lockData = lockItem.toObject();
      
      // If the lock has a formId, get the form name
      if (lockData.formId) {
        try {
          const form = await Form.findById(lockData.formId);
          if (form) {
            lockData.formName = form.name;
          }
        } catch (err) {
          console.error(`Error fetching form for formId ${lockData.formId}:`, err);
        }
      }
      
      return {
        starttime: lockData.starttime,
        endtime: lockData.endtime,
        lock_type: lockData.lock_type,
        formId: lockData.formId || null,
        formName: lockData.formName || null
      };
    }));
    
    const result = {
      lock_id: lock._id,
      lockJsonObject: {
        originalContentUrl: lock.OriginalContentUrl,
        lockedContentUrl: lock.LockedContentUrl,
        contentId: lock.contentId,
        platformName: lock.PlatformName,
        userName: lock.UserName,
        locks: processedLocks
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
