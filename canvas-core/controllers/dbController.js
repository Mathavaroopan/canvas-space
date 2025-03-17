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
    const { inputVideoUrl } = req.body;
    if (!inputVideoUrl) {
      return res.status(400).json({ message: "Missing inputVideoUrl in request body." });
    }
    const lock = await Lock.findOne({ OriginalContentUrl: inputVideoUrl });
    if (!lock) {
      return res.status(404).json({ message: "Lock not found." });
    }
    return res.status(200).json({ lock_id: lock._id });
  } catch (error) {
    console.error("Error in getLockIdByInputVideoUrl:", error);
    return res.status(500).json({ message: error.message });
  }
}

module.exports = {
  getLockIdByContentId,
  getLockJsonObject,
  getLockIdByInputVideoUrl
};
