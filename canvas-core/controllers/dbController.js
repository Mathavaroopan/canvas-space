// canvas-core/controllers/dbController.js
const Lock = require('../models/Lock');

async function getLockByContentId(req, res) {
  try {
    const { contentId } = req.params;
    const lock = await Lock.findOne({ contentId });
    if (!lock) {
      return res.status(404).json({ message: "Lock not found." });
    }
    return res.status(200).json(lock);
  } catch (error) {
    console.error("Error in getLockByContentId:", error);
    return res.status(500).json({ message: error.message });
  }
}

async function getLockJsonObject(req, res) {
  try {
    const { lockId } = req.params;
    const lock = await Lock.findOne({ _id: lockId });
    if (!lock) {
      return res.status(404).json({ message: "Lock not found." });
    }
    // Form a temporary lockJsonObject.
    const result = {
        lock_id: lockId,
        lockJsonObject : {
        originalcontenturl: lock.OriginalContentUrl,
        lockedcontenturl: lock.LockedContentUrl,
        contentid: lock.contentId,
        locks: lock.locks
      }
    }
    return res.status(200).json({ result });
  } catch (error) {
    console.error("Error in getLockJsonObject:", error);
    return res.status(500).json({ message: error.message });
  }
}

module.exports = {
  getLockByContentId,
  getLockJsonObject
};
