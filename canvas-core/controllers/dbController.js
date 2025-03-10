// canvas-core/controllers/dbController.js
const Lock = require('../models/Lock');

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

module.exports = {
  getLockByContentId,
  getLockJsonObject
};
