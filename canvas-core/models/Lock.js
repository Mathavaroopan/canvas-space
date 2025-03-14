const mongoose = require('mongoose');
const Schema = mongoose.Schema;

// Base schema for a lock item in the locks array
const LockItemSchema = new Schema({
  lock_type: {
    type: String,
    enum: ['replacement-video-lock', 'blackout-lock', 'form-lock'],
    required: true
  },
  starttime: { type: Number, required: true }
}, {
  discriminatorKey: 'lock_type', // Field used for discriminating subdocument types
  _id: false
});

// Main Lock schema (generalized)
const LockSchema = new Schema({
  PlatformID: { type: Schema.Types.ObjectId, ref: 'Platform', required: true },
  UserID: { type: Schema.Types.ObjectId, ref: 'User', required: true },
  OriginalContentUrl: { type: String, required: true },
  LockedContentUrl: { type: String, default: null },
  contentId: { type: String, required: true },
  storage_type: {
    type: String,
    enum: ['AWS', 'GOOGLE_CLOUD', 'AZURE'],
    required: true,
  },
  locks: [LockItemSchema],
  CreatedAt: { type: Date, default: Date.now }
});

// Create discriminators for additional fields based on lock_type:

// For "replacement-video-lock": add endtime and replacement_video_url.
LockSchema.path('locks').discriminator('replacement-video-lock',
  new Schema({
    endtime: { type: Number, required: true },
    replacement_video_url: { type: String, required: true }
  }, { _id: false })
);

// For "blackout-lock": add endtime.
LockSchema.path('locks').discriminator('blackout-lock',
  new Schema({
    endtime: { type: Number, required: true }
  }, { _id: false })
);

// For "form-lock": add endtime and customJson.
LockSchema.path('locks').discriminator('form-lock',
  new Schema({
    endtime: { type: Number, required: true },
    customJson: { type: Schema.Types.Mixed, required: true }
  }, { _id: false })
);

module.exports = mongoose.model('Lock', LockSchema);
