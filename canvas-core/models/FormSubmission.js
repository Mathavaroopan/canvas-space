const mongoose = require('mongoose');
const Schema = mongoose.Schema;

// Schema for a form field submission
const FormFieldSchema = new Schema({
  label: { 
    type: String, 
    required: true 
  },
  value: { 
    type: String, 
    required: true 
  }
}, { _id: false });

// Main FormSubmission schema
const FormSubmissionSchema = new Schema({
  formId: { 
    type: Schema.Types.ObjectId, 
    ref: 'Form',
    default: null
  },
  lockId: {
    type: Schema.Types.ObjectId,
    ref: 'Lock',
    required: true
  },
  contentId: {
    type: String,
    required: true
  },
  videoUrl: {
    type: String,
    required: true
  },
  segmentStartTime: {
    type: Number,
    required: true
  },
  segmentEndTime: {
    type: Number,
    required: true
  },
  formData: [FormFieldSchema],
  ipAddress: {
    type: String,
    default: null
  },
  userAgent: {
    type: String,
    default: null
  },
  createdAt: {
    type: Date,
    default: Date.now
  }
});

// Create indexes for efficient querying
FormSubmissionSchema.index({ lockId: 1 });
FormSubmissionSchema.index({ formId: 1 });
FormSubmissionSchema.index({ contentId: 1 });
FormSubmissionSchema.index({ createdAt: 1 });

module.exports = mongoose.model('FormSubmission', FormSubmissionSchema); 