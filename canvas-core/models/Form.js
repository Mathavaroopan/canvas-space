const mongoose = require('mongoose');

const formElementSchema = new mongoose.Schema({
  type: { 
    type: String, 
    required: true,
    enum: ['text', 'email', 'select', 'textarea']
  },
  label: { 
    type: String, 
    required: true 
  },
  placeholder: { 
    type: String, 
    default: '' 
  },
  required: { 
    type: Boolean, 
    default: false 
  },
  options: [{ 
    type: String 
  }]
}, { _id: false });

const formSchema = new mongoose.Schema({
  name: {
    type: String,
    required: true
  },
  elements: [formElementSchema],
  userId: {
    type: mongoose.Schema.Types.ObjectId,
    ref: 'User',
    required: true
  },
  createdAt: {
    type: Date,
    default: Date.now
  },
  updatedAt: {
    type: Date,
    default: Date.now
  }
});

formSchema.pre('save', function(next) {
  this.updatedAt = Date.now();
  next();
});

module.exports = mongoose.model('Form', formSchema); 