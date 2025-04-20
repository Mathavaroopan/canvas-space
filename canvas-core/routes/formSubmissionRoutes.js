const express = require('express');
const router = express.Router();
const FormSubmission = require('../models/FormSubmission');
const Lock = require('../models/Lock');
const mongoose = require('mongoose');

// Submit form data from BradmaxPlayer
router.post('/form-submissions', async (req, res) => {
  try {
    const { 
      formId, 
      lockId, 
      contentId, 
      videoUrl, 
      segmentStartTime, 
      segmentEndTime, 
      formData 
    } = req.body;
    console.log(req.body);
    // Basic validation
    if (!lockId || !contentId || !videoUrl || segmentStartTime === undefined || segmentEndTime === undefined) {
      return res.status(400).json({ 
        success: false,
        message: 'Missing required fields' 
      });
    }
    
    if (!formData || !Array.isArray(formData) || formData.length === 0) {
      return res.status(400).json({ 
        success: false,
        message: 'Form data is required' 
      });
    }
    
    // Validate lockId is a valid ObjectId
    if (!mongoose.Types.ObjectId.isValid(lockId)) {
      return res.status(400).json({ 
        success: false,
        message: 'Invalid lock ID format' 
      });
    }
    
    // Check if lockId exists
    const lock = await Lock.findById(lockId);
    if (!lock) {
      return res.status(404).json({ 
        success: false,
        message: 'Lock not found' 
      });
    }
    
    // Create form submission
    const submission = new FormSubmission({
      formId: formId || null,
      lockId,
      contentId,
      videoUrl,
      segmentStartTime,
      segmentEndTime,
      formData,
      ipAddress: req.ip,
      userAgent: req.headers['user-agent'] || null
    });
    
    await submission.save();
    
    res.status(201).json({ 
      success: true,
      message: 'Form submission saved successfully',
      submissionId: submission._id
    });
  } catch (error) {
    console.error('Error saving form submission:', error);
    res.status(500).json({ 
      success: false,
      message: 'Server error', 
      error: error.message 
    });
  }
});

// Get all form submissions
router.get('/form-submissions', async (req, res) => {
  try {
    const { lockId, formId, contentId, startDate, endDate, limit = 50, page = 1 } = req.query;
    
    // Build query object
    const query = {};
    
    if (lockId) {
      if (!mongoose.Types.ObjectId.isValid(lockId)) {
        return res.status(400).json({ 
          success: false,
          message: 'Invalid lock ID format' 
        });
      }
      query.lockId = lockId;
    }
    
    if (formId) {
      if (!mongoose.Types.ObjectId.isValid(formId)) {
        return res.status(400).json({ 
          success: false,
          message: 'Invalid form ID format' 
        });
      }
      query.formId = formId;
    }
    
    if (contentId) {
      query.contentId = contentId;
    }
    
    // Date range filter
    if (startDate || endDate) {
      query.createdAt = {};
      
      if (startDate) {
        query.createdAt.$gte = new Date(startDate);
      }
      
      if (endDate) {
        query.createdAt.$lte = new Date(endDate);
      }
    }
    
    // Calculate pagination
    const skip = (parseInt(page) - 1) * parseInt(limit);
    const limitNum = parseInt(limit);
    
    // Get total count for pagination info
    const total = await FormSubmission.countDocuments(query);
    
    // Get paginated results
    const submissions = await FormSubmission.find(query)
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limitNum);
    
    // Send response with pagination info
    res.json({
      success: true,
      submissions,
      pagination: {
        total,
        page: parseInt(page),
        limit: limitNum,
        pages: Math.ceil(total / limitNum)
      }
    });
  } catch (error) {
    console.error('Error fetching form submissions:', error);
    res.status(500).json({ 
      success: false,
      message: 'Server error', 
      error: error.message 
    });
  }
});

// Get a specific form submission by ID
router.get('/form-submissions/:id', async (req, res) => {
  try {
    const submissionId = req.params.id;
    
    // Validate ID format
    if (!mongoose.Types.ObjectId.isValid(submissionId)) {
      return res.status(400).json({ 
        success: false,
        message: 'Invalid submission ID format' 
      });
    }
    
    const submission = await FormSubmission.findById(submissionId);
    
    if (!submission) {
      return res.status(404).json({ 
        success: false,
        message: 'Form submission not found' 
      });
    }
    
    res.json({ 
      success: true,
      submission 
    });
  } catch (error) {
    console.error('Error fetching form submission:', error);
    res.status(500).json({ 
      success: false,
      message: 'Server error', 
      error: error.message 
    });
  }
});

// Export form submissions as CSV
router.get('/form-submissions/export/csv', async (req, res) => {
  try {
    const { lockId, formId, contentId, startDate, endDate } = req.query;
    
    // Build query object
    const query = {};
    
    if (lockId) {
      if (!mongoose.Types.ObjectId.isValid(lockId)) {
        return res.status(400).json({ 
          success: false,
          message: 'Invalid lock ID format' 
        });
      }
      query.lockId = lockId;
    }
    
    if (formId) {
      if (!mongoose.Types.ObjectId.isValid(formId)) {
        return res.status(400).json({ 
          success: false,
          message: 'Invalid form ID format' 
        });
      }
      query.formId = formId;
    }
    
    if (contentId) {
      query.contentId = contentId;
    }
    
    // Date range filter
    if (startDate || endDate) {
      query.createdAt = {};
      
      if (startDate) {
        query.createdAt.$gte = new Date(startDate);
      }
      
      if (endDate) {
        query.createdAt.$lte = new Date(endDate);
      }
    }
    
    // Get all submissions matching the query
    const submissions = await FormSubmission.find(query).sort({ createdAt: -1 });
    
    if (submissions.length === 0) {
      return res.status(404).json({ 
        success: false,
        message: 'No form submissions found' 
      });
    }
    
    // Create CSV header
    let csvContent = 'Submission ID,Content ID,Video URL,Segment Start Time,Segment End Time,Form ID,Created At,IP Address';
    
    // Add dynamic form field headers
    // Find all unique form field labels across all submissions
    const allFieldLabels = new Set();
    submissions.forEach(submission => {
      submission.formData.forEach(field => {
        allFieldLabels.add(field.label);
      });
    });
    
    // Add field labels to header
    allFieldLabels.forEach(label => {
      csvContent += `,${label}`;
    });
    
    csvContent += '\n';
    
    // Add data rows
    submissions.forEach(submission => {
      // Convert createdAt to ISO string
      const createdAt = submission.createdAt.toISOString();
      
      // Start with fixed fields
      let row = `${submission._id},${submission.contentId},${submission.videoUrl},${submission.segmentStartTime},${submission.segmentEndTime},${submission.formId || 'N/A'},${createdAt},${submission.ipAddress || 'N/A'}`;
      
      // Add dynamic form fields
      allFieldLabels.forEach(label => {
        const field = submission.formData.find(f => f.label === label);
        row += `,${field ? field.value.replace(/,/g, ';') : ''}`; // Replace commas in values to avoid CSV issues
      });
      
      csvContent += row + '\n';
    });
    
    // Set response headers
    res.setHeader('Content-Type', 'text/csv');
    res.setHeader('Content-Disposition', `attachment; filename=form-submissions-${new Date().toISOString().split('T')[0]}.csv`);
    
    // Send CSV response
    res.send(csvContent);
    
  } catch (error) {
    console.error('Error exporting form submissions:', error);
    res.status(500).json({ 
      success: false,
      message: 'Server error', 
      error: error.message 
    });
  }
});

module.exports = router; 