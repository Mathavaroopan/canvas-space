const express = require('express');
const router = express.Router();
const Form = require('../models/Form');
const mongoose = require('mongoose');

// Create a new form
router.post('/forms', async (req, res) => {
  try {
    const { name, elements, userId } = req.body;
    
    if (!name) {
      return res.status(400).json({ message: 'Form name is required' });
    }
    
    if (!elements || !Array.isArray(elements) || elements.length === 0) {
      return res.status(400).json({ message: 'Form must have at least one element' });
    }
    
    const form = new Form({
      name,
      elements,
      userId: userId || '000000000000000000000000' // Default user ID if not provided
    });
    
    await form.save();
    
    res.status(201).json({ 
      message: 'Form created successfully',
      form
    });
  } catch (error) {
    console.error('Error creating form:', error);
    res.status(500).json({ message: 'Server error', error: error.message });
  }
});

// Get all forms
router.get('/forms', async (req, res) => {
  try {
    const { userId } = req.query;
    let query = {};
    
    // If userId is provided, filter by it, otherwise get all forms
    if (userId) {
      query.userId = userId;
    }
    
    const forms = await Form.find(query).sort({ updatedAt: -1 });
    res.json({ forms });
  } catch (error) {
    console.error('Error fetching forms:', error);
    res.status(500).json({ message: 'Server error', error: error.message });
  }
});

// Get a specific form by ID
router.get('/forms/:id', async (req, res) => {
  try {
    const formId = req.params.id;
    console.log(`Looking up form with ID: ${formId}`);
    
    // Check if the ID is a valid mongoose ObjectId
    if (!mongoose.Types.ObjectId.isValid(formId)) {
      console.log(`Invalid form ID format: ${formId}`);
      return res.status(400).json({ message: 'Invalid form ID format' });
    }
    
    const form = await Form.findById(formId);
    console.log(`Form lookup result:`, form ? 'Found' : 'Not found');
    
    if (!form) {
      return res.status(404).json({ message: 'Form not found' });
    }
    
    console.log(`Returning form: ${form.name} with ${form.elements.length} elements`);
    res.json({ form });
  } catch (error) {
    console.error('Error fetching form:', error);
    res.status(500).json({ message: 'Server error', error: error.message, stack: error.stack });
  }
});

// Update a form
router.put('/forms/:id', async (req, res) => {
  try {
    const { name, elements } = req.body;
    
    if (!name) {
      return res.status(400).json({ message: 'Form name is required' });
    }
    
    if (!elements || !Array.isArray(elements) || elements.length === 0) {
      return res.status(400).json({ message: 'Form must have at least one element' });
    }
    
    const form = await Form.findById(req.params.id);
    
    if (!form) {
      return res.status(404).json({ message: 'Form not found' });
    }
    
    form.name = name;
    form.elements = elements;
    form.updatedAt = Date.now();
    
    await form.save();
    
    res.json({ 
      message: 'Form updated successfully',
      form
    });
  } catch (error) {
    console.error('Error updating form:', error);
    res.status(500).json({ message: 'Server error', error: error.message });
  }
});

// Delete a form
router.delete('/forms/:id', async (req, res) => {
  try {
    const form = await Form.findById(req.params.id);
    
    if (!form) {
      return res.status(404).json({ message: 'Form not found' });
    }
    
    await Form.deleteOne({ _id: req.params.id });
    
    res.json({ message: 'Form deleted successfully' });
  } catch (error) {
    console.error('Error deleting form:', error);
    res.status(500).json({ message: 'Server error', error: error.message });
  }
});

module.exports = router; 