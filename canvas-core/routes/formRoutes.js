const express = require('express');
const router = express.Router();
const Form = require('../models/Form');
const auth = require('../middleware/auth');

// Create a new form
router.post('/forms', auth, async (req, res) => {
  try {
    const { name, elements } = req.body;
    
    if (!name) {
      return res.status(400).json({ message: 'Form name is required' });
    }
    
    if (!elements || !Array.isArray(elements) || elements.length === 0) {
      return res.status(400).json({ message: 'Form must have at least one element' });
    }
    
    const form = new Form({
      name,
      elements,
      userId: req.user.id
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

// Get all forms for the authenticated user
router.get('/forms', auth, async (req, res) => {
  try {
    const forms = await Form.find({ userId: req.user.id }).sort({ updatedAt: -1 });
    res.json({ forms });
  } catch (error) {
    console.error('Error fetching forms:', error);
    res.status(500).json({ message: 'Server error', error: error.message });
  }
});

// Get a specific form by ID
router.get('/forms/:id', auth, async (req, res) => {
  try {
    const form = await Form.findOne({ _id: req.params.id, userId: req.user.id });
    
    if (!form) {
      return res.status(404).json({ message: 'Form not found' });
    }
    
    res.json({ form });
  } catch (error) {
    console.error('Error fetching form:', error);
    res.status(500).json({ message: 'Server error', error: error.message });
  }
});

// Update a form
router.put('/forms/:id', auth, async (req, res) => {
  try {
    const { name, elements } = req.body;
    
    if (!name) {
      return res.status(400).json({ message: 'Form name is required' });
    }
    
    if (!elements || !Array.isArray(elements) || elements.length === 0) {
      return res.status(400).json({ message: 'Form must have at least one element' });
    }
    
    const form = await Form.findOne({ _id: req.params.id, userId: req.user.id });
    
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
router.delete('/forms/:id', auth, async (req, res) => {
  try {
    const form = await Form.findOne({ _id: req.params.id, userId: req.user.id });
    
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