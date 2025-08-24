// src/services/api.js
import axios from 'axios';


const API_KEY = process.env.REACT_APP_API_KEY;


const API_URL = (process.env.REACT_APP_RAG_API_ENDPOINT || '').replace(/\/+$/,'');
console.log('Initializing API with URL:', API_URL);

const api = axios.create({
  baseURL: API_URL,
  headers: { 'Content-Type': 'application/json', 'x-api-key': process.env.REACT_APP_API_KEY }
});

export const sendMessage = async (message, sessionId) => {
  try {
    console.log('Sending request to:', `${API_URL}/chat-response`);
    console.log('Request payload:', {
      query: message,
      session_id: sessionId
    });
    console.log('Headers:', {
      'Content-Type': 'application/json',
      'x-api-key': API_KEY
    });

    // Make the API call directly without using the axios instance
    const response = await axios({
      method: 'post',
      url: `${API_URL}/chat-response`,
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': API_KEY
      },
      data: {
        query: message,
        session_id: sessionId
      }
    });

    console.log('API Response:', response.data);
    return response.data;
  } catch (error) {
    console.error('API Error:', {
      message: error.message,
      response: error.response?.data,
      status: error.response?.status,
      headers: error.response?.headers
    });
    throw error;
  }
};

export const sendFeedback = async (sessionId, timestamp, rating, feedbackText = '') => {
  try {
    console.log('Sending feedback to:', `${API_URL}/feedback`);
    console.log('Feedback payload:', {
      session_id: sessionId,
      timestamp: timestamp,
      rating: rating,
      feedback_text: feedbackText
    });

    const response = await axios({
      method: 'post',
      url: `${API_URL}/feedback`,
      headers: {
        'Content-Type': 'application/json',
        'x-api-key': API_KEY
      },
      data: {
        session_id: sessionId,
        timestamp: timestamp,
        rating: rating,
        feedback_text: feedbackText
      }
    });

    console.log('Feedback Response:', response.data);
    return response.data;
  } catch (error) {
    console.error('Feedback API Error:', {
      message: error.message,
      response: error.response?.data,
      status: error.response?.status,
      headers: error.response?.headers
    });
    throw error;
  }
};

