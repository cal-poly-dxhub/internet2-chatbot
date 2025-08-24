// src/App.jsx
import React, { useState } from 'react';
import './App.css';
import { sendMessage, sendFeedback } from './services/api';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

// Collect only meeting/video-like links for the footer list.
// Inline links remain untouched and clickable in the Markdown.
function collectMeetingLinks(md) {
  const linkRegex = /\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g;
  const meetings = [];
  const seen = new Set();

  const looksLikeVideo = (text, url) => {
    const textHit = /(recording|town\s*hall|recap|meeting|session|forum|research support|cf20|cloud forum|techex)/i.test(text);
    const urlHit  = /(youtube\.com|youtu\.be|vimeo\.com|drive\.google\.com\/file)/i.test(url);
    return textHit || urlHit;
  };

  let m;
  while ((m = linkRegex.exec(md)) !== null) {
    const text = m[1];
    const url  = m[2];
    if (looksLikeVideo(text, url) && !seen.has(url)) {
      seen.add(url);
      meetings.push({ text, url });
    }
  }
  return meetings;
}

function App() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [sessionId] = useState(() => `session_${Date.now()}`);
  const [feedbackSent, setFeedbackSent] = useState(new Set());
  const [feedbackRatings, setFeedbackRatings] = useState({});
  const [feedbackTexts, setFeedbackTexts] = useState({});
  const [feedbackError, setFeedbackError] = useState(null);

  const suggested = [
    'What workloads can I run on AWS?',
    'What workloads can I run on GCP?',
    'What did Lee Pang say about Amazon Omics?',
    'What is AWS Omics?',
    'How do I convince my leadership of the importance of FinOps practices?',
    'Who has a Cloud Center of Excellence?',
    'How are people doing account provisioning?',
    "I've got a consultant coming in to install Control Tower for us...",
    'Do I have to set up a cloud networking architecture for each platform...'
  ];

  const handleFeedback = async (timestamp, rating, feedbackText = '') => {
    try {
      // Call the actual API to send feedback
      await sendFeedback(sessionId, timestamp, rating, feedbackText);
      
      // Update local state to show feedback was sent
      const thumbKey = `${timestamp}_thumb`;
      const textKey = `${timestamp}_text`;
      
      setFeedbackSent(prev => new Set([...prev, thumbKey]));
      if (feedbackText) {
        setFeedbackSent(prev => new Set([...prev, textKey]));
      }
      
      if (rating === 'thumbs_up' || rating === 'thumbs_down') {
        setFeedbackRatings(prev => ({ ...prev, [timestamp]: rating }));
      }
      
      // You can add a success message here if needed
    } catch (error) {
      console.error('Error sending feedback:', error);
      setFeedbackError(`Failed to send feedback: ${error.message}`);
      // Clear error after 5 seconds
      setTimeout(() => setFeedbackError(null), 5000);
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;

    try {
      setIsLoading(true);

      const userMessage = { role: 'user', content: input };
      setMessages(prev => [...prev, userMessage]);
      setInput('');

      const response = await sendMessage(input, sessionId);

      const botMessage = {
        role: 'assistant',
        content: response.response || response.message || 'No response content',
        timestamp: response.timestamp || Date.now()
      };
      setMessages(prev => [...prev, botMessage]);

    } catch (error) {
      const errorMessage = {
        role: 'assistant',
        content: `Error: ${error.message}. Please try again.`,
        isError: true
      };
      setMessages(prev => [...prev, errorMessage]);
    } finally {
      setIsLoading(false);
    }
  };

  const pasteSuggestion = (q) => {
    setInput(q);
  };

  const renderFeedbackButtons = (message, index) => {
    if (message.role !== 'assistant' || !message.timestamp) return null;

    const timestamp = message.timestamp;
    const thumbKey = `${timestamp}_thumb`;
    const textKey = `${timestamp}_text`;
    const hasThumbFeedback = feedbackSent.has(thumbKey);
    const hasTextFeedback = feedbackSent.has(textKey);
    const rating = feedbackRatings[timestamp];

    return (
      <div className="feedback-section">
        <div className="feedback-buttons">
          {hasThumbFeedback && rating === 'thumbs_up' ? (
            <div className="feedback-sent thumbs-up">👍</div>
          ) : (
                         <button
               type="button"
               className="feedback-btn thumbs-up"
               onClick={() => handleFeedback(timestamp, 'thumbs_up')}
               disabled={hasThumbFeedback}
             >
               👍
             </button>
          )}
          
          {hasThumbFeedback && rating === 'thumbs_down' ? (
            <div className="feedback-sent thumbs-down">👎</div>
          ) : (
                         <button
               type="button"
               className="feedback-btn thumbs-down"
               onClick={() => handleFeedback(timestamp, 'thumbs_down')}
               disabled={hasThumbFeedback}
             >
               👎
             </button>
          )}
        </div>
        
        {!hasTextFeedback ? (
          <div className="text-feedback">
            <input
              type="text"
              placeholder="Additional feedback (optional)"
              className="feedback-input"
              value={feedbackTexts[timestamp] || ''}
              onChange={(e) => setFeedbackTexts(prev => ({ ...prev, [timestamp]: e.target.value }))}
            />
            <button
              type="button"
              className="feedback-submit-btn"
                             onClick={() => {
                 const text = feedbackTexts[timestamp];
                 if (text) {
                   handleFeedback(timestamp, 'text_feedback', text);
                 }
               }}
            >
              Submit Feedback
            </button>
          </div>
        ) : (
          <div className="feedback-sent-text">✓ Text feedback submitted</div>
        )}
      </div>
    );
  };

  return (
    <div className="layout">
      {/* Left suggestion rail */}
      <aside className="rail">
        <div className="rail-title">Some questions you can ask me</div>
        <ul className="rail-list">
          {suggested.map((q, i) => (
            <li key={i}>
              <button
                type="button"
                className="rail-item"
                onClick={() => pasteSuggestion(q)}
                title="Click to use this question"
              >
                {q}
              </button>
            </li>
          ))}
        </ul>
        
        {/* New Conversation Button */}
        <div className="rail-footer">
          <button
            type="button"
            className="new-conversation-btn"
            onClick={() => {
              setMessages([]);
              setFeedbackSent(new Set());
              setFeedbackRatings({});
              setFeedbackTexts({});
              setFeedbackError(null);
            }}
            title="Start a new conversation"
          >
            New Conversation
          </button>
        </div>
      </aside>

      {/* Main area */}
      <div className="main">
        <header className="header">
          <div className="title">Internet2 Chatbot PoC</div>
          <div className="session-info">Session ID: {sessionId}</div>
        </header>

        {/* "Prompt card" like Streamlit */}
        <div className="prompt-card">
          <div className="prompt-icon">?</div>
          <div className="prompt-text">What workloads can I run on AWS?</div>
        </div>

        {/* Error message */}
        {feedbackError && (
          <div className="error-message">
            {feedbackError}
          </div>
        )}
        
        {/* Messages card */}
        <div className="card chat-card">
          <div className="messages">
          {messages.map((message, index) => {
  const isAssistant = message.role === 'assistant';
  const hasError = message.isError;

  // Gather meeting/video links for the footer; keep inline links untouched
  const meetingLinks = isAssistant ? collectMeetingLinks(message.content) : [];

  return (
    <div
      key={index}
      className={`message ${message.role} ${hasError ? 'error' : ''}`}
    >
      <div className="bubble">
        {isAssistant ? (
          <ReactMarkdown remarkPlugins={[remarkGfm]}>
            {message.content}
          </ReactMarkdown>
        ) : (
          message.content
        )}

        {message.timestamp && (
          <div className="timestamp">
            {new Date(message.timestamp).toLocaleTimeString()}
          </div>
        )}
        
        {/* Render feedback buttons for assistant messages */}
        {renderFeedbackButtons(message, index)}
      </div>
    </div>
  );
})}


            {isLoading && (
              <div className="message assistant">
                <div className="bubble loading">Processing...</div>
              </div>
            )}
          </div>

          {/* Fixed input bar */}
          <form onSubmit={handleSubmit} className="input-form">
            <input
              type="text"
              value={input}
              onChange={(e) => setInput(e.target.value)}
              placeholder="Type your question here..."
              disabled={isLoading}
            />
            <button type="submit" disabled={isLoading}>
              {isLoading ? 'Sending…' : 'Send'}
            </button>
          </form>
        </div>
      </div>
    </div>
  );
}

export default App;



