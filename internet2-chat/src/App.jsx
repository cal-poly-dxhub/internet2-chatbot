// src/App.jsx
import React, { useState, useEffect } from 'react';
import './App.css';
import { sendMessage } from './services/api';

function App() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [sessionId] = useState(() => `session_${Date.now()}`);

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!input.trim() || isLoading) return;

    try {
      setIsLoading(true);
      console.log('Sending message:', {
        input: input,
        sessionId: sessionId
      });

      // Add user message to chat
      const userMessage = { role: 'user', content: input };
      setMessages(prev => [...prev, userMessage]);
      setInput('');

      // Send message to API
      const response = await sendMessage(input, sessionId);
      console.log('Received response:', response);

      // Add bot response to chat
      const botMessage = {
        role: 'assistant',
        content: response.response || response.message || 'No response content',
        timestamp: new Date().toISOString()
      };
      setMessages(prev => [...prev, botMessage]);

    } catch (error) {
      console.error('Chat error:', error);
      // Add error message to chat
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

  return (
    <div className="app">
      <header>
        <h1>Internet2 Chatbot PoC</h1>
        <div className="session-info">Session ID: {sessionId}</div>
      </header>
      
      <div className="chat-container">
        <div className="messages">
          {messages.map((message, index) => (
            <div 
              key={index} 
              className={`message ${message.role} ${message.isError ? 'error' : ''}`}
            >
              {message.content}
              {message.timestamp && (
                <div className="timestamp">
                  {new Date(message.timestamp).toLocaleTimeString()}
                </div>
              )}
            </div>
          ))}
          {isLoading && (
            <div className="message assistant loading">
              Processing...
            </div>
          )}
        </div>
        
        <form onSubmit={handleSubmit} className="input-form">
          <input
            type="text"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="Type your question here..."
            disabled={isLoading}
          />
          <button type="submit" disabled={isLoading}>
            {isLoading ? 'Sending...' : 'Send'}
          </button>
        </form>
      </div>
    </div>
  );
}

export default App;


