// src/App.jsx
import React, { useState } from 'react';
import './App.css';
import { sendMessage, sendFeedback } from './services/api';
import { 
  SuggestionRail, 
  ChatHeader, 
  ChatCard 
} from './components';

function App() {
  const [messages, setMessages] = useState([]); //conversation history
  const [input, setInput] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  //const [sessionId] = useState(() => `session_${Date.now()}`);
  const [sessionId] = useState(() => {
    const existingSession = localStorage.getItem('chatSessionId');
    if (existingSession) return existingSession;
    const newSession = `session_${Date.now()}`;
    localStorage.setItem('chatSessionId', newSession);
    return newSession;
  });
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

 

  const handleSendMessage = async (message) => {
    try {
      const response = await sendMessage(message, sessionId);
      setMessages(prevMessages => [...prevMessages, 
        { role: 'user', content: message },
        { role: 'assistant', content: response.answer }
      ]);
    } catch (error) {
      console.error('Error sending message:', error);
    }
  };

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

  const handleFeedbackTextChange = (timestamp, text) => {
    setFeedbackTexts(prev => ({ ...prev, [timestamp]: text }));
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
      console.log("DEBUG RESPONSE", response);

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

  const handleNewConversation = () => {
    setMessages([]);
    setFeedbackSent(new Set());
    setFeedbackRatings({});
    setFeedbackTexts({});
    setFeedbackError(null);
  };

  const handleInputChange = (e) => {
    setInput(e.target.value);
  };

  return (
    <div className="layout">
      {/* Left suggestion rail */}
      <SuggestionRail
        suggested={suggested}
        onPasteSuggestion={pasteSuggestion}
        onNewConversation={handleNewConversation}
      />

      {/* Main area */}
      <div className="main">
        <ChatHeader sessionId={sessionId} />

        {/* Error message */}
        {feedbackError && (
          <div className="error-message">
            {feedbackError}
          </div>
        )}
        
        {/* Chat card with messages and input */}
        <ChatCard
          messages={messages}
          isLoading={isLoading}
          feedbackSent={feedbackSent}
          feedbackRatings={feedbackRatings}
          feedbackTexts={feedbackTexts}
          onFeedback={handleFeedback}
          onFeedbackTextChange={handleFeedbackTextChange}
          input={input}
          onInputChange={handleInputChange}
          onSubmit={handleSubmit}
        />
      </div>
    </div>
  );
}

export default App;



