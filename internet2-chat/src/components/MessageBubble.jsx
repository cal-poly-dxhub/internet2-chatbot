import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import FeedbackSection from './FeedbackSection';

const MessageBubble = ({ 
  message, 
  index, 
  feedbackSent, 
  feedbackRatings, 
  feedbackTexts, 
  onFeedback, 
  onFeedbackTextChange 
}) => {
  const isAssistant = message.role === 'assistant';
  const hasError = message.isError;

  return (
    <div
      className={`message ${message.role} ${hasError ? 'error' : ''}`}
    >
      {isAssistant ? (
        <div className="avatar assistant" aria-hidden>AI</div>
      ) : (
        <div className="avatar user" aria-hidden>U</div>
      )}
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
        {isAssistant && message.timestamp && (
          <FeedbackSection
            timestamp={message.timestamp}
            feedbackSent={feedbackSent}
            feedbackRatings={feedbackRatings}
            feedbackTexts={feedbackTexts}
            onFeedback={onFeedback}
            onFeedbackTextChange={onFeedbackTextChange}
          />
        )}
      </div>
    </div>
  );
};

export default MessageBubble;
