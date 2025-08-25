import React from 'react';
import MessageBubble from './MessageBubble';
import ChatInput from './ChatInput';

const ChatCard = ({ 
  messages, 
  isLoading, 
  feedbackSent, 
  feedbackRatings, 
  feedbackTexts, 
  onFeedback, 
  onFeedbackTextChange,
  input,
  onInputChange,
  onSubmit
}) => {
  return (
    <div className="card chat-card">
      <div className="messages">
        {messages.map((message, index) => (
          <MessageBubble
            key={index}
            message={message}
            index={index}
            feedbackSent={feedbackSent}
            feedbackRatings={feedbackRatings}
            feedbackTexts={feedbackTexts}
            onFeedback={onFeedback}
            onFeedbackTextChange={onFeedbackTextChange}
          />
        ))}

        {isLoading && (
          <div className="message assistant">
            <div className="bubble loading">Processing...</div>
          </div>
        )}
      </div>

      {/* Fixed input bar */}
      <ChatInput
        input={input}
        onInputChange={onInputChange}
        onSubmit={onSubmit}
        isLoading={isLoading}
      />
    </div>
  );
};

export default ChatCard;
