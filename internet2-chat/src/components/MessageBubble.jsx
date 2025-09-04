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

  // Render sources and meetings exactly like the original markdown
  const renderSourcesAndMeetings = () => {
    if (!isAssistant || (!message.sources?.length && !message.meetings?.length)) return null;

    return (
      <div className="sources-and-meetings">
        {/* Render sources inline with the text (exactly like markdown links) */}
        {message.sources && message.sources.length > 0 && (
          <div className="sources">
            {message.sources.map((source, idx) => (
              <span key={idx} className="source-inline">
                <a 
                  href={source.url} 
                  target="_blank" 
                  rel="noopener noreferrer"
                  className="source-link"
                >
                  {source.title}
                </a>
                {source.timestamp && (
                  <span className="source-timestamp">#t={source.timestamp}</span>
                )}
                <span className="source-badge"> — _{source.badge}_</span>
              </span>
            ))}
          </div>
        )}

        {/* Render meetings section (exactly like markdown) */}
        {message.meetings && message.meetings.length > 0 && (
          <div className="meetings">
            <div className="meetings-header">
              <strong>Meetings referenced:</strong>
            </div>
            <ul className="meetings-list">
              {message.meetings.map((meeting, idx) => (
                <li key={idx} className="meeting-item">
                  <a 
                    href={meeting.url} 
                    target="_blank" 
                    rel="noopener noreferrer"
                    className="meeting-link"
                  >
                    {meeting.name}
                  </a>
                  <span className="meeting-badge"> — {meeting.badge}</span>
                </li>
              ))}
            </ul>
          </div>
        )}
      </div>
    );
  };

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
          <div>
            <ReactMarkdown remarkPlugins={[remarkGfm]}>
              {message.content}
            </ReactMarkdown>
            {renderSourcesAndMeetings()}
          </div>
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
