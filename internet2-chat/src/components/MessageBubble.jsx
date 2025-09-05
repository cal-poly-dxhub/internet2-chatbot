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
          <div>
            <ReactMarkdown 
              remarkPlugins={[remarkGfm]}
              components={{
                a: ({ href, children, ...props }) => (
                  <a 
                    href={href} 
                    target="_blank" 
                    rel="noopener noreferrer"
                    onClick={(e) => {
                      e.preventDefault();
                      e.stopPropagation();
                      window.open(href, '_blank', 'noopener,noreferrer');
                    }}
                    {...props}
                  >
                    {children}
                  </a>
                )
              }}
            >
              {message.content}
            </ReactMarkdown>
            
            {/* Render sources and meetings from JSON */}
            {(message.sources && message.sources.length > 0) || (message.meetings && message.meetings.length > 0) ? (
              <div className="sources-and-meetings">
                {message.sources && message.sources.length > 0 && (
                  <div className="sources">
                    <h4>Sources:</h4>
                    <ul>
                      {message.sources.map((source, index) => (
                        <li key={index}>
                          <a 
                            href={source.url} 
                            target="_blank" 
                            rel="noopener noreferrer"
                            onClick={(e) => {
                              e.preventDefault();
                              e.stopPropagation();
                              window.open(source.url, '_blank', 'noopener,noreferrer');
                            }}
                          >
                            {source.title}
                          </a>
                          <span className="badge">{source.badge}</span>
                          {source.timestamp && (
                            <span className="timestamp"> (at {source.timestamp}s)</span>
                          )}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
                
                {message.meetings && message.meetings.length > 0 && (
                  <div className="meetings">
                    <h4>Meetings referenced:</h4>
                    <ul>
                      {message.meetings.map((meeting, index) => (
                        <li key={index}>
                          <a 
                            href={meeting.url} 
                            target="_blank" 
                            rel="noopener noreferrer"
                            onClick={(e) => {
                              e.preventDefault();
                              e.stopPropagation();
                              window.open(meeting.url, '_blank', 'noopener,noreferrer');
                            }}
                          >
                            {meeting.name}
                          </a>
                          <span className="badge">{meeting.badge}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            ) : null}
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
