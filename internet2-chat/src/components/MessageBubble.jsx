import React from 'react';
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

  // Regex to detect [1], [2], etc.
  const placeholderRegex = /\[(\d+)\]/g;

  // Render inline sources where placeholders appear
  const renderWithInlineSources = (text, sources) => {
    const parts = [];
    let lastIndex = 0;
    let match;

    while ((match = placeholderRegex.exec(text)) !== null) {
      const index = match.index;
      const placeholderId = parseInt(match[1], 10);

      // Push plain text before placeholder
      if (index > lastIndex) {
        parts.push(
          <span key={`text-${lastIndex}`}>
            {text.slice(lastIndex, index)}
          </span>
        );
      }

      // Find matching source
      const source = sources?.find(s => s.id === placeholderId);

      if (source) {
        parts.push(
          <span key={`source-${placeholderId}-${index}`} className="inline-source">
            <a
              href={source.url}
              target="_blank"
              rel="noopener noreferrer"
              onClick={(e) => {
                e.preventDefault();
                e.stopPropagation();
                window.open(source.url, '_blank', 'noopener,noreferrer');
              }}
              className="source-link"
            >
              {source.title}
            </a>
            <span className="source-badge"> — {source.badge}</span>
          </span>
        );
      } else {
        // If no match, show raw placeholder
        parts.push(<span key={`raw-${index}`}>[{placeholderId}]</span>);
      }

      lastIndex = index + match[0].length;
    }

    // Push remaining text
    if (lastIndex < text.length) {
      parts.push(<span key={`text-${lastIndex}`}>{text.slice(lastIndex)}</span>);
    }

    return parts;
  };

  return (
    <div className={`message ${message.role} ${hasError ? 'error' : ''}`}>
      {isAssistant ? (
        <div className="avatar assistant" aria-hidden>AI</div>
      ) : (
        <div className="avatar user" aria-hidden>U</div>
      )}

      <div className="bubble">
        {isAssistant ? (
          <div>
            {/* Render assistant response */}
            <div className="response-text">
              {message.content.split('\n').map((line, index) => (
                <div key={index} className="response-line">
                  {renderWithInlineSources(line, message.sources)}
                </div>
              ))}
            </div>

            {/* Meetings section: separate from sources */}
            {message.meetings && message.meetings.length > 0 && (
              <div className="meetings-section">
                <h4>Meetings referenced:</h4>
                <ul>
                  {message.meetings
                    .sort((a, b) => a.id - b.id)
                    .map((meeting) => (
                      <li key={meeting.id}>
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
                        <span className="badge"> — {meeting.badge}</span>
                      </li>
                    ))}
                </ul>
              </div>
            )}
          </div>
        ) : (
          message.content
        )}

        {message.timestamp && (
          <div className="timestamp">
            {new Date(message.timestamp).toLocaleTimeString()}
          </div>
        )}

        {/* Feedback section */}
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