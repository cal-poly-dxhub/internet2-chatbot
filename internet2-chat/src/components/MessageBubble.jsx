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

  // Function to parse and format content with better structure
  const parseContent = (text) => {
    if (!text) return text;
    
    // Split by lines and process each line
    return text.split('\n').map((line, lineIndex) => {
      // Skip empty lines
      if (line.trim() === '') return null;
      
      // Check if line is a numbered section (1., 2., 3., etc.)
      const numberedMatch = line.match(/^(\d+\.\s+)(.+)/);
      if (numberedMatch) {
        const [, number, content] = numberedMatch;
        return (
          <div key={lineIndex} className="numbered-section">
            <span className="section-number">{number}</span>
            <span className="section-title">{content}</span>
          </div>
        );
      }
      
      // Check if line is a bullet point
      const bulletMatch = line.match(/^(\s*[-•*]\s+)(.+)/);
      if (bulletMatch) {
        const [, bullet, content] = bulletMatch;
        return (
          <div key={lineIndex} className="bullet-point">
            <span className="bullet-mark">{bullet}</span>
            <span className="bullet-content">{parseMarkdownLinks(content)}</span>
          </div>
        );
      }
      
      // Check if line contains markdown links
      if (line.includes('[') && line.includes('](')) {
        return (
          <div key={lineIndex} className="content-line">
            {parseMarkdownLinks(line)}
          </div>
        );
      }
      
      // Regular content line
      return (
        <div key={lineIndex} className="content-line">
          {line}
        </div>
      );
    }).filter(Boolean);
  };

  // Function to parse markdown links and convert them to clickable elements
  const parseMarkdownLinks = (text) => {
    if (!text) return text;
    
    // Regex to match markdown links: [text](url)
    const linkRegex = /\[([^\]]+)\]\(([^)]+)\)/g;
    
    const parts = [];
    let lastIndex = 0;
    let match;
    
    while ((match = linkRegex.exec(text)) !== null) {
      // Add text before the link
      if (match.index > lastIndex) {
        parts.push(text.slice(lastIndex, match.index));
      }
      
      // Add the clickable link
      const linkText = match[1].replace(/_/g, '').replace(/\*/g, '');
      const linkUrl = match[2];
      
      parts.push(
        <a
          key={match.index}
          href={linkUrl}
          target="_blank"
          rel="noopener noreferrer"
          className="inline-link"
          onClick={(e) => {
            e.preventDefault();
            e.stopPropagation();
            window.open(linkUrl, '_blank', 'noopener,noreferrer');
          }}
        >
          {linkText}
        </a>
      );
      
      lastIndex = match.index + match[0].length;
    }
    
    // Add remaining text after the last link
    if (lastIndex < text.length) {
      parts.push(text.slice(lastIndex));
    }
    
    return parts.length > 0 ? parts : text;
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
          <div className="assistant-content">
            {/* Assistant text with enhanced formatting */}
            <div className="response-text">
              {parseContent(message.content)}
            </div>

            {/* Sources section */}
            {message.sources && message.sources.length > 0 && (
              <div className="sources-section">
                <h4>Sources</h4>
                <ul className="sources-list">
                  {message.sources.map((src, i) => (
                    <li key={i} className={`source-item ${src.type === 'video' || src.type === 'podcast' ? 'video-source' : ''}`}>
                      <a
                        href={src.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className={`source-link ${src.type === 'video' || src.type === 'podcast' ? 'video-link' : ''}`}
                        onClick={(e) => {
                          e.preventDefault();
                          e.stopPropagation();
                          window.open(src.url, '_blank', 'noopener,noreferrer');
                        }}
                      >
                        {src.type === 'video' || src.type === 'podcast' ? (
                          <span className="video-title">
                            <span className="video-icon">🎥</span>
                            {src.title}
                            {src.timestamp && (
                              <span className="video-timestamp">
                                (starts at {Math.floor(src.timestamp / 60)}:{(src.timestamp % 60).toFixed(0).padStart(2, '0')})
                              </span>
                            )}
                          </span>
                        ) : (
                          src.title
                        )}
                      </a>
                      <span className="source-badge"> — {src.badge}</span>
                    </li>
                  ))}
                </ul>
              </div>
            )}

            {/* Meetings section - Distinct bubble */}
            {message.meetings && message.meetings.length > 0 && (
              <div className="meetings-container">
                <div className="meetings-bubble">
                  <div className="meetings-header">
                    <h4>Meetings Referenced</h4>
                    <span className="meetings-count">{message.meetings.length} meeting{message.meetings.length !== 1 ? 's' : ''}</span>
                  </div>
                  <div className="meetings-content">
                    {message.meetings
                      .sort((a, b) => a.name.localeCompare(b.name))
                      .map((meeting, i) => (
                        <div key={i} className="meeting-item">
                          <a
                            href={meeting.url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="meeting-link"
                            onClick={(e) => {
                              e.preventDefault();
                              e.stopPropagation();
                              window.open(meeting.url, '_blank', 'noopener,noreferrer');
                            }}
                          >
                            <span className="meeting-icon">📅</span>
                            <span className="meeting-name">{meeting.name}</span>
                          </a>
                          <span className="meeting-badge">{meeting.badge.replace(/_/g, '').replace(/\*/g, '')}</span>
                        </div>
                      ))}
                  </div>
                </div>
              </div>
            )}
          </div>
        ) : (
          <div className="user-content">{message.content}</div>
        )}

        {/* Timestamp */}
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