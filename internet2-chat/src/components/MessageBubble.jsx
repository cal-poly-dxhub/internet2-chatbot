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


  // Function to convert standalone URLs to clickable links
  const convertUrlsToLinks = (text) => {
    const urlRegex = /(https?:\/\/[^\s]+)/g;
    const parts = [];
    let lastIndex = 0;
    let match;

    while ((match = urlRegex.exec(text)) !== null) {
      const index = match.index;
      const url = match[0];

      // Push plain text before URL
      if (index > lastIndex) {
        parts.push(text.slice(lastIndex, index));
      }

      // Create clickable link
      parts.push(
        <a
          key={`url-${index}`}
          href={url}
          target="_blank"
          rel="noopener noreferrer"
          className="url-link"
          onClick={(e) => {
            e.preventDefault();
            e.stopPropagation();
            window.open(url, '_blank', 'noopener,noreferrer');
          }}
        >
          {url}
        </a>
      );

      lastIndex = index + url.length;
    }

    // Push remaining text
    if (lastIndex < text.length) {
      parts.push(text.slice(lastIndex));
    }

    return parts.length > 1 ? parts : text;
  };

  // Function to convert markdown-style links to clickable titles (fallback for old backend)
  const convertMarkdownLinks = (text) => {
    // Pattern to match [Title](URL) — _[Badge]_ format from old backend
    const markdownLinkRegex = /\[([^\]]+)\]\(([^)]+)\)\s*—\s*_\[([^\]]+)\]_/g;
    const parts = [];
    let lastIndex = 0;
    let match;

    while ((match = markdownLinkRegex.exec(text)) !== null) {
      const index = match.index;
      const title = match[1];
      const url = match[2];
      const badge = match[3];

      // Push plain text before the pattern
      if (index > lastIndex) {
        parts.push(text.slice(lastIndex, index));
      }

      // Create clickable title with badge (hide the URL completely)
      parts.push(
        <a
          key={`markdown-link-${index}`}
          href={url}
          target="_blank"
          rel="noopener noreferrer"
          className="title-link"
          onClick={(e) => {
            e.preventDefault();
            e.stopPropagation();
            window.open(url, '_blank', 'noopener,noreferrer');
          }}
        >
          [{title}] — _[{badge}]_
        </a>
      );

      lastIndex = index + match[0].length;
    }

    // Push remaining text
    if (lastIndex < text.length) {
      parts.push(text.slice(lastIndex));
    }

    return parts.length > 1 ? parts : text;
  };

  // Render inline sources where placeholders appear
  const renderWithInlineSources = (text, sources) => {
    // Debug logging
    console.log('renderWithInlineSources called with:', { text, sources });
    
    // First try to convert any markdown links (for old backend compatibility)
    const processedText = convertMarkdownLinks(text);
    
    // If we found markdown links, return them
    if (Array.isArray(processedText)) {
      return processedText;
    }

    // Process placeholders with JSON sources (new backend)
    const parts = [];
    let lastIndex = 0;
    let match;

    // Reset regex lastIndex to ensure it works properly
    placeholderRegex.lastIndex = 0;

    while ((match = placeholderRegex.exec(text)) !== null) {
      const index = match.index;
      const placeholderId = parseInt(match[1], 10);

      // Push plain text before placeholder
      if (index > lastIndex) {
        const beforeText = text.slice(lastIndex, index);
        parts.push(
          <span key={`text-${lastIndex}`}>
            {convertUrlsToLinks(beforeText)}
          </span>
        );
      }

      // Find matching source
      const source = sources?.find(s => s.id === placeholderId);
      console.log(`Looking for source with id ${placeholderId}:`, source);

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
              className="title-link"
            >
              [{source.title}] — _[{source.badge}]_
            </a>
          </span>
        );
      } else {
        // If no match, show raw placeholder
        console.log(`No source found for id ${placeholderId}`);
        parts.push(<span key={`raw-${index}`}>[{placeholderId}]</span>);
      }

      lastIndex = index + match[0].length;
    }

    // Push remaining text
    if (lastIndex < text.length) {
      const remainingText = text.slice(lastIndex);
      parts.push(
        <span key={`text-${lastIndex}`}>
          {convertUrlsToLinks(remainingText)}
        </span>
      );
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
          <div className="assistant-content">
            {/* Render assistant response with better formatting */}
            <div className="response-text">
              {message.content.split('\n').map((line, index) => {
                // Skip empty lines
                if (!line.trim()) return <br key={index} />;
                
                // Check if line starts with a number and period (numbered list)
                const numberedMatch = line.match(/^(\d+)\.\s+(.+)/);
                if (numberedMatch) {
                  const [, number, content] = numberedMatch;
                  return (
                    <div key={index} className="response-line numbered-section">
                      <strong>{number}.</strong> {renderWithInlineSources(content, message.sources)}
                    </div>
                  );
                }
                
                // Check if line starts with bullet point or hyphen and normalize to bullet point
                if (line.trim().startsWith('•') || line.trim().startsWith('-')) {
                  // Normalize hyphens to bullet points for consistent formatting
                  const normalizedLine = line.trim().startsWith('-') 
                    ? line.replace(/^-\s*/, '• ') 
                    : line;
                  
                  return (
                    <div key={index} className="response-line bullet-point">
                      {renderWithInlineSources(normalizedLine, message.sources)}
                    </div>
                  );
                }
                
                return (
                  <div key={index} className="response-line">
                    {renderWithInlineSources(line, message.sources)}
                  </div>
                );
              })}
            </div>

            {/* Meetings section: separate from sources */}
            {message.meetings && message.meetings.length > 0 && (
              <div className="meetings-section">
                <h4>Meetings referenced:</h4>
                <ul className="meetings-list">
                  {message.meetings
                    .sort((a, b) => a.id - b.id)
                    .map((meeting) => (
                      <li key={meeting.id} className="meeting-item">
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
                          {meeting.name}
                        </a>
                        <span className="meeting-badge"> — {meeting.badge}</span>
                      </li>
                    ))}
                </ul>
              </div>
            )}
          </div>
        ) : (
          <div className="user-content">
            {message.content}
          </div>
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