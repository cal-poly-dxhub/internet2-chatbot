import React from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import FeedbackSection from './FeedbackSection';

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

  // Gather meeting/video links for the footer; keep inline links untouched
  const meetingLinks = isAssistant ? collectMeetingLinks(message.content) : [];

  return (
    <div
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
