import React from 'react';

const FeedbackSection = ({ 
  timestamp, 
  feedbackSent, 
  feedbackRatings, 
  feedbackTexts, 
  onFeedback, 
  onFeedbackTextChange 
}) => {
  const thumbKey = `${timestamp}_thumb`;
  const textKey = `${timestamp}_text`;
  const hasThumbFeedback = feedbackSent.has(thumbKey);
  const hasTextFeedback = feedbackSent.has(textKey);
  const rating = feedbackRatings[timestamp];

  return (
    <div className="feedback-section">
      <div className="feedback-buttons">
        {hasThumbFeedback && rating === 'thumbs_up' ? (
          <div className="feedback-sent thumbs-up">👍</div>
        ) : (
          <button
            type="button"
            className="feedback-btn thumbs-up"
            onClick={() => onFeedback(timestamp, 'thumbs_up')}
            disabled={hasThumbFeedback}
          >
            👍
          </button>
        )}
        
        {hasThumbFeedback && rating === 'thumbs_down' ? (
          <div className="feedback-sent thumbs-down">👎</div>
        ) : (
          <button
            type="button"
            className="feedback-btn thumbs-down"
            onClick={() => onFeedback(timestamp, 'thumbs_down')}
            disabled={hasThumbFeedback}
          >
            👎
          </button>
        )}
      </div>
      
      {!hasTextFeedback ? (
        <div className="text-feedback">
          <input
            type="text"
            placeholder="Additional feedback (optional)"
            className="feedback-input"
            value={feedbackTexts[timestamp] || ''}
            onChange={(e) => onFeedbackTextChange(timestamp, e.target.value)}
          />
          <button
            type="button"
            className="feedback-submit-btn"
            onClick={() => {
              const text = feedbackTexts[timestamp];
              if (text) {
                onFeedback(timestamp, 'text_feedback', text);
              }
            }}
          >
            Submit Feedback
          </button>
        </div>
      ) : (
        <div className="feedback-sent-text">✓ Text feedback submitted</div>
      )}
    </div>
  );
};

export default FeedbackSection;
