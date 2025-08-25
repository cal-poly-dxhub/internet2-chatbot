import React from 'react';

const SuggestionRail = ({ suggested, onPasteSuggestion, onNewConversation }) => {
  return (
    <aside className="rail">
      <div className="rail-title">Some questions you can ask me</div>
      <ul className="rail-list">
        {suggested.map((q, i) => (
          <li key={i}>
            <button
              type="button"
              className="rail-item"
              onClick={() => onPasteSuggestion(q)}
              title="Click to use this question"
            >
              {q}
            </button>
          </li>
        ))}
      </ul>
      
      {/* New Conversation Button */}
      <div className="rail-footer">
        <button
          type="button"
          className="new-conversation-btn"
          onClick={onNewConversation}
          title="Start a new conversation"
        >
          New Conversation
        </button>
      </div>
    </aside>
  );
};

export default SuggestionRail;
