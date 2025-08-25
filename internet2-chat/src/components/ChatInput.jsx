import React from 'react';

const ChatInput = ({ input, onInputChange, onSubmit, isLoading }) => {
  return (
    <form onSubmit={onSubmit} className="input-form">
      <input
        type="text"
        value={input}
        onChange={onInputChange}
        placeholder="Type your question here..."
        disabled={isLoading}
      />
      <button type="submit" disabled={isLoading}>
        {isLoading ? 'Sending…' : 'Send'}
      </button>
    </form>
  );
};

export default ChatInput;
