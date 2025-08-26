import React from 'react';

const ChatHeader = ({ sessionId }) => {
  return (
    <header className="header">
      <div className="brand">
        <div className="brand-mark" aria-hidden>i2</div>
        <div className="brand-text">Internet2 Chatbot PoC</div>
      </div>
    </header>
  );
};

export default ChatHeader;
