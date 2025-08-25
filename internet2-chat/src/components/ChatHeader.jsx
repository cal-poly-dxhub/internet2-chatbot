import React from 'react';

const ChatHeader = ({ sessionId }) => {
  return (
    <header className="header">
      <div className="title">Internet2 Chatbot PoC</div>
      <div className="session-info">Session ID: {sessionId}</div>
    </header>
  );
};

export default ChatHeader;
