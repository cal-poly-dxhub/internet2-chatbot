// hooks/useChat.js
import { useState, useCallback } from 'react';
import { v4 as uuidv4 } from 'uuid';
import { sendMessage } from '../services/api';

export const useChat = () => {
  const [messages, setMessages] = useState([]);
  const [sessionId, setSessionId] = useState(() => uuidv4());
  const [loading, setLoading] = useState(false);

  const sendChatMessage = useCallback(async (message) => {
    setLoading(true);
    try {
      const response = await sendMessage(message, sessionId);
      setMessages(prev => [...prev, 
        { role: 'user', content: message },
        { role: 'assistant', content: response.response, timestamp: response.timestamp }
      ]);
      if (response.session_id) {
        setSessionId(response.session_id);
      }
    } catch (error) {
      console.error('Error in chat:', error);
    } finally {
      setLoading(false);
    }
  }, [sessionId]);

  return { messages, sendChatMessage, loading, sessionId };
};
