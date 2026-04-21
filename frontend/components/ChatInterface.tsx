'use client';

import { useState, useRef, useEffect } from 'react';
import { Send, MessageCircle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Avatar, AvatarImage, AvatarFallback } from '@/components/ui/avatar';
import { buildApiUrl } from '@/lib/api';

interface Message {
  id: string;
  type: 'user' | 'assistant';
  content: string;
  timestamp: Date;
}

interface ChatHistoryItem {
  role: 'user' | 'assistant';
  content: string;
}

const AI_AVATAR_SRC = '/ai-avatar.png';
const USER_AVATAR_SRC = '/user-no-av.png';

export default function ChatInterface() {
  const [messages, setMessages] = useState<Message[]>([
    {
      id: '0',
      type: 'assistant',
      content:
        'I am InsightFlow AI. Ask me about your dataset and I will respond with direct answers.',
      timestamp: new Date(),
    },
  ]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [history, setHistory] = useState<ChatHistoryItem[]>([]);
  const chatContainerRef = useRef<HTMLDivElement>(null);

  const scrollToBottom = () => {
    if (chatContainerRef.current) {
      chatContainerRef.current.scrollTo({
        top: chatContainerRef.current.scrollHeight,
        behavior: 'smooth',
      });
    }
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSendMessage = async (e: React.FormEvent) => {
    e.preventDefault();

    if (!input.trim()) return;

    // Add user message
    const userMessage: Message = {
      id: Date.now().toString(),
      type: 'user',
      content: input,
      timestamp: new Date(),
    };

    setMessages((prev) => [...prev, userMessage]);
    setInput('');
    setLoading(true);

    try {
      // Send to backend
      const response = await fetch(buildApiUrl('/api/chat'), {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          question: input,
          history,
        }),
      });

      if (!response.ok) {
        throw new Error('Failed to get response');
      }

      const data = await response.json();
      if (Array.isArray(data.history)) {
        setHistory(data.history);
      }

      // Add assistant message
      const assistantMessage: Message = {
        id: (Date.now() + 1).toString(),
        type: 'assistant',
        content: data.answer,
        timestamp: new Date(),
      };

      setMessages((prev) => [...prev, assistantMessage]);
    } catch (error) {
      console.error('Error sending message:', error);

      const errorMessage: Message = {
        id: (Date.now() + 2).toString(),
        type: 'assistant',
        content:
          'Sorry, I encountered an error processing your question. Please try again.',
        timestamp: new Date(),
      };

      setMessages((prev) => [...prev, errorMessage]);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="flex h-150 flex-col rounded-xl border border-white/6 bg-transparent">
      {/* Header */}
      <div className="border-b border-white/6 p-4">
        <div className="flex items-center gap-2">
          <MessageCircle className="h-5 w-5 text-brand-teal" />
          <h2 className="font-heading text-2xl text-white">
            Ask Questions About Your Data
          </h2>
        </div>
      </div>

      {/* Messages */}
      <div className="flex-1 overflow-y-auto p-4 space-y-4" ref={chatContainerRef}>
        {messages.map((message) => (
          <div
            key={message.id}
            className={`flex items-end gap-2 ${
              message.type === 'user' ? 'justify-end' : 'justify-start'
            }`}
          >
            {message.type === 'assistant' && (
              <Avatar className="h-8 w-8 border border-white/10">
                <AvatarImage
                  src={AI_AVATAR_SRC}
                  fallbackSrc={AI_AVATAR_SRC}
                  alt="AI avatar"
                />
                <AvatarFallback className="bg-brand-teal/20 text-xs text-brand-teal">
                  AI
                </AvatarFallback>
              </Avatar>
            )}
            <div
              className={`max-w-xs lg:max-w-md px-4 py-2 rounded-lg ${
                message.type === 'user'
                  ? 'rounded-br-none bg-brand-teal text-white'
                  : 'rounded-bl-none border border-white/8 bg-white/4 text-[#E3E2E8]'
              }`}
            >
              <p className="text-sm whitespace-pre-wrap">{message.content}</p>
              <p
                className={`text-xs mt-1 ${
                  message.type === 'user'
                    ? 'text-white/70'
                    : 'text-[#938EA0]'
                }`}
              >
                {message.timestamp.toLocaleTimeString([], {
                  hour: '2-digit',
                  minute: '2-digit',
                })}
              </p>
            </div>
            {message.type === 'user' && (
              <Avatar className="h-8 w-8 border border-white/10">
                <AvatarImage src={USER_AVATAR_SRC} alt="User avatar" />
                <AvatarFallback className="bg-brand-teal/20 text-xs text-brand-teal">
                  U
                </AvatarFallback>
              </Avatar>
            )}
          </div>
        ))}

        {loading && (
          <div className="flex items-end gap-2 justify-start">
            <Avatar className="h-8 w-8 border border-white/10">
              <AvatarImage
                src={AI_AVATAR_SRC}
                fallbackSrc={AI_AVATAR_SRC}
                alt="AI avatar"
              />
              <AvatarFallback className="bg-brand-teal/20 text-xs text-brand-teal">
                AI
              </AvatarFallback>
            </Avatar>
            <div className="rounded-lg rounded-bl-none border border-white/8 bg-white/4 px-4 py-2 text-[#E3E2E8]">
              <div className="flex gap-1">
                <div className="h-2 w-2 animate-bounce rounded-full bg-[#938EA0]"></div>
                <div className="h-2 w-2 animate-bounce rounded-full bg-[#938EA0]"
                  style={{ animationDelay: '0.2s' }}></div>
                <div className="h-2 w-2 animate-bounce rounded-full bg-[#938EA0]"
                  style={{ animationDelay: '0.4s' }}></div>
              </div>
            </div>
          </div>
        )}

        <div />
      </div>

      {/* Input */}
      <div className="border-t border-white/6 p-4">
        <form onSubmit={handleSendMessage} className="flex gap-2">
          <Input
            type="text"
            placeholder="Ask about your data..."
            value={input}
            onChange={(e) => setInput(e.target.value)}
            disabled={loading}
            className="border-white/8 bg-white/4 text-white placeholder:text-[#938EA0] focus:border-brand-teal/60"
          />
          <Button
            type="submit"
            disabled={loading || !input.trim()}
          >
            <Send className="w-4 h-4" />
          </Button>
        </form>
      </div>
    </div>
  );
}
