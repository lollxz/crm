import React from 'react';

interface LoadingOverlayProps {
  isOpen: boolean;
  message?: string;
}

/**
 * A reusable loading overlay component that displays a spinner and optional message.
 * Appears as a semi-transparent overlay covering the entire screen.
 */
export function LoadingOverlay({ isOpen, message = 'Loading...' }: LoadingOverlayProps) {
  if (!isOpen) return null;

  return (
    <div className="fixed inset-0 bg-black bg-opacity-30 z-[9999] flex items-center justify-center">
      <div className="bg-white rounded-lg shadow-lg p-8 flex flex-col items-center space-y-4">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-500"></div>
        <p className="text-gray-700 font-medium">{message}</p>
      </div>
    </div>
  );
}
