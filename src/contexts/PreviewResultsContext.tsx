import React, { createContext, useContext, useState } from 'react';

type PreviewResultsContextShape = {
  previewResults: any[] | null;
  setPreviewResults: (v: any[] | null) => void;
  open: boolean;
  setOpen: (v: boolean) => void;
};

const PreviewResultsContext = createContext<PreviewResultsContextShape | null>(null);

export const PreviewResultsProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [previewResults, setPreviewResults] = useState<any[] | null>(null);
  const [open, setOpen] = useState<boolean>(false);

  return (
    <PreviewResultsContext.Provider value={{ previewResults, setPreviewResults, open, setOpen }}>
      {children}
    </PreviewResultsContext.Provider>
  );
};

export const usePreviewResults = () => {
  const ctx = useContext(PreviewResultsContext);
  if (!ctx) throw new Error('usePreviewResults must be used inside PreviewResultsProvider');
  return ctx;
};

export default PreviewResultsContext;

