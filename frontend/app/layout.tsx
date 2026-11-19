import React from "react";
import "./globals.css";

export const metadata = {
  title: "APEX-OMEGA V9 // DUAL-CORE HYPER-COMPOUNDER",
  description: "APEX-OMEGA V9 Dual-Core Hyper-Compounder & 5-Stage Asymmetric Ratchet",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className="bg-[#0B0F12] text-slate-200 antialiased font-mono">{children}</body>
    </html>
  );
}
