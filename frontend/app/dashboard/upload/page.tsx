"use client";
import { useEffect } from "react";
import { useRouter } from "next/navigation";

export default function UploadPage() {
  const router = useRouter();
  useEffect(() => {
    router.replace("/analyze");
  }, [router]);
  return (
    <div
      className="flex min-h-[40vh] items-center justify-center"
      style={{ fontFamily: "var(--font-dm-sans), sans-serif" }}
    >
      <p className="text-sm text-[#888]">Redirecting to analyzer...</p>
    </div>
  );
}
