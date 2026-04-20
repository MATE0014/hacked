"use client";

import { useRef, useEffect } from "react";

const AnimatedCanvas = () => {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;

    const context = canvas.getContext("2d");
    if (!context) return;
    const drawContext: CanvasRenderingContext2D = context;

    let width = 0;
    let height = 0;
    let animationFrameId = 0;

    const setSize = () => {
      width = canvas.width = window.innerWidth;
      height = canvas.height = window.innerHeight;
    };

    setSize();

    const pointer = { x: width / 2, y: height / 2, active: false };

    const dots: Dot[] = [];
    const ripples: {
      x: number;
      y: number;
      radius: number;
      alpha: number;
    }[] = [];
    const numDots = 140;

    class Dot {
      x: number;
      y: number;
      vx: number;
      vy: number;
      radius: number;
      alpha: number;

      constructor(x: number, y: number, vx: number, vy: number) {
        this.x = x;
        this.y = y;
        this.vx = vx;
        this.vy = vy;
        this.radius = Math.random() * 1.8 + 0.7;
        this.alpha = Math.random() * 0.45 + 0.25;
      }

      draw() {
        drawContext.beginPath();
        drawContext.arc(this.x, this.y, this.radius, 0, Math.PI * 2);
        drawContext.fillStyle = `rgba(123, 92, 240, ${this.alpha})`;
        drawContext.fill();
      }

      update() {
        if (pointer.active) {
          const dx = this.x - pointer.x;
          const dy = this.y - pointer.y;
          const distance = Math.hypot(dx, dy) || 1;
          const influenceRadius = 170;

          if (distance < influenceRadius) {
            const force = (influenceRadius - distance) / influenceRadius;
            this.vx += (dx / distance) * force * 0.08;
            this.vy += (dy / distance) * force * 0.08;
          }
        }

        this.x += this.vx;
        this.y += this.vy;

        this.vx *= 0.985;
        this.vy *= 0.985;

        if (this.x < 0 || this.x > width) this.vx *= -1;
        if (this.y < 0 || this.y > height) this.vy *= -1;

        this.x = Math.max(0, Math.min(width, this.x));
        this.y = Math.max(0, Math.min(height, this.y));
      }
    }

    const init = () => {
      for (let i = 0; i < numDots; i++) {
        const x = Math.random() * width;
        const y = Math.random() * height;
        const vx = (Math.random() - 0.5) * 0.4;
        const vy = (Math.random() - 0.5) * 0.4;
        dots.push(new Dot(x, y, vx, vy));
      }
    };

    const drawRipples = () => {
      for (let i = ripples.length - 1; i >= 0; i -= 1) {
        const ripple = ripples[i];
        if (!ripple) {
          continue;
        }
        ripple.radius += 1.8;
        ripple.alpha -= 0.012;

        if (ripple.alpha <= 0) {
          ripples.splice(i, 1);
          continue;
        }

        drawContext.beginPath();
        drawContext.arc(ripple.x, ripple.y, ripple.radius, 0, Math.PI * 2);
        drawContext.strokeStyle = `rgba(20, 184, 166, ${ripple.alpha})`;
        drawContext.lineWidth = 1.2;
        drawContext.stroke();
      }
    };

    const animate = () => {
      const gradient = drawContext.createLinearGradient(0, 0, width, height);
      gradient.addColorStop(0, "#07080C");
      gradient.addColorStop(1, "#0B101A");
      drawContext.fillStyle = gradient;
      drawContext.fillRect(0, 0, width, height);

      dots.forEach((dot) => {
        dot.update();
        dot.draw();
      });

      drawRipples();

      for (let i = 0; i < dots.length; i++) {
        for (let j = i + 1; j < dots.length; j++) {
          const dotA = dots[i];
          const dotB = dots[j];
          if (!dotA || !dotB) {
            continue;
          }

          const dist = Math.hypot(dotA.x - dotB.x, dotA.y - dotB.y);
          if (dist < 150) {
            drawContext.beginPath();
            drawContext.moveTo(dotA.x, dotA.y);
            drawContext.lineTo(dotB.x, dotB.y);
            drawContext.strokeStyle = `rgba(123, 92, 240, ${(1 - dist / 150) * 0.4})`;
            drawContext.lineWidth = 0.7;
            drawContext.stroke();
          }
        }
      }

      animationFrameId = requestAnimationFrame(animate);
    };

    const handleMouseMove = (event: MouseEvent) => {
      pointer.x = event.clientX;
      pointer.y = event.clientY;
      pointer.active = true;
    };

    const handleMouseLeave = () => {
      pointer.active = false;
    };

    const handleClick = (event: MouseEvent) => {
      ripples.push({
        x: event.clientX,
        y: event.clientY,
        radius: 4,
        alpha: 0.45,
      });

      for (let i = 0; i < 12; i += 1) {
        dots.push(
          new Dot(
            event.clientX,
            event.clientY,
            (Math.random() - 0.5) * 2.2,
            (Math.random() - 0.5) * 2.2
          )
        );
      }

      if (dots.length > numDots + 50) {
        dots.splice(0, dots.length - (numDots + 50));
      }
    };

    window.addEventListener("resize", setSize);
    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseleave", handleMouseLeave);
    window.addEventListener("click", handleClick);

    init();
    animate();

    return () => {
      cancelAnimationFrame(animationFrameId);
      window.removeEventListener("resize", setSize);
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseleave", handleMouseLeave);
      window.removeEventListener("click", handleClick);
    };
  }, []);

  return (
    <canvas
      ref={canvasRef}
      className="fixed inset-0 z-0 h-full w-full"
    />
  );
};

export default AnimatedCanvas;