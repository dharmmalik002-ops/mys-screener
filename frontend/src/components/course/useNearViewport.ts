import { useEffect, useRef, useState } from "react";

/** Whether the element is on screen or within ~one screen of it. Course charts
    fetch and draw only near the screen, so pages carrying dozens of them stay
    light; `seen` stays true once the element has come near. */
export function useNearViewport<T extends Element>(margin = "900px 0px") {
  const ref = useRef<T | null>(null);
  const [near, setNear] = useState(false);
  const [seen, setSeen] = useState(false);
  useEffect(() => {
    const el = ref.current;
    if (!el || typeof IntersectionObserver === "undefined") {
      setNear(true);
      setSeen(true);
      return;
    }
    const io = new IntersectionObserver(
      ([entry]) => {
        setNear(entry.isIntersecting);
        if (entry.isIntersecting) setSeen(true);
      },
      { rootMargin: margin },
    );
    io.observe(el);
    return () => io.disconnect();
  }, [margin]);
  return { ref, near, seen };
}
